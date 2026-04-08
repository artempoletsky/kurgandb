import fs from "fs";
import fsPromise from "fs/promises";
import { start } from "repl";
import CommitQueue from "./CommitQueue";

class CavityLengthsArray {
  public array: [number, number][];
  constructor() {
    this.array = [];
  }

  binarySearch(length: number): { found: boolean, index: number } {
    let left = 0;
    let right = this.array.length - 1;
    let mid = 0;
    while (left <= right) {
      let mid = ((left + right) >> 1);
      if (this.array[mid][0] < length) {
        left = mid + 1;
      } else if (this.array[mid][0] > length) {
        right = mid - 1;
      } else {
        return { found: true, index: mid };
      }
    }
    return { found: false, index: mid };
  }

  findBestFit(length: number): [number, number] | undefined {
    let idx = this.binarySearch(length);
    if (idx.found) {
      return this.array[idx.index];
    }
    return undefined;
  }

  delete(length: number, start: number) {
    let idx = this.binarySearch(length);
    if (!idx.found) {
      throw "LogicalMemoryHeap: attempt to delete non existent cavity with length: " + length + " and start: " + start;
    }
    ;
    for (let i = idx.index; i > 0; i--) {
      if (this.array[i][1] == start) {
        this.array.splice(i, 1);
        return;
      }
      if (this.array[i][0] != length) break;
    }

    for (let i = idx.index; i < this.array.length; i++) {
      if (this.array[i][1] == start) {
        this.array.splice(i, 1);
        return;
      }
      if (this.array[i][0] != length) break;
    }
    throw "LogicalMemoryHeap: attempt to delete non existent cavity with length: " + length + " and start: " + start;
  }

  add(length: number, start: number) {
    let { index } = this.binarySearch(length);
    this.array.splice(index, 0, [length, start]);
  }
}

export default class LogicalMemoryHeap {

  get __debug() {
    if (process.env.NODE_ENV !== "test") {
      throw "__debug method should only be used in tests";
    }
    return {
      heap: this.heap,
      fd: this.fd,
      path: this.path,
      indexMap: this.indexMap,
      currentWritePos: this.currentWritePos,
      lastId: this.lastId,
      cavityStarts: this.cavityStarts,
      cavityEnds: this.cavityEnds,
      cavityLengths: this.cavityLengths.array,
    }
  }

  protected heap!: Buffer;
  protected fd!: number;
  protected path: string;
  protected indexMap!: Map<number, Uint32Array>;

  protected currentWritePos!: number;
  protected lastId!: number;
  protected cavityStarts!: Map<number, number>;
  protected cavityEnds!: Map<number, number>;
  protected cavityLengths!: CavityLengthsArray;
  protected commitQueueId: string;
  constructor(path: string) {
    this.path = path;
    this.commitQueueId = CommitQueue.register("LogicalMemoryHeap_");
    this.reset();
  }

  async commit() {
    return new Promise<void>((resolve, reject) => {

      CommitQueue.start(this.commitQueueId);
      let header = this.serializeHeader();
      const result = [header];
      let starts = Array.from(this.cavityStarts.keys()).sort();

      let currentVectorStart = 0;
      for (const start of starts) {
        result.push(this.heap.subarray(currentVectorStart, start));
        currentVectorStart = this.cavityStarts.get(start)!;
      }
      result.push(this.heap.subarray(currentVectorStart, this.currentWritePos));
      console.log("write: " + result[0].readUint32LE(0));

      // fs.writeSync(this.fd, Buffer.concat(result), 0, );
      fs.writev(this.fd, result, 0, (err, written) => {
        if (err) {
          reject(err);
        } else {
          // todo: only compact memory instead of reloading
          this.reset();
          CommitQueue.end(this.commitQueueId);          
          resolve();
        }
      });



    });
  }

  serializeHeader(): Buffer {
    //[headerLen(4)][lastId(8)] [id(8)][start(4)][end(4)]
    let headerLen = 4 + 8 + this.indexMap.size * (8 + 4 + 4);
    const buf = Buffer.allocUnsafe(headerLen);
    buf.writeUint32LE(headerLen);
    buf.writeDoubleLE(this.lastId, 4);
    let writePos = 4 + 8;
    for (const [id, [start, end]] of this.indexMap) {
      buf.writeDoubleLE(id, writePos);
      buf.writeUint32LE(start, writePos + 8);
      buf.writeUint32LE(end, writePos + 12);
      writePos += 16;
    }
    return buf;
  }

  readHeader(buf: Buffer) {
    this.lastId = buf.readDoubleLE(4);
    let len = (buf.byteLength - 12) / 16;
    this.indexMap = new Map();
    for (let i = 0; i < len; i++) {
      let pos = 12 + i * 16;
      this.indexMap.set(buf.readDoubleLE(pos), new Uint32Array(buf.subarray(pos + 8, pos + 12)));
    }
  }

  allocate(size: number) {
    this.heap = Buffer.concat([this.heap, Buffer.allocUnsafe(size)]);
  }

  allocateOverflow() {
    console.warn("LogicalMemoryHeap: Unexpected allocateOverflow() call. Use allocate() manually for big operations");
    this.allocate(1024 * 1024 * 2);
  }

  reset() {
    // let sizeHeader = 1024 * 300;
    if (!fs.existsSync(this.path)) {
      this.indexMap = new Map();
      this.lastId = 0;
      fs.writeFileSync(this.path, this.serializeHeader());
    }

    let stat = fs.statSync(this.path);
    this.fd = fs.openSync(this.path, "r+");

    let data = Buffer.allocUnsafe(stat.size + 1024 * 1024);
    fs.readSync(this.fd, data);

    let sizeHeader = data.readUint32LE(0);
    console.log("read: " + sizeHeader);

    this.heap = data.subarray(sizeHeader);
    this.currentWritePos = stat.size - sizeHeader;

    this.readHeader(data.subarray(0, sizeHeader));

    this.cavityEnds = new Map();
    this.cavityStarts = new Map();
    this.cavityLengths = new CavityLengthsArray();
  }


  update(id: number, data: Buffer) {
    const found = this.indexMap.get(id);
    if (!found) {
      throw "trying to update non existent id from LogicalMemoryHeap, id: " + id;
    }
    let newEnd = found[0] + data.buffer.byteLength;
    if (newEnd == found[1]) {
      data.copy(this.heap, found[0]);
    } else if (newEnd > found[1]) {
      this.writeTailId(data, id);
      this.createCavity(found[0], found[1]);
    } else {
      data.copy(this.heap, found[0]);
      this.createCavity(found[0] + data.byteLength, found[1])
    }
  }

  updateString(id: number, string: string) {
    this.update(id, Buffer.from(string, "utf-8"));
  }

  createCavity(start: number, end: number) {
    if (this.cavityStarts.has(end)) {
      let oldEnd = this.cavityStarts.get(end)!;
      this.cavityStarts.delete(end);
      this.cavityEnds.set(start, oldEnd);
      this.cavityEnds.set(oldEnd, start);
      this.cavityLengths.delete(oldEnd - end, end);
      this.cavityLengths.add(oldEnd - start, start);
      return;
    }

    if (this.cavityEnds.has(start)) {
      let oldStart = this.cavityEnds.get(start)!;
      this.cavityEnds.delete(start);
      this.cavityStarts.set(oldStart, end);
      this.cavityStarts.set(end, oldStart);
      this.cavityLengths.delete(start - oldStart, oldStart);
      this.cavityLengths.add(end - oldStart, oldStart);
      return;

    }
    this.cavityStarts.set(start, end);
    this.cavityEnds.set(end, start);
    this.cavityLengths.add(end - start, start);


  }

  delete(id: number) {
    const found = this.indexMap.get(id);
    if (!found) {
      throw "trying to delete non existent id from LogicalMemoryHeap, id: " + id;
    }
    this.indexMap.delete(id);
    this.createCavity(found[0], found[1]);
  }

  writeTailId(data: Buffer, id: number) {
    let start = this.currentWritePos;
    let end = start + data.byteLength;
    if (end > this.heap.byteLength) {
      this.allocateOverflow();
    }
    this.currentWritePos += data.byteLength;
    data.copy(this.heap, start);
    this.indexMap.set(id, new Uint32Array([start, end]));
  }

  add(data: Buffer): number {
    this.lastId++;
    this.writeToCavityOrTail(data, this.lastId);
    return this.lastId;
  }

  writeToCavityOrTail(data: Buffer, id: number) {
    let freePos = this.cavityLengths.findBestFit(data.byteLength);
    if (!freePos) {
      this.writeTailId(data, id);
      return;
    }
    let start = freePos[1];
    let end = start + data.byteLength;

    if (freePos.length == data.byteLength) { // completely fill cavity
      this.cavityLengths.delete(end - start, start);
      this.cavityStarts.delete(start);
      this.cavityEnds.delete(end);
    } else if (freePos.length < data.byteLength) {
      throw "LogicalMemoryHeap: attempt to write in the smaller than the buffer cavity";
    } else {
      let dlen = freePos.length - data.byteLength;
      let newStart = end - dlen;
      this.cavityLengths.delete(end - start, start);
      this.cavityLengths.add(dlen, newStart);
      this.cavityStarts.delete(start);
      this.cavityStarts.set(newStart, end);
      this.cavityEnds.set(end, newStart);
    }

    this.indexMap.set(id, new Uint32Array([start, end]));
  }

  addString(string: string) {
    return this.add(Buffer.from(string, "utf-8"));
  }

  read(id: number): Buffer | undefined {
    const found = this.indexMap.get(id);
    if (!found) return undefined;
    return this.heap.subarray(found[0], found[1]);
  }

  readString(id: number): string | undefined {
    return this.read(id)?.toString("utf-8");
  }
}