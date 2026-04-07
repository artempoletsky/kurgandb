import fs from "fs";

class CavityLengthsArray {
  constructor() {
  }

  findBestFit(length: number): [number, number] | undefined {
    return undefined;
  }

  remove(length: number, start: number) {
  }

  add(length: number, start: number) {
  }
}

export default class LogicalMemoryHeap {

  public heap!: Buffer;
  protected fd!: number;
  protected path: string;
  protected indexMap!: Map<number, Uint32Array>;

  protected currentWritePos!: number;
  protected lastId!: number;
  protected cavityStarts!: Map<number, number>;
  protected cavityEnds!: Map<number, number>;
  protected cavityLengths!: CavityLengthsArray;
  constructor(path: string) {
    this.path = path;
    this.reset();
  }

  commit() {
    let header = this.serializeHeader();
    const result = [header];
    let starts = Array.from(this.cavityStarts.keys()).sort();

    for (const start of starts) {
      let end = this.cavityStarts.get(start)!;
      result.push(this.heap.subarray(start, end));
    }
    fs.writevSync(this.fd, result);

    // todo: only compact memory instead of reloading
    this.reset();
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
      buf.writeUint32LE(id, writePos + 8);
      buf.writeUint32LE(id, writePos + 12);
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
    console.warn("LogicalMemoryHeap.allocateOverflow() Use allocate() manually for big operations");
    this.allocate(1024 * 1024 * 2);
  }

  reset() {
    // let sizeHeader = 1024 * 300;
    if (!fs.existsSync(this.path)) {
      fs.writeFileSync(this.path, Buffer.alloc(0));
    }

    let stat = fs.statSync(this.path);
    this.fd = fs.openSync(this.path, "r+");

    let data = Buffer.allocUnsafe(stat.size + 1024 * 1024);
    fs.readSync(this.fd, data);

    let sizeHeader = data.readUint32LE(0);
    this.heap = data.subarray(sizeHeader);
    this.currentWritePos = this.heap.byteLength;

    this.readHeader(data.subarray(0, sizeHeader));

    this.cavityEnds = new Map();
    this.cavityStarts = new Map();
    this.cavityLengths = new CavityLengthsArray();
  }


  update(id: number, data: Buffer) {
    const found = this.indexMap.get(id);
    if (!found) {
      throw "trying to update non existend id from LogicalMemoryHeap, id: " + id;
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
      this.cavityLengths.remove(oldEnd - end, end);
      this.cavityLengths.add(oldEnd - start, start);
      return;
    }

    if (this.cavityEnds.has(start)) {
      let oldStart = this.cavityEnds.get(start)!;
      this.cavityEnds.delete(start);
      this.cavityStarts.set(oldStart, end);
      this.cavityStarts.set(end, oldStart);
      this.cavityLengths.remove(start - oldStart, oldStart);
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
      throw "trying to delete non existend id from LogicalMemoryHeap, id: " + id;
    }
    this.indexMap.delete(id);
    this.createCavity(found[0], found[1]);
  }

  writeTailId(data: Buffer, id: number) {
    let start = this.currentWritePos;
    let end = start + data.byteLength;
    this.currentWritePos += data.byteLength;
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
    
    this.cavityLengths.remove(end - start, start);
    this.cavityStarts.delete(start);
    this.cavityEnds.delete(end);
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