import fs from "fs";
import fsPromise from "fs/promises";
import { start } from "repl";
import CommitQueue from "./CommitQueue";
import NamedByteBuffer, { TSuperblock, TPage } from "./NamedByteBuffer";


//[headerLen(4)][lastId(8)] [id(8)][len(4)] [data(var)]
type SUPERBLOCK_KEY = "headerLength" | "lastId";
const SUPERBLOCK_STRUCTURE = new Map<SUPERBLOCK_KEY, number>([
  ["headerLength", 4],
  ["lastId", 8],
]);

type HEADER_KEY = "id" | "sortKey" | "length";
const HEADER_STRUCTURE = new Map<HEADER_KEY, number>([
  ["id", 8],
  ["sortKey", 16],
  ["length", 4],
]);


export default class LogicalMemoryHeap {

  protected superblock!: TSuperblock<SUPERBLOCK_KEY>;

  get __debug() {
    if (process.env.NODE_ENV !== "test") {
      throw "__debug method should only be used in tests";
    }
    return {
      superblock: this.superblock,
      fd: this.fd,
      path: this.path,
      indexMap: this.indexMap
    }
  }

  protected heapMap!: Map<number, Buffer>;
  protected sizeHeap!: number;
  protected fd!: number;
  protected path: string;
  protected indexMap!: Map<number, Uint32Array>;

  protected commitQueueId: string;
  constructor(path: string) {
    this.path = path;
    this.commitQueueId = CommitQueue.register("LogicalMemoryHeap_");
    this.reset();
  }

  async commit() {

    CommitQueue.start(this.commitQueueId);
    // let header = this.serializeHeader();
    let buffers: Buffer[] = this.serialize();


    let writingPos = 0;
    for (const b of buffers) {
      await new Promise((resolve) => {
        fs.write(this.fd, b, 0, undefined, writingPos, resolve);
        writingPos += b.byteLength;
      });
    }

    CommitQueue.end(this.commitQueueId);
  }

  serialize(): Buffer[] {
    //[headerLen(4)][lastId(8)] [id(8)][len(4)] [data(var)]

    this.superblock.headerLength = this.heapMap.size;
    let result: Buffer[] = [this.superblock.$getBuffer()];
    if (!this.heapMap.size) {
      return [result[0], Buffer.alloc(0), Buffer.alloc(0)];
    }

    let header = NamedByteBuffer.createArray(HEADER_STRUCTURE, this.heapMap.size);

    let heapBuf = Buffer.allocUnsafe(this.sizeHeap);
    let heapWritePos = 0;
    let i = 0;
    for (const [id, data] of this.heapMap) {
      header.id.set(i, id);
      header.length.set(i, data.byteLength);
      if (data.byteLength > 16) {
        data.copy(heapBuf, heapWritePos);
        header.sortKey.set16(i, data.subarray(0, 16));
        heapWritePos += data.byteLength;
      } else {
        header.sortKey.set16(i, data);
      }

      i++;
    }
    result.push(header.$getBuffer());
    result.push(heapBuf);

    return result;
  }

  unserialize(buf: Buffer) {


    let recordsNum = this.superblock.headerLength;
    this.heapMap = new Map();
    if (!recordsNum) {
      this.sizeHeap = 0;
      return;
    }
    let header = NamedByteBuffer.createArray(HEADER_STRUCTURE, recordsNum);
    let sizeEntryHeader = header.$sizeEntry;

    let headerReadPos = this.superblock.$size;
    let heapReadPos = headerReadPos + sizeEntryHeader * recordsNum;

    const sizeHeader = heapReadPos;

    header.$setBuffer(buf.subarray(headerReadPos, sizeHeader));
    // this.header =

    for (let i = 0; i < recordsNum; i++) {
      let id = header.id.get(i);
      let len = header.length.get(i);

      let data: Buffer;
      if (len <= 16) {
        data = Buffer.alloc(len);
        header.sortKey.get16(i, data);
      } else {
        data = buf.subarray(heapReadPos, heapReadPos + len);
        heapReadPos += len;
      }

      this.heapMap.set(id, data);

      // headerReadPos += sizeEntryHeader;

    }
    this.sizeHeap = heapReadPos - sizeHeader;
  }


  reset() {
    let superblock = this.superblock = NamedByteBuffer.createSuperblock(SUPERBLOCK_STRUCTURE);
    // let sizeHeader = 1024 * 300;
    if (!fs.existsSync(this.path)) {
      this.indexMap = new Map();
      fs.writeFileSync(this.path, superblock.$getBuffer());
    }

    let stat = fs.statSync(this.path);
    this.fd = fs.openSync(this.path, "r+");

    let data = Buffer.allocUnsafe(stat.size);
    fs.readSync(this.fd, data);


    let b = superblock.$getBuffer();
    data.copy(b, 0, 0, b.byteLength);
    this.sizeHeap = stat.size - superblock.headerLength;

    this.unserialize(data);

  }


  update(id: number, data: Buffer) {
    const found = this.indexMap.get(id);
    if (!found) {
      throw "trying to update non existent id from LogicalMemoryHeap, id: " + id;
    }

    let dLen = data.byteLength - found.byteLength;
    this.sizeHeap += dLen;
    this.heapMap.set(id, data);
  }

  updateString(id: number, string: string) {
    this.update(id, Buffer.from(string, "utf-8"));
  }



  delete(id: number) {
    const found = this.indexMap.get(id);
    if (!found) {
      throw "trying to delete non existent id from LogicalMemoryHeap, id: " + id;
    }
    this.sizeHeap -= found.byteLength;
    this.indexMap.delete(id);
  }



  add(data: Buffer): number {
    let lastId = this.superblock.lastId;
    lastId++;
    this.superblock.lastId = lastId;
    this.heapMap.set(lastId, data);
    this.sizeHeap += data.byteLength;
    return lastId;
  }

  addString(string: string) {
    return this.add(Buffer.from(string, "utf-8"));
  }

  read(id: number): Buffer | undefined {
    return this.heapMap.get(id);
  }

  readString(id: number): string | undefined {
    return this.read(id)?.toString("utf-8");
  }

  static getSortingKey(data: Buffer): Buffer {
    const buf = Buffer.alloc(16);
    // buf.write(string, "utf-8");
    data.copy(buf, 0);
    return buf;
  }

  static compareSortingKeys(key1: Buffer, key2: Buffer) {
    return key1.compare(key2);
  }

  compareString(string: string, id: number) {
    let b = this.read(id);
    if (!b) {
      throw new Error(`LogicalMemoryHeap.compareString; id not found; id:${id} string: ${string}`);
    }
    return LogicalMemoryHeap.compareStringBuffer(string, b, 0, b.byteLength)
  }

  static compareStringBuffer(str: string, buf: Buffer, offset: number, idLen: number): number {
    const target = Buffer.from(str, "utf-8");
    const minLen = Math.min(idLen, target.length);

    for (let i = 0; i < minLen; i++) {
      const a = buf[offset + i];
      const b = target[i];
      if (a !== b) return a - b;
    }

    if (idLen === target.length) return 0;
    return idLen - target.length;
  }

}