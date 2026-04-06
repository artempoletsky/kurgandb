import * as fs from "fs";
import { DataBase } from "./db";
import TableUtilities from "./TableUtilities";

type STRUCTURE_KEY = "id"
  | "page"
  | "offset";

const HEADER_STRUCTURE: { key: STRUCTURE_KEY, length: number }[] = [];
HEADER_STRUCTURE.push({ key: "id", length: 8 });
HEADER_STRUCTURE.push({ key: "page", length: 2 });
HEADER_STRUCTURE.push({ key: "offset", length: 50 });

let _currentOffset = 0;
const pageHeaderOffsets = HEADER_STRUCTURE.reduce((result, e) => {
  _currentOffset += e.length;
  result[e.key] = _currentOffset;
  return result;
}, {} as Record<HEADER_STRUCTURE_KEY, number>);


const MIN_BUFFER_SIZE = 1024 * 1024; // 1MB
const TOMBSTONE = 0xFFFFFFFF;

export class IndexOneNumber extends Page {

  constructor(path: string)
  constructor(tableUtils: TableUtilities, columnName: string)
  constructor(a1: string | TableUtilities, a2?: string) {
    let path: string;
    if (typeof a1 == "object") {
      // throw new Error("Not implemented yet");
      path = a1.getIndexDictDir(a2!) + "index.bin";
    } else {
      path = a1;
    }

    this.path = DataBase.workingDirectory + "/" + path;
    this.reset();
  }

  protected path: string;
  protected buffer!: Buffer;

  protected bufferLength = 0;
  protected metaIndexLength = 0;

  get(id: number): number {
    const { found, pos } = this.binarySearchNumber(id);

    if (found) {
      const result = this.buffer.readUInt32BE(pos + 4);
      if (result === TOMBSTONE) {
        return -1;
      }
      return result;
    }

    return -1;
  }


  widenBuffers() {
    this.buffer = Buffer.concat([this.buffer, Buffer.allocUnsafe(this.buffer.length)]);
  }

  binarySearchNumber(value: number) {

    // file structure for number keyType is [value (16 bytes)][id (16 bytes)]
    const entrySize = 32;
    const length = Math.floor(this.bufferLength / entrySize);

    let low = 0;
    let high = length - 1;
    let pos = 0;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      pos = mid * entrySize;
      const currentId = this.buffer.readDoubleLE(pos);

      if (currentId === value) {
        return { pos, found: true };
      }

      if (currentId < value) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return { pos, found: false };
  }

  set(value: number, id: number) {
    let entrySize = 32;
    let keySize = 4;

    const { found, pos } = this.binarySearchNumber(value);
    if (found) {
      this.buffer.writeDoubleLE(id, pos + 16);
    } else {

      let newBufferLength = this.bufferLength + entrySize;
      if (newBufferLength > this.buffer.length) {
        this.widenBuffers();
      }
      // shift entries to make space for new entry
      this.buffer.copy(this.buffer, pos + entrySize, pos, this.bufferLength);

      this.buffer.writeUInt32BE(value as number, pos);
      this.buffer.writeUInt32BE(id, pos + 4);
      this.bufferLength = newBufferLength;
    }

  }

  delete(id: number) {
    const { found, pos } = this.binarySearchNumber(id);
    if (found) {
      this.buffer.writeUInt32BE(TOMBSTONE, pos + 4);
    }
  }

  fastFill(keyValuePairs: { key: number, offset: number }[], startingBufferSize: number): void
  fastFill(fn: (index: number) => Buffer, length: number, startingBufferSize: number): void
  fastFill(arg1: any, arg2: any, arg3?: any) {
    let startingBufferSize: number;
    let length: number;

    if (typeof arg3 === "number") {
      startingBufferSize = arg3;
      length = arg2;
    } else {
      startingBufferSize = arg2;
      length = arg1.length;
    }

    this.buffer = Buffer.allocUnsafe(startingBufferSize);
    this.bufferLength = 0;

    for (let i = 0; i < length; i++) {

      let resultLength = this.bufferLength + 8;

      if (resultLength > this.buffer.length) {
        this.widenBuffers();
      }

      let buf: Buffer;
      if (typeof arg1 === "function") {
        buf = arg1(i);
      } else {
        buf = Buffer.allocUnsafe(8);
        buf.writeUInt32BE(arg1[i].key, 0);
        buf.writeUInt32BE(arg1[i].offset, 4);
      }

      this.buffer.set(buf, this.bufferLength);
      this.bufferLength = resultLength
    }

  }

  compact() {
    const newBuffer = Buffer.allocUnsafe(this.buffer.length);

    const entrySize = 8;
    let writePos = 0;
    for (let readPos = 0; readPos < this.bufferLength; readPos += entrySize) {
      const offset = this.buffer.readUInt32BE(readPos + 4);
      if (offset !== TOMBSTONE) {
        newBuffer.copy(this.buffer, writePos, readPos, readPos + entrySize);
        writePos += entrySize;
      }
    }
    this.bufferLength = writePos;
    this.buffer = newBuffer;
  }

  save() {
    this.compact();
    fs.writeFileSync(this.path, this.buffer.subarray(0, this.bufferLength));
  }

  reset() {
    if (!fs.existsSync(this.path)) {
      fs.writeFileSync(this.path, Buffer.alloc(0));
    }
    const stats = fs.statSync(this.path);
    const fileSize = stats.size;

    this.buffer = Buffer.allocUnsafe(Math.max(fileSize * 1.5, MIN_BUFFER_SIZE));

    this.bufferLength = fileSize;

    fs.readSync(fs.openSync(this.path, 'r'), this.buffer, 0, this.bufferLength, 0);
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