
import LogicalMemoryHeap from "./LogicalMemoryHeap";
import NamedByteBuffer, { TPageView, TSuperblock } from "./PageViewArray";
import PagesManager from "./PagesManager";


type SUPERBLOCK_KEY =
  "numberOfChunks"
  | "numberOfRecords"
  | "minValue"
  | "maxValue";

const SUPERBLOCK_STRUCTURE = new Map<SUPERBLOCK_KEY, number>([
  ["numberOfChunks", 2],
  ["numberOfRecords", 4],
  ["minValue", 8],
  ["maxValue", 8],
]);

type HEADER_KEY =
  "page"
  | "numberOfRecords"
  | "minValue"
  | "maxValue";

const HEADER_STRUCTURE = new Map<HEADER_KEY, number>([
  ["page", 2],
  ["numberOfRecords", 4],
  ["minValue", 8],
  ["maxValue", 8],
]);

type PAGE_ENTRY_KEY =
  "idHeap"
  | "value";

const PAGE_STRUCTURE = new Map<PAGE_ENTRY_KEY, number>([
  ["idHeap", 8],
  ["value", 8],
]);

// stores multiple offsets for a single number key
// one to many relationship between key and offsets
export class IndexManyNumber {

  protected heap!: LogicalMemoryHeap;
  constructor(path: string)
  constructor(tableName: string, columnName: string)
  constructor(a1: string, a2?: string) {
    const path = a1;
    if (a2) {
      throw new Error("Not implemented yet");
    }

    this.pathFixed = path;
    this.pathOffsets = path + "_var";
    this.reset();
  }

  protected pathFixed: string;
  protected pathOffsets;

  protected bufferFixed!: Buffer; // file structure is [key (4 bytes)][start+tombstone (4 bytes)][len (2 bytes)]
  protected bufferOffsets!: Buffer; // file structure is [offset (4 bytes)]

  protected lengthBufferFixed = 0;
  protected lengthBufferOffset = 0;

  getFixedBuffer() {
    return this.bufferFixed.subarray(0, this.lengthBufferFixed);
  }

  getOffsetsBuffer() {
    return this.bufferOffsets.subarray(0, this.lengthBufferOffset);
  }

  getFixedBufferLength() {
    return this.lengthBufferFixed;
  }

  getVariableBufferLength() {
    return this.lengthBufferOffset;
  }

  findChunx(value: number): IterableIterator<[number, TPageView<PAGE_ENTRY_KEY>]> {
    return {
      [Symbol.iterator]() {
        return this;
      },
      next(): IteratorResult<[number, TPageView<PAGE_ENTRY_KEY>]> {
        return { done: true, value: undefined as any };
      }
    };
  }

  static binarySearchChunk(chunk: TPageView<"value">, length: number, value: number) {
    let lo = 0;
    let hi = length - 1;
    let mid = 0;

    while (lo <= hi) {
      mid = Math.floor((lo + hi) / 2);
      let v = chunk.value.get(mid);
      if (v === value) {
        return {
          found: true,
          pos: mid,
        };
      }
      if (v < value) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }

    return {
      found: false,
      pos: mid,
    };
  }

  findHeapId(value: number): number {
    for (const [len, chunk] of this.findChunx(value)) {

    }
  }

  get(value: number) {
    let heapId = this.findHeapId(value);
    if (heapId === -1) return undefined;
    return new Float64Array(this.heap.read(heapId)!);
  }


  widenBuffers() {
    this.bufferFixed = Buffer.concat([this.bufferFixed, Buffer.allocUnsafe(this.bufferFixed.length)]);
    this.bufferOffsets = Buffer.concat([this.bufferOffsets, Buffer.allocUnsafe(this.bufferOffsets.length)]);
  }


  readOffsetsAtPositionInFixedBuffer(pos: number) {

    const len = this.bufferFixed.readUInt16LE(pos + LEN_OFFSET);
    if (len === 0) return new Uint32Array();

    const start = this.bufferFixed.readUInt32LE(pos + START_OFFSET);
    const slice = this.bufferOffsets.subarray(start, start + len * 4);




    return new Uint32Array(slice.buffer,
      slice.byteOffset,
      len);
  }

  binarySearch(value: number) {
    const length = Math.floor(this.lengthBufferFixed / FIXED_RECORD_SIZE);
    let low = 0;
    let high = length - 1;
    let pos = 0;
    let idPos = 0;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      pos = mid * FIXED_RECORD_SIZE;
      // file structure is [start (4 bytes)][len (4 bytes)]
      // const offset = this.buffer.readUInt32LE(pos);

      const currentId = this.bufferFixed.readUInt32LE(pos);
      if (currentId == value) {
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

  appendToOffsetsBuffer(buf: Buffer) {
    if (this.lengthBufferOffset + buf.length > this.bufferOffsets.length) {
      let toAllocate = Math.max(MIN_BUFFER_SIZE, buf.length);
      this.bufferOffsets = Buffer.concat([this.bufferOffsets, Buffer.allocUnsafe(toAllocate)]);
    }
    this.bufferOffsets.set(buf, this.lengthBufferOffset);
    this.lengthBufferOffset += buf.length;
  }

  setArray(value: number, offsets: Uint32Array) {

    const { pos, found } = this.binarySearch(value);


    let toAppend = Buffer.from(offsets.buffer, offsets.byteOffset,
      offsets.byteLength);

    // let toAppend = Buffer.allocUnsafe(offsets.length * 4);

    // for (let i = 0; i < offsets.length; i++) {
    //   toAppend.writeUInt32LE(offsets[i], i * 4);
    // }

    if (found) {
      const currentLen = this.bufferFixed.readUInt32LE(pos + LEN_OFFSET);

      if (currentLen >= offsets.length) {
        this.bufferOffsets.set(toAppend, this.bufferFixed.readUInt32LE(pos + START_OFFSET));
        this.bufferFixed.writeUInt16LE(offsets.length, pos + LEN_OFFSET);
        return;
      }


    }


    this.bufferFixed.writeUInt32LE(value, pos);
    this.bufferFixed.writeUInt32LE(this.lengthBufferOffset, pos + START_OFFSET);
    this.bufferFixed.writeUInt16LE(offsets.length, pos + LEN_OFFSET);
    this.lengthBufferFixed += FIXED_RECORD_SIZE;
    this.appendToOffsetsBuffer(toAppend);

  }



  delete(value: number) {

    const { pos, found } = this.binarySearch(value);

    if (!found) return;

    this.bufferFixed.writeUInt32LE(0, pos + LEN_OFFSET);
  }

  fastFill(keyValuePairs: { key: number, offsets: number[] }[], startingBufferSize?: number): void
  fastFill(fn: (index: number) => Buffer, length: number, startingBufferSize?: number): void
  fastFill(arg1: any, arg2: any, arg3?: any) {
    let startingBufferSize: number;
    let length: number;

    if (typeof arg3 === "number") {
      startingBufferSize = arg3 || 0;
      length = arg2;
    } else {
      startingBufferSize = arg2 || 0;
      length = arg1.length;
    }

    startingBufferSize = Math.max(startingBufferSize, MIN_BUFFER_SIZE, length * FIXED_RECORD_SIZE * 2);

    this.bufferFixed = Buffer.allocUnsafe(startingBufferSize);
    this.lengthBufferFixed = 0;

    this.bufferOffsets = Buffer.allocUnsafe(startingBufferSize);
    this.lengthBufferOffset = 0;

    for (let i = 0; i < length; i++) {

      let buf: Buffer;
      if (typeof arg1 === "function") {
        buf = arg1(i);
      } else {

        buf = Buffer.allocUnsafe(FIXED_RECORD_SIZE);
        buf.writeUInt32LE(arg1[i].key, 0);
        buf.writeUInt32LE(this.lengthBufferOffset, START_OFFSET);
        buf.writeUInt16LE(arg1[i].offsets.length, LEN_OFFSET);
      }

      this.appendToOffsetsBuffer(Buffer.from(new Uint32Array(arg1[i].offsets).buffer));
      this.bufferFixed.set(buf, this.lengthBufferFixed);
      this.lengthBufferFixed += FIXED_RECORD_SIZE;
    }

  }

  readFixedRecord(position: number) {
    return {
      value: this.bufferFixed.readUInt32LE(position),
      len: this.bufferFixed.readUInt16LE(position + LEN_OFFSET),
      start: this.bufferFixed.readUInt32LE(position + START_OFFSET)
    }
  }

  compact() {
    const newFixedBuffer = Buffer.allocUnsafe(this.bufferFixed.length);

    const newOffsetsBuffer = Buffer.allocUnsafe(this.bufferOffsets.length);

    let fixedWritePos = 0;
    let variableWritePos = 0;


    for (let i = 0; i < this.lengthBufferFixed / FIXED_RECORD_SIZE; i++) {
      const recordPos = i * FIXED_RECORD_SIZE;


      const len = this.bufferFixed.readUInt16LE(recordPos + LEN_OFFSET);
      if (len !== 0) {

        const key = this.bufferFixed.readUInt32LE(recordPos);
        const start = this.bufferFixed.readUInt32LE(recordPos + START_OFFSET);


        this.bufferFixed.copy(newFixedBuffer, fixedWritePos, recordPos, recordPos + FIXED_RECORD_SIZE);
        fixedWritePos += FIXED_RECORD_SIZE;

        this.bufferOffsets.copy(newOffsetsBuffer, variableWritePos, start, start + len * 4);
        variableWritePos += len * 4;
      }
    }

    this.bufferFixed = newFixedBuffer;
    this.bufferOffsets = newOffsetsBuffer;
    this.lengthBufferFixed = fixedWritePos;
    this.lengthBufferOffset = variableWritePos;

  }


  save() {
    this.compact();
    fs.writeFileSync(this.pathFixed, this.bufferFixed.subarray(0, this.lengthBufferFixed));
    fs.writeFileSync(this.pathOffsets, this.bufferOffsets.subarray(0, this.lengthBufferOffset));
  }

  reset() {
    if (!fs.existsSync(this.pathFixed)) {
      fs.writeFileSync(this.pathFixed, Buffer.alloc(0));
    }
    const stats = fs.statSync(this.pathFixed);
    const fileSize = stats.size;

    this.bufferFixed = Buffer.allocUnsafe(Math.max(fileSize * 1.5, MIN_BUFFER_SIZE));

    this.lengthBufferFixed = fileSize;

    fs.readSync(fs.openSync(this.pathFixed, 'r'), this.bufferFixed, 0, this.lengthBufferFixed, 0);


    if (!fs.existsSync(this.pathOffsets)) {
      fs.writeFileSync(this.pathOffsets, Buffer.alloc(0));
    }
    const variableFileSize = fs.statSync(this.pathOffsets).size;
    this.lengthBufferOffset = variableFileSize;
    this.bufferOffsets = Buffer.allocUnsafe(Math.max(variableFileSize * 2, MIN_BUFFER_SIZE));
    fs.readSync(fs.openSync(this.pathOffsets, 'r'), this.bufferOffsets, 0, this.lengthBufferOffset, 0);

  }

}