import fs from "fs";
import { DataBase } from "./db";
import TableUtilities from "./TableUtilities";
import PagesManager from "./PagesManager";


type ChunkMeta = {
  length: number;
  min: number;
  max: number;
  page: number;
}

export default class ChunkedIndex extends PagesManager {
  // public chunks!: Buffer[];


  protected readonly sizeEntry: number = 8 + 8;
  protected readonly sizeEntryHeader: number = 2 + 8 + 8 + 2;
  protected readonly sizeSmallHeader: number = 2 + 2 + 8 + 8;
  protected readonly capacityChunk: number;

  public get numberOfChunks(): number {
    return this.header.readUint16LE(0);
  }
  protected set numberOfChunks(value: number) {
    this.header.writeUint16LE(value, 0);
  }

  public get numberOfRecords(): number {
    return this.header.readUint16LE(2);
  }
  protected set numberOfRecords(value: number) {
    this.header.writeUint16LE(value, 2);
  }

  public get minValue(): number {
    return this.header.readDoubleLE(4);
  }
  protected set minValue(value: number) {
    this.header.writeDoubleLE(value, 4);
  }

  public get maxValue(): number {
    return this.header.readDoubleLE(12);
  }
  protected set maxValue(value: number) {
    this.header.writeDoubleLE(value, 12);
  }

  constructor(path: string) {
    super({
      path,
      sizeHeader: 0x2000,
    });
    this.capacityChunk = Math.floor(this.sizePage / this.sizeEntry);
  }

  // override reset(): void {
  //   super.reset();
  //   let stat = fs.statSync(this.path);
  //   // let buf = Buffer.allocUnsafe(stat.size);
  //   // let minSize = this.sizeBigHeader + this.sizePage;
  //   this.header = Buffer.alloc(this.sizeHeader);
  //   if (stat.size >= this.sizeHeader) {
  //     fs.readSync(this.fd, this.header);
  //   }
  //   //   buf = Buffer.alloc(minSize);
  //   // } else {
  //   //   buf = Buffer.allocUnsafe(stat.size);
  //   //   fs.readSync(this.fd, buf);
  //   // }

  //   // this.header = buf.subarray(0, this.sizeBigHeader);
  //   // this.chunks = Array(this.numberOfChunks);
  //   // for (let i = 0; i < this.numberOfChunks; i++) {
  //   //   this.chunks[i] = buf.subarray(
  //   //     this.sizeBigHeader + i * this.sizePage,
  //   //     this.sizeBigHeader + (i + 1) * this.sizePage);
  //   // }

  // }

  findValue(value: number) {
    let { chunkIndex, chunkMeta } = this.findChunkIndex(value);
    if (!chunkMeta || !chunkMeta.length) return;

    const chunk = this.readPage(chunkMeta.page);

    const { found, pos } = ChunkedIndex.binarySearchNumber(value, chunk, this.sizeEntry, chunkMeta.length);
    if (!found) return;

    return {
      chunkIndex,
      chunkMeta,
      pos,
      chunk,
    }
  }

  delete(value: number) {
    const found = this.findValue(value);
    if (!found) return;
    const { chunk, pos, chunkIndex, chunkMeta } = found;
    chunk.copy(chunk, pos, pos + this.sizeEntry);
    chunkMeta.length--;
    if (chunkMeta.length > 0) {
      if (value === chunkMeta.max) {
        chunkMeta.max = chunk.readDoubleLE(pos - this.sizeEntry);
      } else if (value === chunkMeta.min) {
        chunkMeta.min = chunk.readDoubleLE(pos);
      }
    }
    this.writeChunkMeta(chunkMeta, chunkIndex);
  }

  get(value: number) {
    const found = this.findValue(value);
    if (!found) return;
    return found.chunk.readDoubleLE(found.pos + 8);
  }

  set(value: number, id: number) {
    if (this.numberOfRecords == 0) {
      this.addChunk("right", value, id);
      return;
    }

    let { chunkIndex, chunkMeta } = this.findChunkIndex(value);
    if (chunkMeta) {

      const chunk = this.getWritingPage(chunkMeta.page);

      let freeSpacePos = chunkMeta.length * this.sizeEntry;
      if (freeSpacePos + this.sizeEntry > 0x2000) {
        this.splitChunk(chunkIndex);
        this.set(value, id);
        return;
      }

      const { found, pos } = ChunkedIndex.binarySearchNumber(value, chunk, this.sizeEntry, chunkMeta.length);

      if (!found) {
        // shift entries to make space for new entry
        chunk.copy(chunk, pos + this.sizeEntry, pos, freeSpacePos);
        chunkMeta.length++;
        this.numberOfRecords++;
      }

      chunkMeta.max = Math.max(value, chunkMeta.max);
      chunkMeta.min = Math.min(value, chunkMeta.min);
      this.writeChunkMeta(chunkMeta, chunkIndex);
      this.writeToChunk(value, id, chunk, pos);


    } else if (chunkIndex = -1) {
      let meta = this.readChunkMeta(0);
      if (meta.length < this.capacityChunk) {
        meta.min = value;
        meta.length++;
        this.writeChunkMeta(meta, 0);

        const chunk = this.readPage(meta.page);

        chunk.copy(chunk, 0, this.sizeEntry);
        this.writeToChunk(value, id, chunk, 0);
      } else {
        // this.splitChunk(0);
        this.set(value, id);
      }
    } else {
      let chunkIndex = this.numberOfChunks - 1;
      let meta = this.readChunkMeta(chunkIndex);
      if (meta.length < this.capacityChunk) {
        meta.max = value;
        meta.length++;
        this.writeChunkMeta(meta, 0);

        const chunk = this.readPage(meta.page);

        // chunk.copy(chunk, 0, this.sizeEntry);
        let pos = meta.length * this.sizeEntry;
        this.writeToChunk(value, id, chunk, pos);
      } else {
        // this.splitChunk(chunkIndex);
        this.set(value, id);
      }
    }
  }

  static binarySearchNumber(value: number, buffer: Buffer, entrySize: number, length: number) {

    // file structure for number keyType is [value (8 bytes)][id (8 bytes)]
    let low = 0;
    let high = length - 1;
    let pos = 0;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      pos = mid * entrySize;
      const currentId = buffer.readDoubleLE(pos);

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

  writeChunkMeta(meta: ChunkMeta, chunkIndex: number) {
    let oldMeta = this.readChunkMeta(chunkIndex);
    if (this.numberOfRecords == 0 || oldMeta.min != meta.min && meta.min < this.minValue) {
      this.minValue = meta.min;
    }
    if (this.numberOfRecords == 0 || oldMeta.max != meta.max && meta.max > this.maxValue) {
      this.maxValue = meta.max;
    }
    if (oldMeta.length != meta.length) {
      let d = oldMeta.length - meta.length;
      this.numberOfRecords -= d;
    }

    const pos = this.sizeSmallHeader + chunkIndex * this.sizeEntryHeader;
    this.header.writeUint16LE(meta.length, pos);
    this.header.writeDoubleLE(meta.min, pos + 2);
    this.header.writeDoubleLE(meta.max, pos + 10);
    this.header.writeUint16LE(meta.page, pos + 18);
  }

  readChunkMeta(chunkIndex: number): ChunkMeta {
    const pos = this.sizeSmallHeader + chunkIndex * this.sizeEntryHeader;

    const length = this.header.readUint16LE(pos);
    const min = this.header.readDoubleLE(pos + 2);
    const max = this.header.readDoubleLE(pos + 10);
    const page = this.header.readUint16LE(pos + 18);
    return {
      length,
      min,
      max,
      page,
    }
  }


  findChunkIndex(value: number): { chunkIndex: number; chunkMeta: null | ChunkMeta } {
    const numberOfChunks = this.numberOfChunks;
    if (value < this.minValue) return {
      chunkIndex: -1,
      chunkMeta: null,
    };
    if (value > this.maxValue) return {
      chunkIndex: numberOfChunks + 1,
      chunkMeta: null,
    };

    for (let i = 0; i < numberOfChunks; i++) {
      const meta = this.readChunkMeta(i);
      if (meta.min <= value && value <= meta.max) {
        return {
          chunkIndex: i,
          chunkMeta: meta,
        };
      }
    }
    throw "should be unreachable";
  }

  splitChunk(chunkIndex: number) {
    const oldMeta = this.readChunkMeta(chunkIndex);
    const oldChunk = this.readPage(oldMeta.page);
    const newChunk = Buffer.alloc(this.sizePage);

    let splitPointLogical = Math.floor(oldMeta.length / 2);
    let splitPointBytes = splitPointLogical * this.sizeEntry;
    const newMin = oldChunk.readDoubleLE(splitPointBytes);

    // add new chunk to the end of the pages
    const newMeta: ChunkMeta = {
      length: oldMeta.length - splitPointLogical,
      max: oldMeta.max,
      min: newMin,
      page: this.numberOfChunks
    };
    this.numberOfChunks++;

    oldChunk.copy(newChunk, 0, splitPointBytes);
    oldMeta.length = splitPointLogical;
    oldMeta.max = oldChunk.readDoubleLE(splitPointBytes - this.sizeEntry);
    this.writeChunkMeta(oldMeta, chunkIndex);

    this.shiftRightHeader(chunkIndex);

    this.writeChunkMeta(newMeta, chunkIndex + 1);
    // we don't need to physically clear old chunk updating meta is enough
    // this.writePage(oldMeta.page, oldChunk);
    this.writePage(newMeta.page, newChunk);
  }

  addChunk(direction: "left" | "right", value: any, id: any) {
    const meta: ChunkMeta = {
      length: 1,
      max: value,
      min: value,
      page: this.numberOfChunks,
    };
    let chunkIndex = direction == "left" ? 0 : this.numberOfChunks;
    if (direction == "left") {
      this.shiftRightHeader(0);
    }
    this.writeChunkMeta(meta, chunkIndex);
    this.numberOfChunks++;

    const chunk = Buffer.alloc(this.sizePage);
    this.writeToChunk(value, id, chunk, 0);

    this.writePage(meta.page, chunk);
  }

  shiftRightHeader(chunkIndex: number) {
    const pos = this.sizeSmallHeader + chunkIndex * this.sizeEntry;
    this.header.copy(this.header, pos + this.sizeEntry, pos);
  }

  writeToChunk(value: any, id: any, chunk: Buffer, pos: number) {
    chunk.writeDoubleLE(value, pos);
    chunk.writeDoubleLE(id, pos + 8);
  }

  // fastFill(keyValuePairs: { key: number, offset: number }[]): void
  fastFill(
    fn: (buf: Buffer, index: number) => void,
    length: number): void {

    // let length: number;
    // let fn = arg1;
    // if (typeof arg2 === "number") {
    //   length = arg2;
    // } else {
    //   length = arg1.length;
    // }



    let page = this.getWritingPage(0);
    let pageWritePos = 0;
    let buf = Buffer.alloc(this.sizeEntry);
    let min: number | undefined = undefined;
    fn(buf, 0);
    min = buf.readDoubleLE(0);
    this.minValue = min;

    let numberOfChunks = Math.ceil(length / this.capacityChunk);
    for (let p = 0; p < numberOfChunks; p++) {
      let end = (p + 1) * this.capacityChunk;
      let start = p * this.capacityChunk;

      fn(buf, start);
      let min = buf.readDoubleLE(0);
      let l = 0;

      page = this.getWritingPage(p);
      pageWritePos = 0;
      for (let i = start; i < end && i < length; i++) {
        fn(buf, i);
        buf.copy(page, pageWritePos, 0, this.sizeEntry);
        pageWritePos += this.sizeEntry;
        l++;
      }

      this.writeChunkMeta({
        length: l,
        max: buf.readDoubleLE(0),
        min,
        page: p
      }, p);
    }

    this.numberOfChunks = numberOfChunks;
    this.numberOfRecords = length;
  }

}