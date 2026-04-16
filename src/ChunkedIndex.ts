import fs from "fs";

import NamedByteBuffer, { TSuperblock, TPage } from "./NamedByteBuffer";
import PagesManager from "./PagesManager";



type SMALL_HEADER_STRUCTURE_KEY =
  "numberOfChunks"
  | "numberOfRecords"
  | "minValue"
  | "maxValue"
  | "metaStart";

const SMALL_HEADER_STRUCTURE = new Map<SMALL_HEADER_STRUCTURE_KEY, number>([
  ["numberOfChunks", 2],
  ["numberOfRecords", 4],
  ["minValue", 8],
  ["maxValue", 8],
]);

type HEADER_ENTRY_KEY =
  "page"
  | "length"
  | "minValue"
  | "maxValue"
  | "metaStart";

const HEADER_ENTRY_STRUCTURE = new Map<HEADER_ENTRY_KEY, number>([
  ["page", 2],
  ["length", 4],
  ["minValue", 8],
  ["maxValue", 8],
]);

type PAGE_ENTRY_KEY =
  "value"
  | "id";

const PAGE_ENTRY_STRUCTURE = new Map<PAGE_ENTRY_KEY, number>([
  ["value", 8],
  ["id", 8],
]);



type ChunkMeta = {
  length: number;
  min: number;
  max: number;
  page: number;
}

export default class ChunkedIndex extends PagesManager {
  // public chunks!: Buffer[];



  protected superblock!: TSuperblock<SMALL_HEADER_STRUCTURE_KEY>;
  protected header!: TPage<HEADER_ENTRY_KEY>;
  protected pageCurrent!: TPage<PAGE_ENTRY_KEY>;
  protected fdHeader!: number;
  protected pathHeader!: string;
  constructor(path: string) {


    super({
      path
    });

    this.pageCurrent = NamedByteBuffer.createPage(PAGE_ENTRY_STRUCTURE, 0x2000);
  }

  override reset(): void {
    let pathHeader = this.pathHeader = this.path + ".header";
    this.superblock = NamedByteBuffer.createSuperblock(SMALL_HEADER_STRUCTURE);
    if (!fs.existsSync(pathHeader)) {
      fs.writeFileSync(pathHeader, Buffer.alloc(this.superblock.$getBuffer().byteLength));
    }

    const stat = fs.statSync(pathHeader);
    let rawHeader = Buffer.allocUnsafe(stat.size);
    let fdHeader = this.fdHeader = fs.openSync(pathHeader, "r+");

    fs.readSync(fdHeader, rawHeader, 0, rawHeader.byteLength, 0);
    let b = this.superblock.$getBuffer();
    rawHeader.copy(b, 0, 0, b.byteLength);
    let readPos = b.byteLength;


    let padding = 1000;
    this.header = NamedByteBuffer.createArray(HEADER_ENTRY_STRUCTURE, this.superblock.numberOfRecords + padding);
    b = this.header.$getBuffer();
    rawHeader.copy(b, 0, readPos);


    super.reset();
  }

  get __debug() {
    let r = super.__debug;
    return {
      ...r,
      headerSmall: this.superblock,
      header: this.header,
      pageCurrent: this.pageCurrent,
    }
  }

  findValue(value: number) {
    let { chunkIndex, chunkMeta } = this.findChunkIndex(value);
    if (!chunkMeta || !chunkMeta.length) return;

    this.pageCurrent.$setBuffer(this.readPage(chunkMeta.page));

    const { found, pos } = ChunkedIndex.binarySearchNumber(this.pageCurrent, chunkMeta.length, value);
    if (!found) return;

    return {
      chunkIndex,
      chunkMeta,
      pos,
      page: this.pageCurrent,
    }
  }

  protected async _commitBefore(): Promise<void> {
    let b = this.superblock.$getBuffer();
    await new Promise((resolve) => fs.write(this.fdHeader, b, 0, b.byteLength, 0, resolve));
    let writePos = b.byteLength
    b = this.header.$getBuffer();
    await new Promise((resolve) => fs.write(this.fdHeader, b, 0, b.byteLength, writePos, resolve));
  }

  delete(value: number) {
    const found = this.findValue(value);
    if (!found) return;
    const { page, pos, chunkIndex, chunkMeta } = found;

    page.$shiftLeft(chunkMeta.length, pos);

    chunkMeta.length--;
    if (chunkMeta.length > 0) {
      if (value === chunkMeta.max) {
        // chunkMeta.max = chunk.readDoubleLE(pos - sizeEntry);
        chunkMeta.max = page.value.get(pos);
      } else if (value === chunkMeta.min) {
        chunkMeta.min = page.value.get(pos);
      }
    }
    this.writeChunkMeta(chunkMeta, chunkIndex);
  }

  get(value: any) {
    const found = this.findValue(value);
    if (!found) return;
    return found.page.id.get(found.pos);
  }

  set(value: any, id: number) {
    if (this.superblock.numberOfRecords == 0) {
      this.addChunk("right", value, id);
      return;
    }

    let { chunkIndex, chunkMeta } = this.findChunkIndex(value);
    let p = this.pageCurrent;

    if (chunkMeta) {

      const chunk = this.getWritingPage(chunkMeta.page);

      p.$setBuffer(chunk);

      if (!this.pageCurrent.$canShiftRight(chunkMeta.length)) {
        this.splitChunk(chunkIndex);
        this.set(value, id);
        return;
      }

      const { found, pos } = ChunkedIndex.binarySearchNumber(this.pageCurrent, chunkMeta.length, value);

      if (!found) {
        // shift entries to make space for new entry
        // chunk.copy(chunk, pos + this.sizeEntry, pos, freeSpacePos);
        this.pageCurrent.$shiftRight(chunkMeta.length, pos);
        chunkMeta.length++;
        this.superblock.numberOfRecords++;
      }

      chunkMeta.max = Math.max(value, chunkMeta.max);
      chunkMeta.min = Math.min(value, chunkMeta.min);
      this.writeChunkMeta(chunkMeta, chunkIndex);
      p.id.set(pos, id);
      p.value.set(pos, value);

    } else if (chunkIndex = -1) {
      let meta = this.readChunkMeta(0);
      if (meta.length < this.header.$capacityArray) {
        meta.min = value;
        meta.length++;
        this.writeChunkMeta(meta, 0);

        p.$setBuffer(this.readPage(meta.page))
        p.$shiftRight(meta.length, 0);
        p.id.set(0, id);
        p.value.set(0, value);
      } else {
        this.addChunk("left", value, id);
      }
    } else {
      let chunkIndex = this.superblock.numberOfChunks - 1;
      let meta = this.readChunkMeta(chunkIndex);
      if (meta.length < this.header.$capacityArray) {
        meta.max = value;
        meta.length++;
        this.writeChunkMeta(meta, 0);
        p.$setBuffer(this.readPage(meta.page));
        p.id.set(meta.length - 1, id);
        p.value.set(meta.length - 1, value);
      } else {
        this.addChunk("right", value, id);
      }
    }
  }

  static binarySearchNumber(page: TPage<PAGE_ENTRY_KEY>, length: number, value: number) {

    // file structure for number keyType is [value (8 bytes)][id (8 bytes)]
    let low = 0;
    let high = length - 1;
    let pos = 0;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      pos = mid;
      const currentValue = page.value.get(mid);

      if (currentValue === value) {
        return { pos, found: true };
      }

      if (currentValue < value) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return { pos, found: false };
  }

  writeChunkMeta(meta: ChunkMeta, chunkIndex: number) {
    let oldMeta = this.readChunkMeta(chunkIndex);
    if (this.superblock.numberOfRecords == 0 || oldMeta.min != meta.min && meta.min < this.superblock.minValue) {
      this.superblock.minValue = meta.min;
    }
    if (this.superblock.numberOfRecords == 0 || oldMeta.max != meta.max && meta.max > this.superblock.maxValue) {
      this.superblock.maxValue = meta.max;
    }
    if (oldMeta.length != meta.length) {
      let d = oldMeta.length - meta.length;
      this.superblock.numberOfRecords -= d;
    }

    this.header.length.set(chunkIndex, meta.length);
    this.header.minValue.set(chunkIndex, meta.min);
    this.header.maxValue.set(chunkIndex, meta.max);
    this.header.page.set(chunkIndex, meta.page);
  }

  readChunkMeta(chunkIndex: number): ChunkMeta {
    return {
      length: this.header.length.get(chunkIndex),
      min: this.header.minValue.get(chunkIndex),
      max: this.header.maxValue.get(chunkIndex),
      page: this.header.page.get(chunkIndex),
    }
  }


  findChunkIndex(value: number): { chunkIndex: number; chunkMeta: null | ChunkMeta } {
    const numberOfChunks = this.superblock.numberOfChunks;
    if (value < this.superblock.minValue) return {
      chunkIndex: -1,
      chunkMeta: null,
    };
    if (value > this.superblock.maxValue) return {
      chunkIndex: numberOfChunks + 1,
      chunkMeta: null,
    };

    for (let i = 0; i < numberOfChunks; i++) {
      let min = this.header.minValue.get(i);
      let max = this.header.maxValue.get(i);
      if (min <= value && value <= max) {
        return {
          chunkIndex: i,
          chunkMeta: {
            min,
            max,
            length: this.header.length.get(i),
            page: this.header.page.get(i),
          },
        };
      }
    }
    throw "ChunkedIndex.findChunkIndex: should be unreachable";
  }

  splitChunk(chunkIndex: number) {
    const oldMeta = this.readChunkMeta(chunkIndex);
    const oldChunk = this.readPage(oldMeta.page);
    const newChunk = Buffer.alloc(this.sizePage);
    const sizeEntry = this.pageCurrent.$sizeEntry;

    let splitPointLogical = Math.floor(oldMeta.length / 2);
    let splitPointBytes = splitPointLogical * sizeEntry;
    const newMin = oldChunk.readDoubleLE(splitPointBytes);

    let numberOfChunks = this.superblock.numberOfChunks;
    // add new chunk to the end of the pages
    const newMeta: ChunkMeta = {
      length: oldMeta.length - splitPointLogical,
      max: oldMeta.max,
      min: newMin,
      page: numberOfChunks
    };

    this.superblock.numberOfChunks = numberOfChunks + 1;

    oldChunk.copy(newChunk, 0, splitPointBytes);
    oldMeta.length = splitPointLogical;
    oldMeta.max = oldChunk.readDoubleLE(splitPointBytes - sizeEntry);
    this.writeChunkMeta(oldMeta, chunkIndex);

    this.header.$shiftRight(numberOfChunks, chunkIndex);

    this.writeChunkMeta(newMeta, chunkIndex + 1);
    // we don't need to physically clear old chunk updating meta is enough
    // this.writePage(oldMeta.page, oldChunk);
    this.writePage(newMeta.page, newChunk);
  }

  addChunk(direction: "left" | "right", value: number, id: number) {
    let numberOfChunks = this.superblock.numberOfChunks;
    const meta: ChunkMeta = {
      length: 1,
      max: value,
      min: value,
      page: numberOfChunks,
    };
    let chunkIndex = direction == "left" ? 0 : numberOfChunks;
    if (direction == "left") {
      this.header.$shiftRight(numberOfChunks, 0);
    }
    this.writeChunkMeta(meta, chunkIndex);
    this.superblock.numberOfChunks = numberOfChunks + 1;

    let p = this.pageCurrent;
    p.$setBuffer(Buffer.alloc(this.sizePage));

    p.value.set(0, value);
    p.id.set(0, id);

    this.writePage(meta.page, p.$getBuffer());
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
    let buf = Buffer.alloc(this.header.$sizeEntry);
    let min: number | undefined = undefined;
    fn(buf, 0);
    min = buf.readDoubleLE(0);
    this.superblock.minValue = min;

    let capacity = this.header.$capacityArray;
    let numberOfChunks = Math.ceil(length / capacity);
    for (let p = 0; p < numberOfChunks; p++) {
      let end = (p + 1) * capacity;
      let start = p * capacity;


      fn(buf, start);
      let min = buf.readDoubleLE(0);
      let l = 0;

      page = this.getWritingPage(p);
      pageWritePos = 0;
      for (let i = start; i < end && i < length; i++) {
        fn(buf, i);
        buf.copy(page, pageWritePos, 0, capacity);
        pageWritePos += capacity;
        l++;
      }

      this.writeChunkMeta({
        length: l,
        max: buf.readDoubleLE(0),
        min,
        page: p
      }, p);
    }

    this.superblock.numberOfChunks = numberOfChunks;
    this.superblock.numberOfRecords = length;
  }

}