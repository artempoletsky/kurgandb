
import fs from "fs";
import CommitQueue from "./CommitQueue";


const DEFAULT_MEMORY_BUFFER_SIZE = 1024 * 1024 * 10;
const PAGE_SIZE = 0x2000;




export default class PagesManager {

  public readonly path: string;
  public readonly pathPatch: string;
  public readonly sizePage: number;

  protected fd!: number;
  protected fdPatch!: number;

  protected currentWritePosPatch: number = 0;

  protected memoryPatch: Buffer | null = null;
  protected memoryBufferSizePatch: number = 0;


  // protected pagesCache: Buffer[];
  protected patchOffsets: Map<number, number>;

  protected idCommitQueue: string;

  protected writingPages: Map<number, Buffer> = new Map();
  protected maxSizeWritingPages = 1;

  get __debug() {
    if (process.env.NODE_ENV !== "test") {
      throw "__debug method should only be used in tests";
    }
    return {
      // heap: this.heap,
      fd: this.fd,
      path: this.path,
      patchOffsets: this.patchOffsets,
      memoryPatch: this.memoryPatch,
      writingPages: this.writingPages,
      writePage: this.writePage.bind(this),
    }
  }

  constructor({
    path,
    memoryBufferSizePatch,
  }: {
    path: string;
    memoryBufferSizePatch?: number;
  }) {
    this.path = path;

    this.pathPatch = path + ".patch";

    this.sizePage = PAGE_SIZE;

    // this.pagesCache = [];
    this.patchOffsets = new Map();


    this.memoryBufferSizePatch = memoryBufferSizePatch ?? DEFAULT_MEMORY_BUFFER_SIZE;

    this.idCommitQueue = CommitQueue.register("PagesManager_");
    this.reset();
  }

  reset() {
    if (!fs.existsSync(this.path)) {
      fs.writeFileSync(this.path, Buffer.alloc(0));
    }

    if (!this.fd) {
      this.fd = fs.openSync(this.path, "r+");
    }

    this.patchOffsets.clear();
    // let stat = fs.statSync(this.path);


    this.currentWritePosPatch = 0;

    if (this.memoryBufferSizePatch) {
      this.memoryPatch = Buffer.alloc(this.memoryBufferSizePatch);
    }
  }

  protected writePage(page: number, data: Buffer) {
    let writePos = this.currentWritePosPatch;
    let isNew = !this.patchOffsets.has(page);
    if (!isNew) {
      writePos = this.patchOffsets.get(page)!;
    }

    this.writePatch(data, writePos)

    if (isNew) {
      this.patchOffsets.set(page, writePos);
      this.currentWritePosPatch += this.sizePage;
    }
  }

  readPage(page: number) {
    if (this.writingPages.has(page)) {
      return this.writingPages.get(page)!;
    }
    let isTouched = this.patchOffsets.has(page);
    if (isTouched) {
      // fs.readSync(this.fdPatchPage, buf, 0, this.pageSize, this.pagePatchOffsetMap.get(page)!);
      return this.readPatch(this.patchOffsets.get(page)!);
    } else {
      let buf = Buffer.allocUnsafe(this.sizePage);
      fs.readSync(this.fd, buf, 0, this.sizePage, page * this.sizePage);
      return buf;
    }
  }

  readPatch(patchOffset: number) {
    if (this.memoryPatch) {
      return this.memoryPatch.subarray(patchOffset, patchOffset + this.sizePage);
    }
    let buf = Buffer.allocUnsafe(this.sizePage);
    fs.readSync(this.fdPatch, buf, 0, this.sizePage, patchOffset);
    return buf;
  }

  async readPatchAsync(buf: Buffer, patchOffset: number) {
    if (this.memoryPatch) {
      this.memoryPatch.copy(buf, 0, patchOffset, patchOffset + this.sizePage);
      return;
    }
    return new Promise((resolve) => {
      fs.read(this.fdPatch, buf, 0, this.sizePage, patchOffset, resolve);
    });
  }

  getWritingPage(page: number) {
    if (this.writingPages.has(page)) {
      return this.writingPages.get(page)!;
    }
    if (this.writingPages.size >= this.maxSizeWritingPages) {
      let entry = this.writingPages.entries().next().value!;
      this.writePage(entry[0], entry[1]);
      this.writingPages.delete(entry[0]);
    }
    let buf = this.readPage(page);
    this.writingPages.set(page, buf);
    return buf;
  }

  protected writePatch(buf: Buffer, patchOffset: number) {


    this.spitToDiskIf(buf.byteLength + patchOffset);

    if (!this.memoryPatch) {
      if (buf.byteLength < this.sizePage) {
        let paddedBuf = Buffer.alloc(this.sizePage);
        buf.copy(paddedBuf, 0, 0, buf.byteLength);
        buf = paddedBuf;
      }
      fs.writeSync(this.fdPatch, buf, 0, this.sizePage, patchOffset);
      return;
    }

    buf.copy(this.memoryPatch, patchOffset);
  }

  protected async _commitBefore(): Promise<void> {

  }

  async commit() {
    CommitQueue.start(this.idCommitQueue);

    for (const [page, buf] of this.writingPages) {
      this.writePage(page, buf);
    }
    this.writingPages.clear();

    await this._commitBefore();

    let buf = Buffer.allocUnsafe(this.sizePage);
    for (const [pageNumber, patchOffset] of this.patchOffsets) {
      await this.readPatchAsync(buf, patchOffset);
      // fs.writevSync()
      await (new Promise((resolve) => {
        fs.write(this.fd, buf, 0, this.sizePage, pageNumber * this.sizePage, resolve);
      }));
    }


    // buf = Buffer.allocUnsafe(1024 * 1024 * 10);
    // for (const [offsetHeap, { offsetWal, sizeCurrent, sizeMax }] of this.heapSections) {
    //   this.virtualReadWalHeap(buf, offsetWal, sizeCurrent);
    //   fs.writeSync(this.fdHeap, buf, 0, sizeMax, offsetHeap);
    // }

    this.reset();
    CommitQueue.end(this.idCommitQueue);
  }



  spitToDiskIf(lastByte: number) {
    if (!this.memoryPatch) { //is already working on the disk
      return;
    }
    if (lastByte < this.memoryBufferSizePatch) { //the data still fits in the memory
      return;
    }
    this.fdPatch = fs.openSync(this.pathPatch, "w+");
    this.writeWithPadding(this.memoryPatch, this.fdPatch, 0, this.currentWritePosPatch);

    this.memoryPatch = null;
  }


  writeWithPadding(buf: Buffer, fd: number, offset: number, minBufferSize: number) {
    let bufferSize = Math.max(buf.byteLength, minBufferSize);
    if (buf.byteLength < minBufferSize) {
      let paddedBuf = Buffer.alloc(minBufferSize);
      buf.copy(paddedBuf, 0, 0, buf.byteLength);
      buf = paddedBuf;
    }
    fs.writeSync(fd, buf, 0, bufferSize, offset);
  }
}