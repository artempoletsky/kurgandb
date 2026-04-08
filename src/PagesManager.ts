
import fs from "fs";
import { DataBase } from "./db";
import CommitQueue from "./CommitQueue";



function getAbsolutePath(relative: string) {
  return DataBase.workingDirectory + "/" + relative;
}

const DEFAULT_MEMORY_BUFFER_SIZE = 1024 * 1024 * 10;
const PAGE_SIZE = 0x2000;




export default class PagesManager {
  public header!: Buffer;

  public readonly path: string;
  public readonly pathPatch: string;
  public readonly sizePage: number;
  public readonly sizeHeader: number;

  protected fd!: number;
  protected fdPatch!: number;

  protected currentWritePos: number = 0;
  protected currentWritePosPatch: number = 0;

  protected memoryPatch: Buffer | null = null;
  protected memoryBufferSizePatch: number = 0;


  protected pagesCache: Buffer[];
  protected patchOffsets: Map<number, number>;

  protected idCommitQueue: string;

  constructor({
    path,
    sizeHeader: sizeHeader,
    memoryBufferSizePatch,
  }: {
    path: string;
    sizeHeader: number;
    memoryBufferSizePatch?: number;
  }) {
    this.path = getAbsolutePath(path);

    this.pathPatch = getAbsolutePath(path + ".patch");

    this.sizePage = PAGE_SIZE;

    this.sizeHeader = sizeHeader;
    this.pagesCache = [];
    this.patchOffsets = new Map();


    this.memoryBufferSizePatch = memoryBufferSizePatch ?? DEFAULT_MEMORY_BUFFER_SIZE;

    this.idCommitQueue = CommitQueue.register("PagesManager_");
    this.reset();
  }

  reset() {
    if (!fs.existsSync(this.path)) {
      fs.writeFileSync(this.path, Buffer.alloc(this.sizeHeader));
    }

    let stat = fs.statSync(this.path);
    this.fd = fs.openSync(this.path, "r+");

    this.currentWritePos = stat.size;
    this.currentWritePosPatch = 0;

    this.header = Buffer.alloc(this.sizeHeader);
    if (stat.size >= this.sizeHeader) {
      fs.readSync(this.fd, this.header);
    }

    if (this.memoryBufferSizePatch) {
      this.memoryPatch = Buffer.allocUnsafe(this.memoryBufferSizePatch);
    }
  }

  writePage(page: number, data: Buffer) {
    let writePos = this.currentWritePos;
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

  readPage(page: number, buf: Buffer) {
    let isTouched = this.patchOffsets.has(page);
    if (isTouched) {
      // fs.readSync(this.fdPatchPage, buf, 0, this.pageSize, this.pagePatchOffsetMap.get(page)!);
      this.readPatch(buf, this.patchOffsets.get(page)!);
    } else {
      fs.readSync(this.fd, buf, 0, this.sizePage, page * this.sizePage);
    }

  }

  readPatch(buf: Buffer, patchOffset: number) {
    if (this.memoryPatch) {
      this.memoryPatch.copy(buf, 0, patchOffset, this.sizePage);
      return;
    }
    fs.readSync(this.fdPatch, buf, 0, this.sizePage, patchOffset);
  }

  writePatch(buf: Buffer, patchOffset: number) {


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

    buf.copy(this.memoryPatch, patchOffset, 0, buf.byteLength);
  }

  async commit() {
    CommitQueue.start(this.idCommitQueue);

    let buf = Buffer.allocUnsafe(this.sizePage);
    for (const [pageNumber, patchOffset] of this.patchOffsets) {
      this.readPatch(buf, patchOffset);
      // fs.writevSync()
      await (new Promise((resolve) => {
        fs.write(this.fd, buf, 0, this.sizePage, this.sizeHeader + pageNumber * this.sizePage, resolve);
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