
import fs from "fs";

function getAbsolutePath(relative: string) {
  return process.cwd() + relative;
}

const HEAP_TOMBSTONE = 0xFFFFFFFF;
const MEMORY_BUFFER_SIZE = 1024 * 1024 * 10;

type HeapSection = {
  sizeCurrent: number;
  sizeMax: number;
  offsetWal: number;
  offsetHeap: number;
}


export default class FilePatchRecord {
  public readonly pathPage: string;
  public readonly pathHeap: string;
  public readonly pathPatchPage: string;
  public readonly pathPatchHeap: string;
  public readonly pageSize: number;

  protected fdPage!: number;
  protected fdHeap!: number;
  protected fdPatchPage!: number;
  protected fdPatchHeap!: number;

  protected pagePatchOffsetMap!: Map<number, number>;
  protected currentWritePosPagePatch: number = 0;
  protected currentWritePosHeapPatch: number = 0;

  protected currentWritePosHeap: number = 0;
  protected heapSections!: Map<number, HeapSection>;


  protected memoryHeap: Buffer | null;
  protected memoryPage: Buffer | null;

  protected memoryBufferSizePage: number;
  protected memoryBufferSizeHeap: number;



  constructor({
    pathPage: pagePath,
    pathHeap: heapPath,
    sizePage: pageSize
  }: {
    pathPage: string;
    pathHeap: string;
    sizePage: number;
  }) {
    this.pathPage = getAbsolutePath(pagePath);
    this.pathHeap = getAbsolutePath(heapPath);

    this.pathPatchPage = getAbsolutePath(pagePath + ".patch");
    this.pathPatchHeap = getAbsolutePath(heapPath + ".patch");

    this.pageSize = pageSize;

    this.reset();
  }

  writePage(page: number, data: Buffer) {
    let writePos = this.currentWritePosPagePatch;
    let isNew = !this.pagePatchOffsetMap.has(page);
    if (!isNew) {
      writePos = this.pagePatchOffsetMap.get(page)!;
    }

    this.virtualWriteWalPage(data, writePos)

    if (isNew) {
      this.pagePatchOffsetMap.set(page, writePos);
      this.currentWritePosPagePatch += this.pageSize;
    }
  }

  readPage(page: number, buf: Buffer) {
    let isTouched = this.pagePatchOffsetMap.has(page);
    if (isTouched) {
      // fs.readSync(this.fdPatchPage, buf, 0, this.pageSize, this.pagePatchOffsetMap.get(page)!);
      this.virtualReadWalPage(buf, this.pagePatchOffsetMap.get(page)!);
    } else {
      fs.readSync(this.fdPage, buf, 0, this.pageSize, page * this.pageSize);
    }

  }

  readHeap(pos: number, byteLength: number): Buffer {
    let isTouched = this.heapSections.has(pos);
    let buf = Buffer.allocUnsafe(byteLength);
    if (!isTouched) {
      fs.readSync(this.fdHeap, buf, 0, byteLength, pos);
      return buf;
    }
    const { offsetWal, sizeCurrent, sizeMax } = this.heapSections.get(pos)!;
    if (sizeCurrent < byteLength || sizeMax < byteLength) {
      throw "wrong byteLength";
    }

    this.virtualReadWalPage(buf, offsetWal);

    return buf;
  }

  writeHeap(data: Buffer, sizeMax?: number, offsetHeap?: number): HeapSection {
    if (!sizeMax) sizeMax = data.byteLength;

    if (data.byteLength > sizeMax) {
      throw "buffer is too big";
    }


    if (offsetHeap === undefined) {
      offsetHeap = this.currentWritePosHeap;
    }

    let isTouched = this.heapSections.has(offsetHeap);

    if (isTouched) {
      let section = this.heapSections.get(offsetHeap)!;
      if (section.sizeMax < sizeMax) {
        throw "attempt to increse maximum size of the heap section instead of appending";
      }
      this.virtualWriteWalHeap(data, section.offsetWal);
      return { ...section };
    }

    let isNew = offsetHeap === this.currentWritePosHeap;


    let section: HeapSection = {
      sizeMax,
      sizeCurrent: data.byteLength,
      offsetHeap,
      offsetWal: this.currentWritePosHeapPatch,
    };
    this.heapSections.set(offsetHeap, section);

    this.currentWritePosHeapPatch += sizeMax;

    if (isNew) {
      this.currentWritePosHeap += sizeMax;
    }

    this.virtualWriteWalHeap(data, section.offsetWal);
    return { ...section };

  }

  virtualReadWalPage(buf: Buffer, walOffset: number) {
    if (this.memoryPage) {
      this.memoryPage.copy(buf, 0, walOffset, this.pageSize);
      return;
    }
    fs.readSync(this.fdPatchPage, buf, 0, this.pageSize, walOffset);
  }

  virtualWriteWalPage(buf: Buffer, walOffset: number) {


    this.spitToDiskIfPage(buf.byteLength + walOffset);

    if (!this.memoryPage) {
      fs.writeSync(this.fdPatchPage, buf, 0, this.pageSize, walOffset);
      return;
    }

    buf.copy(this.memoryPage, walOffset, 0, buf.byteLength);
  }

  virtualReadWalHeap(buf: Buffer, walOffset: number, currentSize: number) {
    if (this.memoryHeap) {
      this.memoryHeap.copy(buf, 0, walOffset, currentSize);
      return;
    }
    fs.readSync(this.fdPatchHeap, buf, 0, currentSize, walOffset);
  }

  virtualWriteWalHeap(buf: Buffer, walOffset: number) {

    this.spitToDiskIfHeap(buf.byteLength + walOffset);

    if (!this.memoryHeap) {
      fs.writeSync(this.fdPatchHeap, buf, 0, buf.byteLength, walOffset);
      return;
    }

    buf.copy(this.memoryHeap, walOffset, 0, buf.byteLength);
  }

  commit() {
    let buf = Buffer.allocUnsafe(this.pageSize);
    for (const [pageNumber, walOffset] of this.pagePatchOffsetMap) {
      this.virtualReadWalPage(buf, walOffset);
      fs.writeSync(this.fdPage, buf, 0, this.pageSize, pageNumber * this.pageSize);
    }


    buf = Buffer.allocUnsafe(1024 * 1024 * 10);
    for (const [offsetHeap, { offsetWal, sizeCurrent, sizeMax }] of this.heapSections) {
      this.virtualReadWalHeap(buf, offsetWal, sizeCurrent);
      fs.writeSync(this.fdHeap, buf, 0, sizeMax, offsetHeap);
    }

    this.reset();
  }

  reset() {
    if (!fs.existsSync(this.pathHeap)) {
      fs.writeFileSync(this.pathHeap, Buffer.alloc(0));
    }
    if (!fs.existsSync(this.pathPage)) {
      fs.writeFileSync(this.pathPage, Buffer.alloc(0));
    }

    let stat = fs.statSync(this.pathHeap);
    this.fdPage = fs.openSync(this.pathPage, "r+");
    this.fdHeap = fs.openSync(this.pathHeap, "r+");

    // fs.writeFileSync(this.walHeapPath, Buffer.alloc(0));
    // fs.writeFileSync(this.walPagePath, Buffer.alloc(0));


    // this.fdPatchPage = fs.openSync(this.pathPatchPage, "w+");
    // this.fdPatchHeap = fs.openSync(this.pathPatchHeap, "w+");

    this.pagePatchOffsetMap = new Map();
    this.currentWritePosPagePatch = 0;
    this.currentWritePosHeap = stat.size;
    this.currentWritePosHeapPatch = 0;

    this.heapSections = new Map();

    this.memoryBufferSizeHeap = MEMORY_BUFFER_SIZE;
    this.memoryBufferSizePage = MEMORY_BUFFER_SIZE;

    this.memoryHeap = Buffer.allocUnsafe(this.memoryBufferSizeHeap);
    this.memoryPage = Buffer.allocUnsafe(this.memoryBufferSizePage);
  }

  spitToDiskIfPage(lastByte: number) {
    if (!this.memoryPage) { //is already working on the disk
      return;
    }
    if (lastByte < this.memoryBufferSizePage) { //the data still fits in the memory
      return;
    }
    this.fdPatchPage = fs.openSync(this.pathPatchPage, "w+");
    fs.writeSync(this.fdPatchPage, this.memoryPage, 0, this.currentWritePosPagePatch, 0);
    this.memoryPage = null;
  }

  spitToDiskIfHeap(lastByte: number) {
    if (!this.memoryHeap) { //is already working on the disk
      return;
    }
    if (lastByte < this.memoryBufferSizeHeap) { //the data still fits in the memory
      return;
    }
    this.fdPatchHeap = fs.openSync(this.pathPatchHeap, "w+");
    fs.writeSync(this.fdPatchHeap, this.memoryHeap, 0, this.currentWritePosHeapPatch, 0);
    this.memoryHeap = null;
  }
}