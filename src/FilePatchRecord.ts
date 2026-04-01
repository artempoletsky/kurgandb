
import fs from "fs";

function getAbsolutePath(relative: string) {
  return process.cwd() + relative;
}

const HEAP_TOMBSTONE = 0xFFFFFFFF;

type HeapSection = {
  sizeCurrent: number;
  sizeMax: number;
  offsetWal: number;
  offsetHeap: number;
}


export default class FilePatchRecord {
  public readonly pathPage: string;
  public readonly pathHeap: string;
  public readonly walPagePath: string;
  public readonly walHeapPath: string;
  public readonly pageSize: number;

  protected fdPage!: number;
  protected fdHeap!: number;
  protected fdWalPage!: number;
  protected fdWalHeap!: number;

  protected pageWalOffsetMap!: Map<number, number>;
  protected currentPageWalWritePos: number = 0;
  protected currentHeapWalWritePos: number = 0;

  protected currentHeapWritePos: number = 0;
  protected heapSections!: Map<number, HeapSection>;

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

    this.walPagePath = getAbsolutePath(pagePath + ".patch");
    this.walHeapPath = getAbsolutePath(heapPath + ".patch");

    this.pageSize = pageSize;

    this.reset();
  }

  writePage(page: number, data: Buffer) {
    let writePos = this.currentPageWalWritePos;
    let isNew = !this.pageWalOffsetMap.has(page);
    if (!isNew) {
      writePos = this.pageWalOffsetMap.get(page)!;
    }

    fs.writeSync(this.fdWalPage, data, 0, data.byteLength, writePos);

    if (isNew) {
      this.pageWalOffsetMap.set(page, writePos);
      this.currentPageWalWritePos += this.pageSize;
    }
  }

  readPage(page: number, buf: Buffer) {
    let isTouched = this.pageWalOffsetMap.has(page);
    if (isTouched) {
      fs.readSync(this.fdWalPage, buf, 0, this.pageSize, this.pageWalOffsetMap.get(page)!);
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

    fs.readSync(this.fdWalHeap, buf, 0, byteLength, offsetWal);
    return buf;
  }

  writeHeap(data: Buffer, sizeMax?: number, offsetHeap?: number): HeapSection {
    if (!sizeMax) sizeMax = data.byteLength;

    if (data.byteLength > sizeMax) {
      throw "buffer is too big";
    }


    if (offsetHeap === undefined) {
      offsetHeap = this.currentHeapWritePos;
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

    let isNew = offsetHeap === this.currentHeapWritePos;


    let section: HeapSection = {
      sizeMax,
      sizeCurrent: data.byteLength,
      offsetHeap,
      offsetWal: this.currentHeapWalWritePos,
    };
    this.heapSections.set(offsetHeap, section);

    this.currentHeapWalWritePos += sizeMax;

    if (isNew) {
      this.currentHeapWritePos += sizeMax;
    }

    this.virtualWriteWalHeap(data, section.offsetWal);
    return { ...section };

  }

  virtualReadWalPage(buf: Buffer, walOffset: number) {
    fs.readSync(this.fdWalPage, buf, 0, this.pageSize, walOffset);

    //todo implement reading from memory
  }

  virtualWriteWalPage(buf: Buffer, walOffset: number) {
    fs.writeSync(this.fdWalPage, buf, 0, this.pageSize, walOffset);

    //todo implement reading from memory
  }

  virtualReadWalHeap(buf: Buffer, walOffset: number, currentSize: number) {
    fs.readSync(this.fdWalHeap, buf, 0, currentSize, walOffset);
    //todo implement reading from memory
  }

  virtualWriteWalHeap(buf: Buffer, walOffset: number) {
    fs.writeSync(this.fdWalHeap, buf, 0, buf.byteLength, walOffset);
    //todo implement reading from memory
  }

  commit() {
    let buf = Buffer.allocUnsafe(this.pageSize);
    for (const [pageNumber, walOffset] of this.pageWalOffsetMap) {
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


    this.fdWalPage = fs.openSync(this.walPagePath, "w+");
    this.fdWalHeap = fs.openSync(this.walHeapPath, "w+");

    this.pageWalOffsetMap = new Map();
    this.currentPageWalWritePos = 0;
    this.currentHeapWritePos = stat.size;
    this.currentHeapWalWritePos = 0;

    this.heapSections = new Map();
  }
}