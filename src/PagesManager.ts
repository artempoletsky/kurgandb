
import fs from "fs";
import CommitQueue from "./CommitQueue";
import PatchFile from "./PatchFile";
import Superblock, { TSuperblock } from "./Superblock";
import { TPage } from "./NamedByteBuffer";


const PAGE_SIZE = 0x2000;

type SUPERBLOCK_KEYS = "lastPage" | "buriedHere" | "prevSematary";
const SUPERBLOCK_STRUCTURE = new Map<SUPERBLOCK_KEYS, number>([
  ["lastPage", 4],
  ["buriedHere", 4],
  ["prevSematary", 4],
]);

type EMPTY_PAGES_KEYS = "page";
const EMPTY_PAGES_STRUCTURE = new Map<EMPTY_PAGES_KEYS, number>([
  ["page", 4],
]);



export default class PagesManager {

  public readonly path: string;
  public readonly sizePage: number;

  protected file: PatchFile;

  protected idCommitQueue: string;

  protected writingPages: Map<number, Buffer> = new Map();
  protected readingPages: Map<number, Buffer> = new Map();
  protected maxSizeWritingPages = 1;
  protected maxSizeReadingPages = 1;

  public superblock!: TSuperblock<SUPERBLOCK_KEYS>;
  public freePages!: TPage<EMPTY_PAGES_KEYS>;

  get __debug() {
    if (process.env.NODE_ENV !== "test") {
      throw "__debug method should only be used in tests";
    }
    return {
      // heap: this.heap,
      path: this.path,
      writingPages: this.writingPages,
      writePage: this.writePage.bind(this),
    }
  }

  constructor({
    path,
  }: {
    path: string;
  }) {
    this.path = path;

    // this.pathPatch = path + ".patch";

    this.sizePage = PAGE_SIZE;

    // this.pagesCache = [];


    // this.memoryBufferSizePatch = memoryBufferSizePatch ?? DEFAULT_MEMORY_BUFFER_SIZE;
    this.file = new PatchFile(path);
    this.idCommitQueue = CommitQueue.register("PagesManager_");
    this.reset();
  }

  reset() {
    this.file.reset();
    let p0 = this.readPage(0);
    this.superblock = Superblock.fromPageTail(SUPERBLOCK_STRUCTURE, p0);
    // this.freePages = 
  }

  deletePage(index: number) {
    if (index <= 0) {
      throw new Error("PagesManager;deletePage: wrong page index");
    }

    let { prevSematary: lastSematary } = this.superblock;

    let semataryCapacity = 123;

    let page = this.readPage(lastSematary);
    let sb = Superblock.fromPageTail(SUPERBLOCK_STRUCTURE, page);
    let buriedHere = sb.buriedHere;
    if (buriedHere >= semataryCapacity) {
      sb.buriedHere = 0;
      sb.prevSematary = lastSematary;
      let p = this.getWritingPage(index);
      sb.$copyToEnd(p);
      this.superblock.prevSematary = index;
      this.saveSuperblock();
      return;
    }
    let p = this.getWritingPage(lastSematary);
    this.freePages.$setBuffer(p);
    this.freePages.page.set(buriedHere, index);
    sb.buriedHere = ++buriedHere;
    sb.$copyToEnd(p);
  }

  saveSuperblock() {
    let wp = this.getWritingPage(0);
    this.superblock.$copyToEnd(wp);
  }

  getFreePageId(): number {
    let { prevSematary: lastSematary, buriedHere } = this.superblock;
    if (lastSematary != 0) {
      let p = this.readPage(lastSematary);
      let sb = Superblock.fromPageTail(SUPERBLOCK_STRUCTURE, p);
      let { buriedHere, prevSematary } = sb;
      if (buriedHere == 0) {
        this.superblock.prevSematary = prevSematary;
        this.saveSuperblock();
        return lastSematary;
      }

      sb.buriedHere = --buriedHere;
      this.freePages.$setBuffer(p);
      p = this.getWritingPage(lastSematary);
      sb.$copyToEnd(p);
      return this.freePages.page.get(buriedHere);
    }

    if (!buriedHere) {
      let last = this.superblock.lastPage;
      this.superblock.lastPage = ++last;
      this.saveSuperblock();
      return last;
    }

    this.superblock.buriedHere = --buriedHere;
    this.saveSuperblock();
    this.freePages.$setBuffer(this.readPage(0));
    return this.freePages.page.get(buriedHere);
  }


  protected writePage(page: number, data: Buffer) {
    let pos = this.sizePage * page;
    this.file.write(pos, data);
  }

  readPage(page: number) {
    if (this.writingPages.has(page)) {
      return this.writingPages.get(page)!;
    }

    if (this.readingPages.has(page)) {
      return this.readingPages.get(page)!;
    }

    let freeBuffer: Buffer | undefined;
    if (this.readingPages.size >= this.maxSizeReadingPages) {
      let entry = this.readingPages.entries().next().value!;
      freeBuffer = entry[1];
      this.readingPages.delete(entry[0])
    }

    if (!freeBuffer) {
      freeBuffer = Buffer.allocUnsafe(this.sizePage);
    }

    this.readingPages.set(page, freeBuffer);
    this.file.read(freeBuffer, page * this.sizePage, this.sizePage);
    return freeBuffer;
  }

  getWritingPage(page: number) {
    if (this.writingPages.has(page)) {
      return this.writingPages.get(page)!;
    }
    let freeBuffer: Buffer | undefined;
    if (this.writingPages.size >= this.maxSizeWritingPages) {
      let entry = this.writingPages.entries().next().value!;
      this.writePage(entry[0], entry[1]);
      this.writingPages.delete(entry[0]);
      freeBuffer = entry[1];
    }
    if (!freeBuffer) {
      freeBuffer = Buffer.allocUnsafe(this.sizePage);
    }
    let buf = this.readPage(page);
    buf.copy(freeBuffer, 0, 0, this.sizePage);
    this.writingPages.set(page, freeBuffer);
    return freeBuffer;
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

    await this.file.commit();

    CommitQueue.end(this.idCommitQueue);
  }
}