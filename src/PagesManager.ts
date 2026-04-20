
import fs from "fs";
import CommitQueue from "./CommitQueue";
import PatchFile from "./PatchFile";
import Superblock, { TSuperblock } from "./PageViewSuperblock";
import BytePageView, { TPageView } from "./PageViewArray";
import { DataBase } from "./db";
import { PageView } from "./PageView";


const PAGE_SIZE = 0x2000;

export type SEMATARY_SUPERBLOCK_KEYS = "lastPage" | "buriedHere" | "prevSematary";
export type SEMATARY_ENTRY_KEYS = "page";

export const SEMATARY = new PageView<SEMATARY_SUPERBLOCK_KEYS, SEMATARY_ENTRY_KEYS>([
  ["lastPage", 4],
  ["buriedHere", 4],
  ["prevSematary", 4],
], [
  ["page", 4],
]);

export default class PagesManager {

  static currentPagesManager: PagesManager;

  static current() {
    if (this.currentPagesManager) return this.currentPagesManager;
    this.currentPagesManager = new PagesManager({
      path: DataBase.workingDirectory + "/pages.bin"
    });
    return this.currentPagesManager;
  }

  public readonly path: string;
  public readonly sizePage: number;

  protected file: PatchFile;

  protected idCommitQueue: string;

  // protected writingPages: Map<number, Buffer> = new Map();
  // protected readingPages: Map<number, Buffer> = new Map();
  protected maxSizeWritingPages = 1;
  protected maxSizeReadingPages = 1;

  /**
   * Temporary page for various utility uses
   */
  protected thePage: Buffer;

  get __debug() {
    if (process.env.NODE_ENV !== "test") {
      throw "__debug method should only be used in tests";
    }
    return {
      // heap: this.heap,
      path: this.path,
      // writingPages: this.writingPages,
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
    this.thePage = Buffer.alloc(this.sizePage);
    this.reset();    
  }

  reset() {
    this.file.reset();
  }

  throwPageAlreadyDeleted(index: number) {
    throw new Error(`PagesManager;deletePage: page: "${index}" was deleted before`);
  }

  doesSemataryContainPage(index: number) {
    let length = SEMATARY.sb.buriedHere;
    for (let i = 0; i < length; i++) {
      if (SEMATARY.ar.page.get(i) == index) {
        return true;
      }
    }
    return false;
  }

  doesPageHaveTombstone(index: number) {
    this.readPage(this.thePage, index);
    let offset = this.thePage.readUInt32LE(0);
    return this.thePage.subarray(offset, offset + 8).toString() == "deadpage";
  }

  deletePage(index: number) {
    if (index <= 0) {
      throw new Error("PagesManager;deletePage: wrong page index");
    }

    let sem = SEMATARY.read(0); //read anyway we need this 100%
    if (this.doesSemataryContainPage(index)) this.throwPageAlreadyDeleted(index);

    const lastSematary = sem.sb.prevSematary;
    if (lastSematary != 0) {
      sem.read(lastSematary); //read anyway we need this 100%
      if (this.doesSemataryContainPage(index)) this.throwPageAlreadyDeleted(index);

      if (sem.sb.prevSematary != 0) //check the tombstone only when we have more than 2 semataries
        if (this.doesPageHaveTombstone(index)) this.throwPageAlreadyDeleted(index);
    }




    let buriedHere = sem.sb.buriedHere;
    if (buriedHere >= sem.capacity) {
      sem.read(index);
      sem.sb.buriedHere = 0;
      sem.sb.prevSematary = lastSematary;
      sem.save();

      sem.read(0).sb.prevSematary = index;
      sem.save();
      return;
    }

    sem.ar.page.set(buriedHere, index);
    sem.sb.buriedHere = ++buriedHere;
    sem.save();
  }

  getFreePageId(): number {
    let sem = SEMATARY.read(0);
    let { prevSematary: lastSematary, buriedHere } = sem.sb;

    if (lastSematary != 0) {
      sem.read(lastSematary);

      let { buriedHere, prevSematary } = sem.sb;
      if (buriedHere == 0) {
        sem.read(0).sb.prevSematary = prevSematary;
        sem.save();
        return lastSematary;
      }

      sem.sb.buriedHere = --buriedHere;
      sem.save();
      return sem.ar.page.get(buriedHere);
    }

    if (!buriedHere) {
      let newPage = ++sem.sb.lastPage
      sem.save();
      this.createPage(newPage);
      return newPage;
    }

    sem.sb.buriedHere = --buriedHere;
    sem.save();
    return sem.ar.page.get(buriedHere);
  }

  protected createPage(index: number) {
    this.thePage.fill(0);
    this.writePage(index, this.thePage);
  }


  writePage(page: number, data: Buffer) {
    let pos = this.sizePage * page;
    this.file.write(pos, data);
  }

  readPage(target: Buffer, page: number): void {
    this.file.read(target, page * this.sizePage, this.sizePage);
  }

  protected async _commitBefore(): Promise<void> {

  }

  async commit() {
    CommitQueue.start(this.idCommitQueue);

    // for (const [page, buf] of this.writingPages) {
    //   this.writePage(page, buf);
    // }
    // this.writingPages.clear();

    await this._commitBefore();

    CommitQueue.end(this.idCommitQueue);

    await this.file.commit();


  }
}