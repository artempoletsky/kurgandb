
import fs from "fs";
import CommitQueue from "./CommitQueue";
import PatchFile from "./PatchFile";


const DEFAULT_MEMORY_BUFFER_SIZE = 1024 * 1024 * 10;
const PAGE_SIZE = 0x2000;




export default class PagesManager {

  public readonly path: string;
  public readonly sizePage: number;

  protected file: PatchFile;

  protected idCommitQueue: string;

  protected writingPages: Map<number, Buffer> = new Map();
  protected maxSizeWritingPages = 1;

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
  }

  protected writePage(page: number, data: Buffer) {
    let pos = this.sizePage * page;
    this.file.write(pos, data);
  }

  readPage(page: number) {
    if (this.writingPages.has(page)) {
      return this.writingPages.get(page)!;
    }

    let buf = Buffer.allocUnsafe(this.sizePage);
    this.file.read(buf, page * this.sizePage, this.sizePage);
    return buf;
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