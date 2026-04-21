import BytePageView, { TPageView } from "./PageViewArray";
import PagesManager from "./PagesManager";
// import PagesManager, { SEMATARY_ENTRY_KEYS, SEMATARY_SUPERBLOCK_KEYS } from "./PagesManager"
import Superblock, { TSuperblock } from "./PageViewSuperblock";



// export type PageTypes = {
//   sematary: [SEMATARY_SUPERBLOCK_KEYS, SEMATARY_ENTRY_KEYS];
// }


// const STORAGE: any = {

// }
// export function registerPageType<T extends keyof PageTypes>(key: T, superblock: Map<PageTypes[T][0], number>, array: Map<PageTypes[T][1], number>) {
//   let pv = new PageView(key, superblock, array);
//   STORAGE[key] = pv;
//   return pv;
// }

// export function getPageView<T extends keyof PageTypes>(key: T): PageView<T> {
//   return STORAGE[key];
// }

export class PageView<T1 extends string, T2 extends string> {
  public readonly sb: TSuperblock<T1>;
  public readonly ar: TPageView<T2>;
  // public key: T;
  public readonly capacity: number;
  public pagesManager!: PagesManager;
  constructor(sbMap: [T1, number][], arMap: [T2, number][]) {
    // this.key = key;
    this.ar = BytePageView.create(new Map(arMap));
    this.buffer = this.ar.$getBuffer();
    this.sb = Superblock.create(new Map(sbMap), this.ar.$sizePage);
    this.sb.$setBuffer(this.buffer);

    this.capacity = this.ar.$capacityArray;
    setTimeout(() => {
      if (!this.pagesManager)
        this.pagesManager = PagesManager.current();
    });

  }

  // protected pageId: number = -1;
  protected buffer: Buffer;

  public currentPage = -1;
  // public isDirty = false;
  read(pageIndex: number) {
    this.currentPage = pageIndex;
    this.pagesManager.readPage(this.buffer, pageIndex);
    return this;
  }

  save() {
    this.pagesManager.writePage(this.currentPage, this.buffer);
    return this;
  }

}


