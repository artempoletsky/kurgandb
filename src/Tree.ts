import BytePageView from "./PageViewArray";
import NamedByteBuffer, { TPageView } from "./PageViewArray";
import PagesManager from "./PagesManager";
import Superblock, { TSuperblock } from "./PageViewSuperblock";
import { PageView } from "./PageView";

type SUPERBLOCK_KEYS = "pageLength" | "pageMin" | "pageMax" | "level" | "parentPage";

type CHUNK_KEYS = "page" | "limbMin" | "limbMax" | "limbLength";
export const NUMBER_BTREE_CHUNK = new PageView<SUPERBLOCK_KEYS, CHUNK_KEYS>([
  ["pageLength", 4],
  ["pageMin", 8],
  ["pageMax", 8],
  ["parentPage", 4],
  ["level", 1],
], [
  ["limbLength", 4],
  ["limbMin", 8],
  ["limbMax", 8],
  ["page", 4],
]);

// let sb = Superblock.create(SUPERBLOCK_STRUCTURE);
// let pv = BytePageView.create(CHUNK_STRUCTURE, sb.$size);




export function addLeaf(limbIndex: number, newPageIndex: number, minValue: number, maxValue: number) {

}

export function deleteLeaf(limbIndex: number, chunkIndex: number) {

}



export function recurFindChunk(page: Buffer, value: number): {
  indexInPage: number
  result: TSuperblock<CHUNK_KEYS> | null
} {
  // sb.$setBuffer(page.subarray(page.byteLength - sb.$size, page.byteLength));
  sb.$readFromPage(page);
  pv.$setBuffer(page);

  let lo = 0;
  let hi = sb.pageLength - 1;
  let mid = 0;
  while (lo <= hi) {
    mid = Math.floor((lo + hi) / 2);
    let left = pv.limbMin.get(mid);
    if (left <= value && value <= pv.limbMax.get(mid)) {
      if (sb.level == 0) {
        let result = Superblock.create(CHUNK_STRUCTURE);
        pv.$copyToSuperblock(result, mid);
        return {
          indexInPage: mid,
          result,
        };
      } else {
        let nextPage: Buffer = PagesManager.current().readPage(pv.page.get(mid));
        return recurFindChunk(nextPage, value);
      }
    }

    if (value < left) {
      hi = mid - 1;
    } else {
      lo = mid + 1;
    }
  }
  return {
    indexInPage: mid,
    result: null,
  };
}


export default class Tree<T extends string> {
  headerPageIndex: number;
  pv: TPageView<T>;

  constructor(headerPageIndex: number, pageStructure: Map<T, number>) {
    this.headerPageIndex = headerPageIndex;
    this.pv = BytePageView.create(pageStructure);
  }

  addLeaf(min: number, max: number, page: number) {
    let chunk = NUMBER_BTREE_CHUNK;

    findInChunkMultiple(chunk.ar, [min, max], chunk.sb.pageLength,);
    let p = PagesManager.current().readPage(this.headerPageIndex);
    let c = recurFindChunk(p, id);
    if (c.result?.page) {

    }
  }

  openRoot(page: number) {

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
}