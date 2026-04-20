import BytePageView from "./PageViewArray";
import NamedByteBuffer, { TPageView } from "./PageViewArray";
import PagesManager from "./PagesManager";
import Superblock, { TSuperblock } from "./PageViewSuperblock";

type SUPERBLOCK_KEYS = "pageLength" | "pageMin" | "pageMax" | "level" | "parentPage";
const SUPERBLOCK_STRUCTURE = new Map<SUPERBLOCK_KEYS, number>([
  ["pageLength", 4],
  ["pageMin", 8],
  ["pageMax", 8],
  ["parentPage", 4],
  ["level", 1],
]);

type CHUNK_KEYS = "page" | "limbMin" | "limbMax" | "limbLength";
const CHUNK_STRUCTURE = new Map<CHUNK_KEYS, number>([
  ["limbLength", 4],
  ["limbMin", 8],
  ["limbMax", 8],
  ["page", 4],
]);

let sb = Superblock.create(SUPERBLOCK_STRUCTURE);
let pv = BytePageView.create(CHUNK_STRUCTURE, sb.$size);

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

export function findInChunk<T extends string>(pv: TPageView<T>,
  lo: number,
  hi: number,
  value: number,
  key: T | "id" = "id"
): { index: number; found: boolean; } {

  let mid = 0;
  while (lo <= hi) {
    mid = Math.floor((lo + hi) / 2);
    let v = pv[key as T].get(mid);
    if (v == value) {
      return {
        found: true,
        index: mid,
      };
    }

    if (value > v) {
      hi = mid - 1;
    } else {
      lo = mid + 1;
    }
  }
  return {
    found: false,
    index: mid,
  };
}

export function findInChunkMultipleRecur<T extends string>(pv: TPageView<T>,
  lo: number,
  hi: number,
  values: number[],
  result: number[],
  resultLo: number,
  resultHi: number,
  key: T,
) {

  let inputMid = Math.floor(resultLo + resultHi / 2); // middle index of the input array
  let foundMid = findInChunk(pv, lo, hi, values[inputMid], key);
  result[inputMid] = foundMid.found ? foundMid.index : -1;

  if (inputMid + 1 < resultHi) {
    findInChunkMultipleRecur(pv, foundMid.index + 1, hi, values, result, inputMid + 1, resultHi, key);
  }

  if (resultLo < inputMid - 1) {
    findInChunkMultipleRecur(pv, lo, foundMid.index - 1, values, result, resultLo, inputMid - 1, key);
  }
}

export function fundInChunkMultiple<T extends string>(pv: TPageView<T>,
  values: [],
  lenght: number,
  key: T | "id" = "id") {
  const result = new Array(values.length);
  findInChunkMultipleRecur(pv as any, 0, lenght - 1, values, result, 0, result.length - 1, key);
  return result;
}

export default class Tree<T extends string> {
  headerPageIndex: number;
  pv: TPageView<T>;

  constructor(headerPageIndex: number, pageStructure: Map<T, number>) {
    this.headerPageIndex = headerPageIndex;
    this.pv = BytePageView.create(pageStructure);
  }

  set(id: number, record: TSuperblock<T>) {
    findInChunkMultiple(this.pv, [id], );
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