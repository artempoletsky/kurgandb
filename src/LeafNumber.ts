import { PageView } from "./PageView";
import { TPageView } from "./PageViewArray";

type SUPERBLOCK_KEYS = "pageLength" | "pageMin" | "pageMax" | "level" | "parentPage";

type CHUNK_KEYS = "value" | "userValue1" | "userValue2";
export const LEAF_NUMBER = new PageView<SUPERBLOCK_KEYS, CHUNK_KEYS>([
  ["pageLength", 4],
  ["pageMin", 8],
  ["pageMax", 8],
  ["parentPage", 4],
], [
  ["value", 8],
  ["userValue1", 8],
  ["userValue2", 8],
]);

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


export function findInChunkMultiple<T extends string>(pv: TPageView<T>,
  values: [],
  lenght: number,
  key: T | "id" = "id") {
  const result = new Array(values.length);
  findInChunkMultipleRecur(pv as any, 0, lenght - 1, values, result, 0, result.length - 1, key);
  return result;
}

export function remove(v: PageView<any, any>, pageIndex: number, value: number) {
  v.read(pageIndex);
  let { pageLength } = v.sb

  let index = v.ar.value.binarySearchValue(value, pageLength);
  if (!index.found) {
    throw new Error(`LeafNumber: Trying to delete not existent record from the leaf`);
  }
  v.ar.$shiftLeft(v.sb.pageLength, index.pos);
  v.save();
}

export function add(v: PageView<any, any>, pageIndex: number, value: number, payload: any) {
  v.read(pageIndex);
  if (v.capacity == v.sb.pageLength) {
    split(pageIndex, value, payload);
    return;
  }
  v.ar.userValue1.set(v.sb.pageLength, payload);
  v.ar.userValue2.set(v.sb.pageLength, payload);
  v.sb.pageLength++;
}

export function split(v: PageView<any, any>, pageIndex: number, value: number, payload: any) {
  let newPageId = v.pagesManager.getFreePageId();
}

