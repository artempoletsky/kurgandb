import BytePageView from "./PageViewArray";
import NamedByteBuffer, { TPageView } from "./PageViewArray";
import * as Leaf from "./LeafNumber";
import PagesManager from "./PagesManager";
import Superblock, { TSuperblock } from "./PageViewSuperblock";
import { PageView } from "./PageView";

type SUPERBLOCK_KEYS = "pageLength" | "pageMin" | "pageMax" | "level" | "parentPage";

type CHUNK_KEYS = "page" | "limbMin" | "limbMax" | "limbLength";

const ENTRY_STRUCTURE: [CHUNK_KEYS, number][] = [
  ["limbLength", 4],
  ["limbMin", 8],
  ["limbMax", 8],
  ["page", 4],
];

export const NUMBER_BTREE_CHUNK = new PageView<SUPERBLOCK_KEYS, CHUNK_KEYS>([
  ["pageLength", 4],
  ["pageMin", 8],
  ["pageMax", 8],
  ["parentPage", 4],
  ["level", 1],
], ENTRY_STRUCTURE);

// let sb = Superblock.create(SUPERBLOCK_STRUCTURE);
// let pv = BytePageView.create(CHUNK_STRUCTURE, sb.$size);


// const serviceEntry = Superblock.create(new Map(ENTRY_STRUCTURE));

export function addLeaf(limbIndex: number, newPageIndex: number, value: number) {
  const { ar, sb } = NUMBER_BTREE_CHUNK.read(limbIndex);
  let { pageLength } = sb;
  let searchResult = findChunk(value);
  let leafIndex = searchResult.indexInPage;


  ar.$shiftRight(pageLength, leafIndex);
  ar.limbLength.set(leafIndex, 1);
  ar.page.set(leafIndex, newPageIndex);
  ar.limbMax.set(leafIndex, value);
  ar.limbMin.set(leafIndex, value);

  // serviceEntry.limbLength = 1;
  // serviceEntry.page = newPageIndex;
  // serviceEntry.limbMax = value;
  // serviceEntry.limbMin = value;
  // ar.$writeEntry(pageLength, serviceEntry);


  sb.pageLength = ++pageLength;
  NUMBER_BTREE_CHUNK.save();
}

export function deleteLeaf(limbIndex: number, leafIndex: number) {
  const { ar, sb } = NUMBER_BTREE_CHUNK.read(limbIndex);
  ar.$shiftLeft(sb.pageLength, leafIndex);
  NUMBER_BTREE_CHUNK.save();
}

export function splitLimb(pageIndex: number, arrayIndex: number) {
  const { ar, sb } = NUMBER_BTREE_CHUNK.read(pageIndex);

  let { } = sb.pageLength;


  const initialLimbLenght = sb.pageLength;
  if (initialLimbLenght < 2)
    throw new Error(`Tree;splitLimb: length is too short for splitting`);

  let leftLenght = Math.floor(initialLimbLenght / 2);
  let rightLength = initialLimbLenght - leftLenght;
  let rightMax = sb.pageMax;
  let rightMin = ar.limbMin.get(leftLenght);
  let rightLevel = sb.level;
  let rightParentPage = sb.parentPage;

  sb.pageLength = leftLenght;
  sb.pageMax = ar.limbMax.get(leftLenght - 1);

  let entryLength = ar.$capacityArray;
  const rightData = Buffer.allocUnsafe((initialLimbLenght - leftLenght) * entryLength);
  let pageBuffer = ar.$getBuffer();
  pageBuffer.copy(rightData, 0, leftLenght * entryLength);
  NUMBER_BTREE_CHUNK.save().create();

  sb.pageLength = rightLength;
  sb.pageMax = rightMax;
  sb.pageMin = rightMin;
  sb.level = rightLevel;
  sb.parentPage = rightParentPage;
  rightData.copy(pageBuffer, 0, 0, 0);

  NUMBER_BTREE_CHUNK.save();
  if (rightParentPage) {

  }
}


export function findChunk(value: number): {
  indexInPage: number;
  found: boolean;
} {
  const { ar, sb } = NUMBER_BTREE_CHUNK;

  let lo = 0;
  let hi = sb.pageLength - 1;
  let mid = 0;
  while (lo <= hi) {
    mid = Math.floor((lo + hi) / 2);
    let left = ar.limbMin.get(mid);
    if (left <= value && value <= ar.limbMax.get(mid)) {
      return {
        indexInPage: mid,
        found: true,
      };
    }

    if (value < left) {
      hi = mid - 1;
    } else {
      lo = mid + 1;
    }
  }
  return {
    indexInPage: mid,
    found: false,
  };
}
export function recurFindChunk(value: number): {
  indexInPage: number;
  found: boolean;
} {
  const { ar, sb } = NUMBER_BTREE_CHUNK;

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


export default class Tree<T1 extends string, T2 extends string> {
  root: number;
  pv: PageView<T1, T2> & PageView<"length", "id">;

  constructor(rootPageIndex: number, pageStructure: PageView<T1, T2> & PageView<"length", "id">) {
    this.root = rootPageIndex;
    this.pv = pageStructure;
  }

  addLeaf(min: number, max: number, page: number) {
    let chunk = NUMBER_BTREE_CHUNK;

    findInChunkMultiple(chunk.ar, [min, max], chunk.sb.pageLength,);
    let p = PagesManager.current().readPage(this.root);
    let c = recurFindChunk(p, id);
    if (c.result?.page) {

    }
  }

  openRoot(page: number) {

  }

  insertRecord(rec: Record<T2, number> & Record<"id", number>) {
    
  }

  findValue(id: number): TSuperblock<T2> | null {

    const { ar, sb } = NUMBER_BTREE_CHUNK.read(this.root);
    let { found, indexInPage } = findChunk(id);
    if (!found) return null;
    let leaf = ar.page.get(indexInPage);
    this.pv.read(leaf);
    let searchRes = Leaf.findInChunk(this.pv.ar, 0, this.pv.sb.length - 1, id, "id");
    if (!searchRes.found) throw Error("Tree: Inconsistent leaf-tree reference");

    return this.pv.ar.$getEntry(searchRes.index);
  }
}
