import NamedByteBuffer, { TPage } from "./NamedByteBuffer";
import Superblock from "./Superblock";

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
let pageNamed = NamedByteBuffer.createPage(CHUNK_STRUCTURE, 0x2000);
export function recurFindChunk(page: Buffer, value: number): ChunkMeta | null {
  sb.$setBuffer(page.subarray(page.byteLength - sb.$size, page.byteLength));
  pageNamed.$setBuffer(page);

  let lo = 0;
  let hi = sb.pageLength - 1;
  let mid = 0;
  while (lo <= hi) {
    mid = Math.floor((lo + hi) / 2);
    if (pageNamed.limbMin.get(mid) <= value && value <= pageNamed.limbMax.get(mid)) {
      if (sb.level == 0) {

      } else {
        // pageNamed.page.get()
        let newPage: Buffer;
        return recurFindChunk(newPage, value);
      }
    }
  }
  return null;
}

export default class Tree {

  openRoot(page: number) {

  }


  findChunkIndex(value: number, node: TPage<"minValue" | "maxValue">): { chunkIndex: number; chunkMeta: null | ChunkMeta } {
    const numberOfChunks = this.superblock.numberOfChunks;
    if (value < this.superblock.minValue) return {
      chunkIndex: -1,
      chunkMeta: null,
    };
    if (value > this.superblock.maxValue) return {
      chunkIndex: numberOfChunks + 1,
      chunkMeta: null,
    };

    for (let i = 0; i < numberOfChunks; i++) {
      let min = node.minValue.get(i);
      let max = node.maxValue.get(i);
      if (min <= value && value <= max) {
        return {
          chunkIndex: i,
          chunkMeta: {
            min,
            max,
            length: this.header.length.get(i),
            page: this.header.page.get(i),
          },
        };
      }
    }
    throw "ChunkedIndex.findChunkIndex: should be unreachable";
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