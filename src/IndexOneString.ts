
import LogicalMemoryHeap from "./LogicalMemoryHeap";
import NamedByteBuffer, { TPageView, TSuperblock } from "./PageViewArray";
import PagesManager from "./PagesManager";


type SUPERBLOCK_KEY =
  "numberOfChunks"
  | "numberOfRecords"
  | "keyMin"
  | "keyMax"
  | "idMin"
  | "idMax";

const SUPERBLOCK_STRUCTURE = new Map<SUPERBLOCK_KEY, number>([
  ["numberOfChunks", 2],
  ["numberOfRecords", 4],
  ["keyMin", 16],
  ["keyMax", 16],
  ["idMin", 8],
  ["idMax", 8],
]);

type HEADER_KEY =
  "page"
  | "numberOfRecords"
  | "keyMin"
  | "keyMax"
  | "idMin"
  | "idMax";

const HEADER_STRUCTURE = new Map<HEADER_KEY, number>([
  ["page", 2],
  ["numberOfRecords", 4],
  ["keyMin", 16],
  ["keyMax", 16],
  ["idMin", 8],
  ["idMax", 8],
]);

type PAGE_ENTRY_KEY =
  "idHeap"
  | "sortKey"
  | "length"
  | "id";

const PAGE_STRUCTURE = new Map<PAGE_ENTRY_KEY, number>([
  ["idHeap", 8],
  ["sortKey", 16],
  ["length", 1],
  ["id", 8],
]);


export class IndexOneString {
  protected superblock!: TSuperblock<SUPERBLOCK_KEY>;
  protected header!: TPageView<HEADER_KEY>;
  protected heap: LogicalMemoryHeap;
  constructor(path: string) {
    // super({ path });
    this.heap = new LogicalMemoryHeap(path + ".heap");

  }

  reset(): void {
    let headerData!: Buffer;
    this.superblock = NamedByteBuffer.createSuperblock(SUPERBLOCK_STRUCTURE);


    const superBlockSize = this.superblock.$size;
    headerData.copy(this.superblock.$getBuffer(), 0, 0, superBlockSize);

    this.header = NamedByteBuffer.createArray()

    this.heap.reset();
    super.reset();
  }

  get(value: string): number | undefined {

  }

  set(value: string, id: number) {
    this.heap.readString()
  }
}