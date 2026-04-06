import { ByteRecord } from "./ByteRecord";

type HEADER_STRUCTURE_KEY = "numberOfRecords"
  | "currentWritingPosition"
  | "reserved"
  | "dataStart";

const HEADER_STRUCTURE: { key: HEADER_STRUCTURE_KEY, length: number }[] = [];
HEADER_STRUCTURE.push({ key: "numberOfRecords", length: 1 });
HEADER_STRUCTURE.push({ key: "currentWritingPosition", length: 2 });
HEADER_STRUCTURE.push({ key: "reserved", length: 50 });
HEADER_STRUCTURE.push({ key: "dataStart", length: 0 });

let _currentOffset = 0;
const pageHeaderOffsets = HEADER_STRUCTURE.reduce((result, e) => {
  _currentOffset += e.length;
  result[e.key] = _currentOffset;
  return result;
}, {} as Record<HEADER_STRUCTURE_KEY, number>);


export default class BytePage {
  public readonly data!: Buffer;
  public readonly pageSize!: number;
  public readonly slotsArray!: Uint16Array;
  public readonly br: ByteRecord<any, any, any, any>;
  constructor() {
    this.br = new ByteRecord();
  }

  readSlotsArray() {
    let len = this.numberOfRecords;
    return new Uint16Array(this.data.buffer, this.pageSize - len * 2, len);
  }

  readRow() {

  }

  deleteRow() {

  }

  createRow() {
    this.numberOfRecords++;
    let pos = this.pageSize - this.numberOfRecords * 2;
    this.data.writeUint16LE(this.currentWritingPosition, pos);

    let rowMaxSize = 260;
    this.currentWritingPosition += rowMaxSize;
  }

  createPage() {
    //@ts-expect-error
    this.data = Buffer.alloc(this.pageSize);
    this.formatPage();
  }

  formatPage() {
    this.numberOfRecords = 0;
    this.currentWritingPosition = pageHeaderOffsets.dataStart;
    //@ts-expect-error
    this.slotsArray = new Uint16Array(0);
  }

  public get numberOfRecords(): number {
    return this.data[pageHeaderOffsets.numberOfRecords];
  }

  protected set numberOfRecords(v: number) {
    this.data[pageHeaderOffsets.numberOfRecords] = v;
  }

  public get currentWritingPosition(): number {
    return this.data.readUInt16LE(pageHeaderOffsets.currentWritingPosition);
  }

  protected set currentWritingPosition(v: number) {
    this.data.writeUInt16LE(v, pageHeaderOffsets.currentWritingPosition);
  }


}