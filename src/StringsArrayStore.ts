import fs from "fs";


const ARRAY_MAX_LEN = 100;

const ARRAY_LENGTH_LENGTH = 1;

const DEFAULT_PAGE_SIZE = 4 * 1024;

type RECORD_STRUCTURE_KEY = "STRING_LENGTH"
  | "STRING_MAX_LENGTH"
  | "STRING_POSITION";

const RECORD_STRUCTURE: { key: RECORD_STRUCTURE_KEY, length: number }[] = [];
RECORD_STRUCTURE.push({ key: "STRING_LENGTH", length: 1 });
RECORD_STRUCTURE.push({ key: "STRING_MAX_LENGTH", length: 1 });
RECORD_STRUCTURE.push({ key: "STRING_POSITION", length: 4 });

let sum = 0;
const OFFSETS = RECORD_STRUCTURE.reduce((res, el) => {
  res[el.key] = sum + el.length;
  return res;
}, {} as Record<RECORD_STRUCTURE_KEY, number>);

const SMALL_RECORD_SIZE = RECORD_STRUCTURE.reduce((sum, el) => sum + el.length, 0);

const MIN_STRING_MAXLENGTH = 15;


export class StringsArrayStore {

  public fdFixed: number;
  public fdVariable: number;
  public pathFixed: string;
  public pathVariable: string;
  public cache: Map<number, Buffer>;
  public readonly pageSize: number;

  constructor(fixedPath: string, variablePath: string, pageSize?: number) {
    this.pathFixed = fixedPath;
    this.pathVariable = variablePath;
    this.pageSize = pageSize || DEFAULT_PAGE_SIZE;

    if (!fs.existsSync(this.pathFixed)) {
      fs.writeFileSync(this.pathFixed, Buffer.alloc(0));
    }
    if (!fs.existsSync(this.pathVariable)) {
      fs.writeFileSync(this.pathVariable, Buffer.alloc(0));
    }

    this.fdFixed = fs.openSync(this.pathFixed, "r");
    this.fdVariable = fs.openSync(this.pathVariable, "r");
    this.cache = new Map();
  }

  readPage(page: number) {
    let buf: Buffer;
    if (this.cache.has(page)) {
      buf = this.cache.get(page)!;
    } else {
      buf = Buffer.allocUnsafe(this.pageSize);
      fs.readSync(this.fdFixed, buf, 0, this.pageSize, page * this.pageSize);
      this.cache.set(page, buf);
    }
    return buf;
  }

  readString(page: number, index: number) {

    const buf = this.readPage(pos);
    pos += SMALL_RECORDS_OFFSET;
    const len = buf.readUInt16LE(pos + SMALL_RECORD_SIZE * index + OFFSETS.STRING_LENGTH);
    const strPos = buf.readUint32LE(pos + SMALL_RECORD_SIZE * index + OFFSETS.STRING_POSITION);

    let res = Buffer.allocUnsafe(len);
    fs.readSync(this.fdVariable, res, 0, len, strPos);

    return res.toString("utf8");
  }

  writeString(pos: number, index: number, string: string) {
    const buf = this.readRecord(pos);
    pos += SMALL_RECORDS_OFFSET;
    const len = buf.readUInt16LE(pos + SMALL_RECORD_SIZE * index + OFFSETS.STRING_LENGTH);
    const strPos = buf.readUint32LE(pos + SMALL_RECORD_SIZE * index + OFFSETS.STRING_POSITION);
  }

  readArray(pos: number): string[] {

  }

  writeArray(pos: number, arr: string[], maxLength: number) {

  }
}