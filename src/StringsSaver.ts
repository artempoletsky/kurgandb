import FilePatchRecord from "./FilePatchRecord";

const MIN_HEAP_SIZE = 1024 * 4;
export default class StringsSaver {
  protected _stringsMetaStart: number;
  protected _stringsTailStart: number;
  protected numberOfStrings: number;
  protected byteLengthsPage!: Uint8Array;
  protected offsetsPage!: Uint16Array;
  protected cache!: string[];
  protected page: Buffer;
  protected heapPositions!: Uint32Array;
  protected heapLenghts!: Uint32Array;
  protected heapMaxLengths!: Uint32Array;
  // protected heapFlags!: Map<number, boolean>;

  protected fpr!: FilePatchRecord;

  constructor(options: {
    bufferPage: Buffer;
    stringsMetaStart: number;
    stringsTailStart: number;
    stringsNum: number;
    fpr: FilePatchRecord;
  }) {
    this.page = options.bufferPage;
    this.numberOfStrings = options.stringsNum;
    this._stringsMetaStart = options.stringsMetaStart;
    this._stringsTailStart = options.stringsTailStart;
  }


  readPage() {
    this.byteLengthsPage = new Uint8Array(this.page.buffer, this._stringsMetaStart, this.numberOfStrings);
    this.offsetsPage = new Uint16Array(this.numberOfStrings);
    let currentOffset = this._stringsTailStart;
    this.heapPositions = new Uint32Array(this.numberOfStrings);
    this.heapLenghts = new Uint32Array(this.numberOfStrings);

    for (let i = 0; i < this.numberOfStrings; i++) {
      let len = this.byteLengthsPage[i];
      if (len = 0xff) {
        len = 12;
      }
      this.offsetsPage[i] = currentOffset;
      currentOffset += this.byteLengthsPage[i];
    }
  }

  getString(index: number) {
    if (this.cache[index] !== undefined) return this.cache[index];

    let len = this.byteLengthsPage[index];
    let start = this.offsetsPage[index];
    let result: string;
    if (len === 0xFF) {
      const heapAddr = this.page.readUInt32LE(start);
      len = this.page.readUInt32LE(start + 4);
      result = this.fpr.readHeap(heapAddr, len).toString("utf-8");
    } else {
      result = this.page.subarray(start, start + len).toString("utf-8");
    }

    this.cache[index] = result;

    return result;
  }


  setString(index: number, string: string) {
    let prev = this.cache[index];
    if (prev == string) return;
    this.cache[index] = string;
  }

  save() {
    for (let i = 0; i < this.numberOfStrings; i++) {
      let cached = this.cache[i];
      let len = 

    }
  }

  buildStringsTail() {
    let tail = "";

    const stringsOffsets = [];
    let writePosition = 0;
    for (let i = 0; i < this.numberOfStrings; i++) {
      let str = this.cache[i];
      if (str === undefined) {

      }
      const strByteLen = Buffer.byteLength(str, "utf-8");
      if (strByteLen > 0xFF) {
        throw "not implemented";
      }
      this.page[this._stringsMetaStart + i * 3] = strByteLen;
      stringsOffsets.push(writePosition);
      tail += str;
      writePosition += strByteLen;
    }

    const JSONOffsets = [];

    for (let i = 0; i < this._jsonLen; i++) {
      const str = this.$getJSON(i);
      const strByteLen = Buffer.byteLength(str, "utf-8");
      if (strByteLen > 0xFFFF) {
        throw "not implemented";
      }
      this.page[this._stringsMetaStart + i * 3] = strByteLen;
      stringsOffsets.push(writePosition);
      tail += str;
      writePosition += strByteLen;
    }

    let tailStart = this._pageSize - writePosition;
    for (let i = 0; i < this._stringNum; i++) {
      this.page.writeInt16LE(tailStart + stringsOffsets[i], this._stringsMetaStart + i * 2 + 1);
    }


    return tail;
  }

}