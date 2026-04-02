import FilePatchRecord from "./FilePatchRecord";


export default class StringsSaver {
  protected _stringsMetaStart: number;
  protected _stringsTailStart: number;
  protected _stringsNum: number;
  protected _stringsByteLengths!: Uint8Array;
  protected _stringsOffsets!: Uint16Array;
  protected _stringsCache!: string[];

  constructor({

  }: {
    bufferPage: Buffer;
    stringsMetaStart: number;
    stringsTailStart: number;
    stringsNum: number;
    fpr: FilePatchRecord;
  }) {
  }


  getString(index: number) {
    if (this._stringsCache[index] !== undefined) return this._stringsCache[index];

    let len = this._stringsByteLengths[index];
    let start = this._stringsOffsets[index];
    let result: string;
    if (len === 0xFF) {
      const heapAddr = this._bufferPage.readUInt32LE(start);
      len = this._bufferPage.readUInt32LE(start + 4);
      result = this.fpr.readHeap(heapAddr, len).toString("utf-8");
    } else {
      result = this._bufferPage.subarray(start, start + len).toString("utf-8");
    }

    this._stringsCache[index] = result;

    return result;
  }


  setString(index: number, string: string) {
    let prev = this._stringsCache[index];
    if (prev == string) return;
    let byteLength = Buffer.byteLength(string, "utf-8");
    if (byteLength > 0xFF) {
      throw "implement separate file field or the heap";
    }
    this._needsStringsTailRebuilding = true;

    this._stringsCache[index] = string;
    this._stringsByteLengths[index] = byteLength;
  }

  buildStringsTail() {
    let tail = "";

    const stringsOffsets = [];
    let writePosition = 0;
    for (let i = 0; i < this._stringsNum; i++) {
      let str = this._stringsCache[i];
      if (str === undefined) {

      }
      const strByteLen = Buffer.byteLength(str, "utf-8");
      if (strByteLen > 0xFF) {
        throw "not implemented";
      }
      this._bufferPage[this._stringsMetaStart + i * 3] = strByteLen;
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
      this._bufferPage[this._stringsMetaStart + i * 3] = strByteLen;
      stringsOffsets.push(writePosition);
      tail += str;
      writePosition += strByteLen;
    }

    let tailStart = this._pageSize - writePosition;
    for (let i = 0; i < this._stringNum; i++) {
      this._bufferPage.writeInt16LE(tailStart + stringsOffsets[i], this._stringsMetaStart + i * 2 + 1);
    }


    return tail;
  }

}