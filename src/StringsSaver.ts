import { th } from "@faker-js/faker";
import FilePatchRecord from "./FilePatchRecord";

const MIN_HEAP_SIZE = 1024 * 4;
export default class StringsSaver {
  protected _stringsMetaStart: number;
  protected _stringsTailStart: number;
  protected numberOfStrings: number;
  protected lengthsPage!: Uint8Array;
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
    this.cache = new Array(this.numberOfStrings);
    this.fpr = options.fpr;
  }


  readPage() {
    this.lengthsPage = new Uint8Array(this.page.buffer, this._stringsMetaStart, this.numberOfStrings);
    this.offsetsPage = new Uint16Array(this.numberOfStrings);
    let currentOffset = this._stringsTailStart;
    this.heapPositions = new Uint32Array(this.numberOfStrings);
    this.heapLenghts = new Uint32Array(this.numberOfStrings);

    for (let i = 0; i < this.numberOfStrings; i++) {
      let len = this.lengthsPage[i];
      if (len = 0xff) {
        len = 12;
      }
      this.offsetsPage[i] = currentOffset;
      currentOffset += this.lengthsPage[i];
    }
    this.cache = new Array(this.numberOfStrings);
  }

  getString(index: number) {
    if (this.cache[index] !== undefined) return this.cache[index];

    let len = this.lengthsPage[index];
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
    let writePos = this._stringsTailStart;
    let readPos = this._stringsTailStart;
    let tempBuff = Buffer.allocUnsafe(this.page.byteLength - this._stringsTailStart);
    for (let i = 0; i < this.numberOfStrings; i++) {
      let cached = this.cache[i];
      let oldLen = this.lengthsPage[i];
      let newLen = oldLen;

      if (cached === undefined) {
        if (oldLen === 0xFF) { // heap string has fixed length of 12 bytes [offset:4][length:4][maxLength:4]
          newLen = 12;
        }
        this.page.copy(tempBuff, writePos, readPos, readPos + newLen);
      } else {
        newLen = Buffer.byteLength(cached, "utf-8");
        if (newLen >= 0xFF) {
          this.lengthsPage[i] = 0xFF;
          const oldHeapOffset = this.page.readUInt32LE(readPos);
          // const oldHeapLength = this.page.readUInt32LE(readPos + 4);
          const oldHeapMaxLength = this.page.readUInt32LE(readPos + 8);

          if (oldLen === 0xFF && oldHeapMaxLength >= newLen) { // if it was already a heap string, we can reuse the same heap space if it fits
            // if (oldHeapMaxLength >= newLen) { // it fits
            this.fpr.writeHeap(Buffer.from(cached, "utf-8"), oldHeapMaxLength, oldHeapOffset);
            tempBuff.writeUInt32LE(oldHeapOffset, writePos);
            tempBuff.writeUInt32LE(newLen, writePos + 4);
            tempBuff.writeUInt32LE(oldHeapMaxLength, writePos + 8);
          } else { //neded to allocate new heap space
            const heapSection = this.fpr.writeHeap(Buffer.from(cached, "utf-8"), Math.max(newLen * 1.5, MIN_HEAP_SIZE));
            tempBuff.writeUInt32LE(heapSection.offsetHeap, writePos);
            tempBuff.writeUInt32LE(heapSection.sizeCurrent, writePos + 4);
            tempBuff.writeUInt32LE(heapSection.sizeMax, writePos + 8);
          }
          if (oldLen === 0xFF) { // if it was a heap the next string is after 12 bytes
            oldLen = 12;
          }
          newLen = 12;
        } else {
          this.lengthsPage[i] = newLen;
          tempBuff.write(cached, writePos, "utf-8");
        }
      }

      writePos += newLen;
      readPos += oldLen;

    }
  }

}