import * as fs from "fs";


const MIN_BUFFER_SIZE = 1024 * 1024; // 1MB
const TOMBSTONE = 0xFFFFFFFF;

export class Index {

  constructor(type: "number" | "string", path: string)
  constructor(type: "number" | "string", tableName: string, columnName: string)
  constructor(type: "number" | "string", a1: string, a2?: string) {
    this.keyType = type;
    const path = a1;
    if (a2) {
      throw new Error("Not implemented yet");
    }

    this.path = path;
    this.reset();
  }

  protected path: string;
  public keyType: "number" | "string";
  protected buffer!: Buffer;
  protected stringMetaIndex: Buffer | null = null; // file structure is [offset (4 bytes)]

  protected bufferLength = 0;
  protected metaIndexLength = 0;

  get(id: number | string): number {
    // file structure for number keyType is [ID (4 bytes)][offset (4 bytes)]
    if (this.keyType === "number") {
      const entrySize = 8; // 4 bytes id + 4 bytes offset
      const length = Math.floor(this.bufferLength / entrySize);
      let low = 0;
      let high = length - 1;

      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        const pos = mid * entrySize;
        const currentId = this.buffer.readUInt32BE(pos);

        if (currentId === (id as number)) {
          const result = this.buffer.readUInt32BE(pos + 4);
          if (result === TOMBSTONE) {
            return -1;
          }
          return result;
        }

        if (currentId < (id as number)) {
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }
    } else {
      const length = Math.floor(this.stringMetaIndex!.length / 4);
      let low = 0;
      let high = length - 1;

      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        const indexPos = mid * 4;
        const idPos = this.stringMetaIndex!.readUInt32BE(indexPos);
        // file structure for string keyType is [ID length (2 bytes)][ID][offset (4 bytes)]
        const idLen = this.buffer.readUInt16BE(idPos);

        const cmp = Index.compareStringBuffer(id as string, this.buffer, idPos + 2, idLen);

        if (cmp === 0) {
          return this.buffer.readUInt32BE(idPos + 2 + idLen);
        }

        if (cmp < 0) {
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }
    }
    return -1;
  }


  widenBuffers() {
    this.buffer = Buffer.concat([this.buffer, Buffer.allocUnsafe(this.buffer.length)]);
    if (this.stringMetaIndex) {
      this.stringMetaIndex = Buffer.concat([this.stringMetaIndex, Buffer.allocUnsafe(this.stringMetaIndex.length)]);
    }
  }

  set(id: number | string, offset: number) {
    const pos = this.buffer.length;

    let entrySize = 8;
    let keySize = 4;
    if (this.keyType === "string") {
      keySize = Buffer.byteLength(id as string, "utf-8");
      entrySize = 2 + keySize + 4;
    }


    // const newEntry: Buffer = Buffer.alloc(entrySize);

    if (this.keyType === "number") {

      const length = Math.floor(this.bufferLength / entrySize);

      let low = 0;
      let high = length - 1;
      let pos = 0;

      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        pos = mid * entrySize;
        const currentId = this.buffer.readUInt32BE(pos);

        if (currentId === (id as number)) {
          this.buffer.writeUInt32BE(offset, pos + 4);
          return;
        }

        if (currentId < (id as number)) {
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }

      let newBufferLength = this.bufferLength + entrySize;
      if (newBufferLength > this.buffer.length) {
        this.widenBuffers();
      }
      // shift entries to make space for new entry
      this.buffer.copy(this.buffer, pos + entrySize, pos, this.bufferLength);

      this.buffer.writeUInt32BE(id as number, pos);
      this.buffer.writeUInt32BE(offset, pos + 4);
      this.bufferLength = newBufferLength;

    } else {

      const length = Math.floor(this.metaIndexLength / 4);

      let low = 0;
      let high = length - 1;
      let pos = 0;

      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        pos = this.stringMetaIndex!.readUInt32BE(mid * 4);

        let cmp = Index.compareStringBuffer(id as string, this.buffer, pos + 2, keySize);

        if (cmp === 0) {
          // this.stringMetaIndex!.writeUInt32BE(pos, mid * 4);
          this.buffer.writeUInt32BE(offset, pos + 2 + keySize);
          return;
        }

        if (cmp < 0) {
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }

      let newBufferLength = this.bufferLength + entrySize;
      if (newBufferLength > this.buffer.length) {
        this.widenBuffers();
      }

      // shift entries to make space for new entry
      this.buffer.copy(this.buffer, pos + entrySize, pos, this.bufferLength);

      this.buffer.writeUInt16BE(keySize, pos);
      this.buffer.write(id as string, pos + 2, "utf-8");
      this.buffer.writeUInt32BE(offset, pos + 2 + keySize);
      this.bufferLength = newBufferLength;

      this.stringMetaIndex!.copy(this.stringMetaIndex!, (low + 1) * 4, low * 4, this.metaIndexLength);
      this.stringMetaIndex!.writeUInt32BE(pos, low * 4);
      this.metaIndexLength += 4;
    }

  }

  delete(id: number | string) {
    if (this.keyType === "number") {

      const entrySize = 8;
      const length = Math.floor(this.bufferLength / entrySize);

      let low = 0;
      let high = length - 1;
      let pos = 0;


      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        pos = mid * entrySize;
        const currentId = this.buffer.readUInt32BE(pos);

        if (currentId === (id as number)) {
          this.buffer.writeUInt32BE(TOMBSTONE, pos + 4);
          return;
        }

        if (currentId < (id as number)) {
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }

    } else {
      throw new Error("Not implemented yet");
      // let metaEntry = Buffer.alloc(4);
      // metaEntry.writeUInt32BE(0, this.buffer.length);
      // this.stringMetaIndex = Buffer.concat([this.stringMetaIndex!, metaEntry]);


    }
  }

  fastFill(keyValuePairs: { key: number | string, offset: number }[], startingBufferSize: number): void
  fastFill(fn: (index: number) => Buffer, length: number, startingBufferSize: number): void
  fastFill(arg1: any, arg2: any, arg3?: any) {
    let startingBufferSize: number;
    let length: number;

    if (typeof arg3 === "number") {
      startingBufferSize = arg3;
      length = arg2;
    } else {
      startingBufferSize = arg2;
      length = arg1.length;
    }

    this.buffer = Buffer.allocUnsafe(startingBufferSize);
    this.bufferLength = 0;

    if (this.keyType === "number") {


      for (let i = 0; i < length; i++) {

        let resultLength = this.bufferLength + 8;

        if (resultLength > this.buffer.length) {
          this.widenBuffers();
        }

        let buf: Buffer;
        if (typeof arg1 === "function") {
          buf = arg1(i);
        } else {
          buf = Buffer.allocUnsafe(8);
          buf.writeInt32BE(arg1[i].key, 0);
          buf.writeInt32BE(arg1[i].offset, 4);
        }

        this.buffer.set(buf, this.bufferLength);
        this.bufferLength = resultLength
      }

    } else {
      this.stringMetaIndex = Buffer.allocUnsafe(startingBufferSize / 2);

      for (let i = 0; i < length; i++) {

        let keySize = Buffer.byteLength(arg1[i].key, "utf-8");
        let recordSize = 2 + keySize + 4;
        let resultLength = this.bufferLength + recordSize;

        if (resultLength > this.buffer.length) {
          this.widenBuffers();
        }

        let buf: Buffer;
        if (typeof arg1 === "function") {
          buf = arg1(i);
        } else {
          buf = Buffer.allocUnsafe(recordSize);
          buf.writeInt16BE(arg1[i].key.length, 0);
          buf.write(arg1[i].key, 2, "utf-8");
          buf.writeInt32BE(arg1[i].offset, 2 + keySize);
        }


        this.stringMetaIndex.writeUInt32BE(this.bufferLength, i * 4);

        this.buffer.set(buf, this.bufferLength);
        this.bufferLength = resultLength;
        this.metaIndexLength = (i + 1) * 4;
      }
    }
  }

  compact() {
    const newBuffer = Buffer.allocUnsafe(this.buffer.length);
    if (this.keyType === "number") {
      const entrySize = 8;
      let writePos = 0;
      for (let readPos = 0; readPos < this.bufferLength; readPos += entrySize) {
        const offset = this.buffer.readUInt32BE(readPos + 4);
        if (offset !== TOMBSTONE) {
          newBuffer.copy(this.buffer, writePos, readPos, readPos + entrySize);
          writePos += entrySize;
        }
      }
      this.bufferLength = writePos;
      this.buffer = newBuffer;
    } else {
      const newMetaIndex = Buffer.allocUnsafe(this.stringMetaIndex!.length);
      let newMetaIndexLength = 0;
      let writePos = 0;


      for (let i = 0; i < this.metaIndexLength / 4; i++) {
        let recordPos = this.stringMetaIndex!.readUInt32BE(i * 4);
        const idLen = this.buffer.readUInt16BE(recordPos);
        const offset = this.buffer.readUInt32BE(recordPos + 2 + idLen);
        if (offset !== TOMBSTONE) {
          newBuffer.copy(this.buffer, writePos, recordPos, recordPos + 2 + idLen + 4);
          newMetaIndex.writeUInt32BE(writePos, i * 4);
          writePos += 2 + idLen + 4;
          newMetaIndexLength += 4;
        }
      }
      this.buffer = newBuffer;
      this.stringMetaIndex = newMetaIndex;
      this.bufferLength = writePos;
      this.metaIndexLength = newMetaIndexLength; // ensure it's a multiple of 4
    }
  }

  save() {
    this.compact();
    fs.writeFileSync(this.path, this.buffer.subarray(0, this.bufferLength));

    if (this.keyType === "string") {
      // throw new Error("Not implemented yet");
      fs.writeFileSync(this.path + "_str", this.stringMetaIndex!.subarray(0, this.metaIndexLength));
    }
  }

  reset() {
    if (!fs.existsSync(this.path)) {
      fs.writeFileSync(this.path, Buffer.alloc(0));
    }
    const stats = fs.statSync(this.path);
    const fileSize = stats.size;

    this.buffer = Buffer.allocUnsafe(Math.max(fileSize * 1.5, MIN_BUFFER_SIZE));

    this.bufferLength = fileSize;

    console.log(this.buffer.length, this.bufferLength);
    fs.readSync(fs.openSync(this.path, 'r'), this.buffer, 0, this.bufferLength, 0);

    if (this.keyType === "string") {
      const metaPath = this.path + "_str";
      if (!fs.existsSync(metaPath)) {
        fs.writeFileSync(metaPath, Buffer.alloc(0));
      }
      const metaFileSize = fs.statSync(metaPath).size;
      this.metaIndexLength = Math.min(metaFileSize, this.buffer.length / 2);
      this.stringMetaIndex = Buffer.allocUnsafe(this.buffer.length / 2);
      fs.readSync(fs.openSync(this.path + "_str", 'r'), this.stringMetaIndex, 0, this.metaIndexLength, 0);
    }
  }

  static compareStringBuffer(str: string, buf: Buffer, offset: number, idLen: number): number {
    const target = Buffer.from(str, "utf-8");
    const minLen = Math.min(idLen, target.length);

    for (let i = 0; i < minLen; i++) {
      const a = buf[offset + i];
      const b = target[i];
      if (a !== b) return a - b;
    }

    if (idLen === target.length) return 0;
    return idLen - target.length;
  }
}