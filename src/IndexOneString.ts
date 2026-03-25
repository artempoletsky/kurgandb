import * as fs from "fs";


const MIN_BUFFER_SIZE = 1024 * 1024; // 1MB
const TOMBSTONE = 0xFFFFFFFF;
const FIXED_RECORD_SIZE = 12; // 4 bytes for offset, 2 bytes for id length, 2 bytes for max id length, 4 bytes for id position

export class IndexOneString {

  constructor(path: string)
  constructor(tableName: string, columnName: string)
  constructor(a1: string, a2?: string) {
    const path = a1;
    if (a2) {
      throw new Error("Not implemented yet");
    }

    this.pathFixed = path;
    this.pathVariable = path + ".txt";
    this.reset();
  }

  protected pathFixed: string;
  protected pathVariable;

  protected bufferFixed!: Buffer; // file structure is [offset (4 bytes)][id_length (2 bytes)][id_max_length (2 bytes)][id_position (4 bytes)]
  protected bufferVariable!: Buffer; // file structure is [variable_length]

  protected bufferFixedLength = 0;
  protected bufferVariableLength = 0;

  getFixedBuffer() {
    return this.bufferFixed.subarray(0, this.bufferFixedLength);
  }

  getVariableBuffer() {
    return this.bufferVariable.subarray(0, this.bufferVariableLength);
  }

  getFixedBufferLength() {
    return this.bufferFixedLength;
  }

  getVariableBufferLength() {
    return this.bufferVariableLength;
  }

  get(id: string): number {
    const { pos, found } = this.binarySearchString(id);

    if (found) {
      const result = this.bufferFixed.readUInt32BE(pos);
      if (result == TOMBSTONE) {
        return -1;
      }
      return result;
    }

    return -1;
  }


  widenBuffers() {
    this.bufferFixed = Buffer.concat([this.bufferFixed, Buffer.allocUnsafe(this.bufferFixed.length)]);
    this.bufferVariable = Buffer.concat([this.bufferVariable, Buffer.allocUnsafe(this.bufferVariable.length)]);
  }

  binarySearchString(id: string) {
    const length = Math.floor(this.bufferFixedLength / FIXED_RECORD_SIZE);
    let low = 0;
    let high = length - 1;
    let pos = 0;
    let idPos = 0;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      pos = mid * 10;
      // file structure is [offset (4 bytes)][id_length (2 bytes)][id_max_length (2 bytes)][id_position (4 bytes)]
      // const offset = this.buffer.readUInt32BE(pos);
      const idLen = this.bufferFixed.readUInt16BE(pos + 4);
      idPos = this.bufferFixed.readUInt32BE(pos + 8);

      const cmp = IndexOneString.compareStringBuffer(id, this.bufferVariable, idPos, idLen);

      if (cmp === 0) {
        return { pos, idPos, found: true };
      }

      if (cmp < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return { pos, idPos, found: false };

  }

  static computeMaxLen(currentLen: number) {
    return Math.max(currentLen * 2, 16);
  }

  set(id: string, offset: number) {

    const { pos, idPos, found } = this.binarySearchString(id);


    let idLen = Buffer.byteLength(id, "utf-8");


    if (found) {
      this.bufferFixed.writeUInt32BE(offset, pos);
      return;
    }

    //todo: widen buffers if necessary before shifting

    //shift records to the right to make space for the new record
    this.bufferFixed.copy(this.bufferFixed, pos + FIXED_RECORD_SIZE, pos, this.bufferFixedLength);
    this.bufferFixedLength += FIXED_RECORD_SIZE;
    const idMaxLen = IndexOneString.computeMaxLen(idLen);


    this.bufferFixed.writeInt32BE(offset, pos);
    this.bufferFixed.writeInt16BE(idLen, pos + 4);
    this.bufferFixed.writeInt16BE(idMaxLen, pos + 6);

    this.bufferFixed.writeInt32BE(this.bufferVariableLength, pos + 8);
    this.bufferVariable.write(id, this.bufferVariableLength, "utf-8");
    this.bufferVariableLength += idMaxLen;


    // the record fits in place, we can just overwrite it
    // if (idLen <= idMaxLen) {
    //   this.bufferFixed.writeInt32BE(idPos, pos + 8);
    //   this.bufferVariable.set(Buffer.from(id, "utf-8"), idPos);
    // } else {
    //   let newIdsBufferLength = this.bufferVariableLength + idMaxLen;
    //   if (newIdsBufferLength > this.bufferVariable.length) {
    //     this.widenBuffers(); // todo: optimize by only widening the idsBuffer
    //   }







  }



  delete(id: string) {

    const { pos, idPos, found } = this.binarySearchString(id);

    if (!found) return;

    this.bufferFixed.writeUInt32BE(TOMBSTONE, pos);
  }

  fastFill(keyValuePairs: { key: string, offset: number }[], approximateIdLen: number): void
  fastFill(fn: (index: number) => Buffer, length: number, approximateIdLen: number): void
  fastFill(arg1: any, arg2: any, arg3?: any) {
    let approximateIdLen: number;
    let length: number;

    if (typeof arg3 === "number") {
      approximateIdLen = arg3;
      length = arg2;
    } else {
      approximateIdLen = arg2;
      length = arg1.length;
    }

    let startingBufferSize = Math.max(IndexOneString.computeMaxLen(approximateIdLen) * length, MIN_BUFFER_SIZE);

    this.bufferFixed = Buffer.allocUnsafe(length * 12 * 2);
    this.bufferFixedLength = 0;

    this.bufferVariable = Buffer.allocUnsafe(startingBufferSize);

    for (let i = 0; i < length; i++) {

      let idLen = Buffer.byteLength(arg1[i].key, "utf-8");
      let maxIdLen = IndexOneString.computeMaxLen(idLen);

      let resultIdsBufferLength = this.bufferFixedLength + maxIdLen;
      if (resultIdsBufferLength > this.bufferVariable.length) {
        this.widenBuffers();
      }


      let buf: Buffer;
      if (typeof arg1 === "function") {
        buf = arg1(i);
      } else {

        // file structure is [offset (4 bytes)][id_length (2 bytes)][id_max_length (2 bytes)][id_position (4 bytes)]

        buf = Buffer.allocUnsafe(12);
        buf.writeInt32BE(arg1[i].offset, 0);
        buf.writeInt16BE(idLen, 4);
        buf.writeInt16BE(maxIdLen, 6);
        buf.writeInt32BE(this.bufferVariableLength, 8);

      }

      this.bufferVariable.write(arg1[i].key, this.bufferVariableLength, "utf-8");
      this.bufferVariableLength = resultIdsBufferLength;

      this.bufferFixed.set(buf, this.bufferFixedLength);
      this.bufferFixedLength += 12;
    }

  }

  readFixedRecord(position: number) {
    return {
      offset: this.bufferFixed.readUInt32BE(position),
      idLen: this.bufferFixed.readUInt16BE(position + 4),
      idMaxLen: this.bufferFixed.readUInt16BE(position + 6),
      idPos: this.bufferFixed.readUInt32BE(position + 8)
    }
  }

  compact() {
    const newFixedBuffer = Buffer.allocUnsafe(this.bufferFixed.length);

    const newVariableBuffer = Buffer.allocUnsafe(this.bufferVariable.length);

    let fixedWritePos = 0;
    let variableWritePos = 0;


    for (let i = 0; i < this.bufferFixedLength / FIXED_RECORD_SIZE; i++) {
      const recordPos = i * FIXED_RECORD_SIZE;
      const offset = this.bufferFixed.readUInt32BE(recordPos);
      if (offset !== TOMBSTONE) {

        const idLen = this.bufferFixed.readUInt16BE(recordPos + 4);
        const idMaxLen = this.bufferFixed.readUInt16BE(recordPos + 6);
        const idPos = this.bufferFixed.readUInt32BE(recordPos + 8);

        this.bufferFixed.copy(newFixedBuffer, fixedWritePos, recordPos, recordPos + FIXED_RECORD_SIZE);
        fixedWritePos += FIXED_RECORD_SIZE;

        this.bufferVariable.copy(newVariableBuffer, variableWritePos, idPos, idPos + idLen);
        variableWritePos += idMaxLen;
      }
    }

    this.bufferFixed = newFixedBuffer;
    this.bufferVariable = newVariableBuffer;
    this.bufferFixedLength = fixedWritePos;
    this.bufferVariableLength = variableWritePos;

  }


  save() {
    this.compact();
    fs.writeFileSync(this.pathFixed, this.bufferFixed.subarray(0, this.bufferFixedLength));
    fs.writeFileSync(this.pathVariable, this.bufferVariable.subarray(0, this.bufferVariableLength));
  }

  reset() {
    if (!fs.existsSync(this.pathFixed)) {
      fs.writeFileSync(this.pathFixed, Buffer.alloc(0));
    }
    const stats = fs.statSync(this.pathFixed);
    const fileSize = stats.size;

    this.bufferFixed = Buffer.allocUnsafe(Math.max(fileSize * 1.5, MIN_BUFFER_SIZE));

    this.bufferFixedLength = fileSize;

    fs.readSync(fs.openSync(this.pathFixed, 'r'), this.bufferFixed, 0, this.bufferFixedLength, 0);


    if (!fs.existsSync(this.pathVariable)) {
      fs.writeFileSync(this.pathVariable, Buffer.alloc(0));
    }
    const variableFileSize = fs.statSync(this.pathVariable).size;
    this.bufferVariableLength = variableFileSize;
    this.bufferVariable = Buffer.allocUnsafe(Math.max(variableFileSize * 2, MIN_BUFFER_SIZE));
    fs.readSync(fs.openSync(this.pathVariable, 'r'), this.bufferVariable, 0, this.bufferVariableLength, 0);

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