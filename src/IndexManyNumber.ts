import * as fs from "fs";


const MIN_BUFFER_SIZE = 1024 * 1024; // 1MB
const TOMBSTONE = 0xFFFFFFFF;
const FIXED_RECORD_SIZE = 12;

export class IndexManyNumber {

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

  protected fixedBuffer!: Buffer; // file structure is [id (4 bytes)][start (4 bytes)][len (4 bytes)]
  protected offsetsBuffer!: Buffer; // file structure is [offset (4 bytes)]

  protected fixedBufferLength = 0;
  protected offsetsBufferLength = 0;

  getFixedBuffer() {
    return this.fixedBuffer.subarray(0, this.fixedBufferLength);
  }

  getOffsetsBuffer() {
    return this.offsetsBuffer.subarray(0, this.offsetsBufferLength);
  }

  getFixedBufferLength() {
    return this.fixedBufferLength;
  }

  getVariableBufferLength() {
    return this.offsetsBufferLength;
  }

  get(value: number): number[] {
    const { pos, found } = this.binarySearch(value);
    if (!found) return [];
    return this.readOffsetsAtPositionInFixedBuffer(pos);
  }


  widenBuffers() {
    this.fixedBuffer = Buffer.concat([this.fixedBuffer, Buffer.allocUnsafe(this.fixedBuffer.length)]);
    this.offsetsBuffer = Buffer.concat([this.offsetsBuffer, Buffer.allocUnsafe(this.offsetsBuffer.length)]);
  }


  readOffsetsAtPositionInFixedBuffer(pos: number) {

    const start = this.fixedBuffer.readUInt16BE(pos + 4);
    const len = this.fixedBuffer.readUInt16BE(pos + 8);

    const offsets: number[] = [];
    for (let i = 0; i < len; i++) {
      offsets.push(this.offsetsBuffer.readUint32BE(start + i * 4));
    }

    return offsets;
  }

  binarySearch(value: number) {
    const length = Math.floor(this.fixedBufferLength / FIXED_RECORD_SIZE);
    let low = 0;
    let high = length - 1;
    let pos = 0;
    let idPos = 0;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      pos = mid * FIXED_RECORD_SIZE;
      // file structure is [start (4 bytes)][len (4 bytes)]
      // const offset = this.buffer.readUInt32BE(pos);

      const currentId = this.fixedBuffer.readUInt16BE(pos);
      if (currentId == value) {
        return { pos, found: true };
      }


      if (currentId < value) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return { pos, found: false };

  }

  static computeMaxLen(currentLen: number) {
    return Math.max(currentLen * 2, 16);
  }

  setArray(value: number, offsets: number[]) {

    const { pos, found } = this.binarySearch(value);




    if (found) {
      const currentLen = this.fixedBuffer.readUInt32BE(pos + 8);
      let start;
      if (currentLen >= offsets.length) {
        start = this.fixedBuffer.readUInt32BE(pos + 4);
      } else {
        start = this.offsetsBufferLength;
      }
      for (let i = 0; i < offsets.length; i++) {
        this.offsetsBuffer.writeUint32BE(offsets[i], start + i * 4);
      }

      return;

    }

    //todo: widen buffers if necessary before shifting

    //shift records to the right to make space for the new record
    this.fixedBuffer.copy(this.fixedBuffer, pos + FIXED_RECORD_SIZE, pos, this.fixedBufferLength);
    this.fixedBufferLength += FIXED_RECORD_SIZE;
    const idMaxLen = IndexOneString.computeMaxLen(idLen);


    this.fixedBuffer.writeInt32BE(offset, pos);
    this.fixedBuffer.writeInt16BE(idLen, pos + 4);
    this.fixedBuffer.writeInt16BE(idMaxLen, pos + 6);

    this.fixedBuffer.writeInt32BE(this.offsetsBufferLength, pos + 8);
    this.bufferVariable.write(value, this.offsetsBufferLength, "utf-8");
    this.offsetsBufferLength += idMaxLen;


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

    this.fixedBuffer.writeUInt32BE(TOMBSTONE, pos);
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

    this.fixedBuffer = Buffer.allocUnsafe(length * 12 * 2);
    this.fixedBufferLength = 0;

    this.bufferVariable = Buffer.allocUnsafe(startingBufferSize);

    for (let i = 0; i < length; i++) {

      let idLen = Buffer.byteLength(arg1[i].key, "utf-8");
      let maxIdLen = IndexOneString.computeMaxLen(idLen);

      let resultIdsBufferLength = this.fixedBufferLength + maxIdLen;
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
        buf.writeInt32BE(this.offsetsBufferLength, 8);

      }

      this.bufferVariable.write(arg1[i].key, this.offsetsBufferLength, "utf-8");
      this.offsetsBufferLength = resultIdsBufferLength;

      this.fixedBuffer.set(buf, this.fixedBufferLength);
      this.fixedBufferLength += 12;
    }

  }

  readFixedRecord(position: number) {
    return {
      offset: this.fixedBuffer.readUInt32BE(position),
      idLen: this.fixedBuffer.readUInt16BE(position + 4),
      idMaxLen: this.fixedBuffer.readUInt16BE(position + 6),
      idPos: this.fixedBuffer.readUInt32BE(position + 8)
    }
  }

  compact() {
    const newFixedBuffer = Buffer.allocUnsafe(this.fixedBuffer.length);

    const newVariableBuffer = Buffer.allocUnsafe(this.bufferVariable.length);

    let fixedWritePos = 0;
    let variableWritePos = 0;


    for (let i = 0; i < this.fixedBufferLength / FIXED_RECORD_SIZE; i++) {
      const recordPos = i * FIXED_RECORD_SIZE;
      const offset = this.fixedBuffer.readUInt32BE(recordPos);
      if (offset !== TOMBSTONE) {

        const idLen = this.fixedBuffer.readUInt16BE(recordPos + 4);
        const idMaxLen = this.fixedBuffer.readUInt16BE(recordPos + 6);
        const idPos = this.fixedBuffer.readUInt32BE(recordPos + 8);

        this.fixedBuffer.copy(newFixedBuffer, fixedWritePos, recordPos, recordPos + FIXED_RECORD_SIZE);
        fixedWritePos += FIXED_RECORD_SIZE;

        this.bufferVariable.copy(newVariableBuffer, variableWritePos, idPos, idPos + idLen);
        variableWritePos += idMaxLen;
      }
    }

    this.fixedBuffer = newFixedBuffer;
    this.bufferVariable = newVariableBuffer;
    this.fixedBufferLength = fixedWritePos;
    this.offsetsBufferLength = variableWritePos;

  }


  save() {
    this.compact();
    fs.writeFileSync(this.pathFixed, this.fixedBuffer.subarray(0, this.fixedBufferLength));
    fs.writeFileSync(this.pathVariable, this.bufferVariable.subarray(0, this.offsetsBufferLength));
  }

  reset() {
    if (!fs.existsSync(this.pathFixed)) {
      fs.writeFileSync(this.pathFixed, Buffer.alloc(0));
    }
    const stats = fs.statSync(this.pathFixed);
    const fileSize = stats.size;

    this.fixedBuffer = Buffer.allocUnsafe(Math.max(fileSize * 1.5, MIN_BUFFER_SIZE));

    this.fixedBufferLength = fileSize;

    fs.readSync(fs.openSync(this.pathFixed, 'r'), this.fixedBuffer, 0, this.fixedBufferLength, 0);


    if (!fs.existsSync(this.pathVariable)) {
      fs.writeFileSync(this.pathVariable, Buffer.alloc(0));
    }
    const variableFileSize = fs.statSync(this.pathVariable).size;
    this.offsetsBufferLength = variableFileSize;
    this.bufferVariable = Buffer.allocUnsafe(Math.max(variableFileSize * 2, MIN_BUFFER_SIZE));
    fs.readSync(fs.openSync(this.pathVariable, 'r'), this.bufferVariable, 0, this.offsetsBufferLength, 0);

  }

}