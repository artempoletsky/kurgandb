import * as fs from "fs";


const MIN_BUFFER_SIZE = 1024 * 1024; // 1MB
const TOMBSTONE = 0xFFFFFFFF;
const FIXED_RECORD_SIZE = 10; // 4 bytes for offset, 2 bytes for id length, 4 bytes for id position
const ID_LEN_OFFSET = 4;
const ID_POS_OFFSET = 6;

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
  protected pathVariable: string;

  protected fixedBuffer!: Buffer; // file structure is [offset (4 bytes)][id_length (2 bytes)][id_position (4 bytes)]
  protected variableBuffer!: Buffer; // file structure is [variable_length]

  protected bufferFixedLength = 0;
  protected bufferVariableLength = 0;

  getFixedBuffer() {
    return this.fixedBuffer.subarray(0, this.bufferFixedLength);
  }

  getVariableBuffer() {
    return this.variableBuffer.subarray(0, this.bufferVariableLength);
  }

  getFixedBufferLength() {
    return this.bufferFixedLength;
  }

  getVariableBufferLength() {
    return this.bufferVariableLength;
  }

  get(id: string): number {
    const { pos, found } = this.binarySearch(id);

    if (found) {
      const result = this.fixedBuffer.readUInt32BE(pos);
      if (result == TOMBSTONE) {
        return -1;
      }
      return result;
    }

    return -1;
  }


  widenBuffers() {
    this.fixedBuffer = Buffer.concat([this.fixedBuffer, Buffer.allocUnsafe(this.fixedBuffer.length)]);
    this.variableBuffer = Buffer.concat([this.variableBuffer, Buffer.allocUnsafe(this.variableBuffer.length)]);
  }

  binarySearch(id: string) {
    const length = Math.floor(this.bufferFixedLength / FIXED_RECORD_SIZE);
    let low = 0;
    let high = length - 1;
    let pos = 0;
    let idPos = 0;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      pos = mid * FIXED_RECORD_SIZE;
      // file structure is [offset (4 bytes)][id_length (2 bytes)][id_position (4 bytes)]
      // const offset = this.buffer.readUInt32BE(pos);
      const idLen = this.fixedBuffer.readUInt16BE(pos + ID_LEN_OFFSET);
      idPos = this.fixedBuffer.readUInt32BE(pos + ID_POS_OFFSET);

      const cmp = IndexOneString.compareStringBuffer(id, this.variableBuffer, idPos, idLen);

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


  set(id: string, offset: number) {

    const { pos, idPos, found } = this.binarySearch(id);


    let idLen = Buffer.byteLength(id, "utf-8");


    if (found) {
      this.fixedBuffer.writeUInt32BE(offset, pos);
      return;
    }

    //todo: widen buffers if necessary before shifting

    //shift records to the right to make space for the new record
    this.fixedBuffer.copy(this.fixedBuffer, pos + FIXED_RECORD_SIZE, pos, this.bufferFixedLength);
    this.bufferFixedLength += FIXED_RECORD_SIZE;



    this.fixedBuffer.writeUInt32BE(offset, pos);
    this.fixedBuffer.writeUInt16BE(idLen, pos + ID_LEN_OFFSET);


    this.fixedBuffer.writeUInt32BE(this.bufferVariableLength, pos + ID_POS_OFFSET);
    this.variableBuffer.write(id, this.bufferVariableLength, "utf-8");
    this.bufferVariableLength += idLen;


    // the record fits in place, we can just overwrite it
    // if (idLen <= idMaxLen) {
    //   this.bufferFixed.writeUInt32BE(idPos, pos + 8);
    //   this.bufferVariable.set(Buffer.from(id, "utf-8"), idPos);
    // } else {
    //   let newIdsBufferLength = this.bufferVariableLength + idMaxLen;
    //   if (newIdsBufferLength > this.bufferVariable.length) {
    //     this.widenBuffers(); // todo: optimize by only widening the idsBuffer
    //   }







  }



  delete(id: string) {

    const { pos, idPos, found } = this.binarySearch(id);

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

    let startingBufferSize = Math.max(approximateIdLen * length, MIN_BUFFER_SIZE);

    this.fixedBuffer = Buffer.alloc(Math.max(length * FIXED_RECORD_SIZE * 2, MIN_BUFFER_SIZE));
    this.bufferFixedLength = 0;

    this.variableBuffer = Buffer.alloc(startingBufferSize);

    for (let i = 0; i < length; i++) {

      let idLen = Buffer.byteLength(arg1[i].key, "utf-8");

      let resultIdsBufferLength = this.bufferFixedLength + idLen;
      if (resultIdsBufferLength > this.variableBuffer.length) {
        this.widenBuffers();
      }


      let buf: Buffer;
      if (typeof arg1 === "function") {
        buf = arg1(i);
      } else {

        // file structure is [offset (4 bytes)][id_length (2 bytes)][id_max_length (2 bytes)][id_position (4 bytes)]

        buf = Buffer.allocUnsafe(FIXED_RECORD_SIZE);
        buf.writeUInt32BE(arg1[i].offset, 0);
        buf.writeUInt16BE(idLen, ID_LEN_OFFSET);
        buf.writeUInt32BE(this.bufferVariableLength, ID_POS_OFFSET);

      }

      this.variableBuffer.write(arg1[i].key, this.bufferVariableLength, "utf-8");
      this.bufferVariableLength = resultIdsBufferLength;

      this.fixedBuffer.set(buf, this.bufferFixedLength);
      this.bufferFixedLength += FIXED_RECORD_SIZE;
    }

  }

  readFixedRecord(position: number) {
    return {
      offset: this.fixedBuffer.readUInt32BE(position),
      idLen: this.fixedBuffer.readUInt16BE(position + ID_LEN_OFFSET),
      idPos: this.fixedBuffer.readUInt32BE(position + ID_POS_OFFSET)
    }
  }

  compact() {
    const newFixedBuffer = Buffer.allocUnsafe(this.fixedBuffer.length);

    const newVariableBuffer = Buffer.allocUnsafe(this.variableBuffer.length);

    let fixedWritePos = 0;
    let variableWritePos = 0;


    for (let i = 0; i < this.bufferFixedLength / FIXED_RECORD_SIZE; i++) {
      const recordPos = i * FIXED_RECORD_SIZE;
      const offset = this.fixedBuffer.readUInt32BE(recordPos);
      if (offset !== TOMBSTONE) {

        const idLen = this.fixedBuffer.readUInt16BE(recordPos + ID_LEN_OFFSET);
        const idPos = this.fixedBuffer.readUInt32BE(recordPos + ID_POS_OFFSET);

        this.fixedBuffer.copy(newFixedBuffer, fixedWritePos, recordPos, recordPos + FIXED_RECORD_SIZE);
        fixedWritePos += FIXED_RECORD_SIZE;
        
        this.variableBuffer.copy(newVariableBuffer, variableWritePos, idPos, idPos + idLen);
        variableWritePos += idLen;
      }
    }

    this.fixedBuffer = newFixedBuffer;
    this.variableBuffer = newVariableBuffer;
    this.bufferFixedLength = fixedWritePos;
    this.bufferVariableLength = variableWritePos;

  }


  save() {
    this.compact();
    fs.writeFileSync(this.pathFixed, this.fixedBuffer.subarray(0, this.bufferFixedLength));
    fs.writeFileSync(this.pathVariable, this.variableBuffer.subarray(0, this.bufferVariableLength));
  }

  reset() {
    if (!fs.existsSync(this.pathFixed)) {
      fs.writeFileSync(this.pathFixed, Buffer.alloc(0));
    }
    const stats = fs.statSync(this.pathFixed);
    const fileSize = stats.size;

    this.fixedBuffer = Buffer.allocUnsafe(Math.max(fileSize * 1.5, MIN_BUFFER_SIZE));

    this.bufferFixedLength = fileSize;

    fs.readSync(fs.openSync(this.pathFixed, 'r'), this.fixedBuffer, 0, this.bufferFixedLength, 0);


    if (!fs.existsSync(this.pathVariable)) {
      fs.writeFileSync(this.pathVariable, Buffer.alloc(0));
    }
    const variableFileSize = fs.statSync(this.pathVariable).size;
    this.bufferVariableLength = variableFileSize;
    this.variableBuffer = Buffer.allocUnsafe(Math.max(variableFileSize * 2, MIN_BUFFER_SIZE));
    fs.readSync(fs.openSync(this.pathVariable, 'r'), this.variableBuffer, 0, this.bufferVariableLength, 0);

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