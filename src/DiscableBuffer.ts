import fs from "fs";

export default class DiscableBuffer {
  protected memory: Buffer | null = null;
  protected fd: number = 0;
  protected maxSize: number;
  protected path: string;

  getPath() {
    return this.path;
  }

  get usingMemory() {
    return this.memory != null;
  }

  constructor(path: string, maxMemorySize: number) {
    this.path = path;
    this.maxSize = maxMemorySize;
    this.reset();
  }

  reset() {
    this.memory = Buffer.alloc(this.maxSize);
  }

  copy(target: Buffer, position: number, length?: number) {
    if (!length)
      length = target.byteLength;

    if (this.memory) {
      this.memory.copy(target, 0, position, target.length + position);
      return;
    }
    fs.readSync(this.fd, target, 0, length, position);
  }

  write(data: Buffer, offset: number) {


    this.spitToDiskIf(data.byteLength + offset);

    if (!this.memory) {
      fs.writeSync(this.fd, data, 0, data.byteLength, offset);
      return;
    }

    data.copy(this.memory, offset);
  }

  spitToDiskIf(lastByte: number) {
    if (!this.memory) { //is already working on the disk
      return;
    }
    if (lastByte < this.maxSize) { //the data still fits in the memory
      return;
    }
    this.fd = fs.openSync(this.path, "w+");
    // this.writeWithPadding(this.memory, this.fd, 0, this.currentWritePosPatch);

    fs.writeSync(this.fd, this.memory, 0, this.memory.byteLength, 0);
    this.memory = null;
  }
}