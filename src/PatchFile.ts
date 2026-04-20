import fs from "fs";
import BytePageView from "./PageViewArray";
import DiscableBuffer from "./DiscableBuffer";
import CommitQueue from "./CommitQueue";
import { pipeline } from "stream/promises";
import Superblock, { TSuperblock } from "./PageViewSuperblock";

type OFFSETS_STRUCTURE_KEY = "offsetPatch" | "offsetFile" | "length";

const OFFSETS_STRUCTURE = new Map<OFFSETS_STRUCTURE_KEY, number>([
  ["offsetPatch", 4],
  ["offsetFile", 4],
  ["length", 4],
]);


export default class PatchFile {
  protected path: string;
  protected fd!: number;
  protected patchOffsets!: Map<number, TSuperblock<OFFSETS_STRUCTURE_KEY>>;
  protected writePosPatch: number = 0;
  protected writePosFile: number = 0;

  protected offsetsLength: number = 0;
  protected memoryOffsetsSize = 1024 * 1024;

  protected memoryPatchSize = 10 * 1024 * 1024;


  protected patchBuffer: DiscableBuffer;
  protected commitQueueId: string;
  constructor(path: string) {
    this.path = path;
    this.patchOffsets = new Map();
    this.patchBuffer = new DiscableBuffer(path + ".patch", this.memoryPatchSize);
    this.commitQueueId = CommitQueue.register("PatchFile_");
    this.reset();
  }

  reset() {
    if (!fs.existsSync(this.path)) {
      fs.writeFileSync(this.path, Buffer.alloc(0));
    }

    if (!this.fd) {
      this.fd = fs.openSync(this.path, "r+");
    }

    this.patchOffsets = new Map();

    let stat = fs.statSync(this.path);
    this.writePosFile = stat.size;

    this.writePosPatch = 0;
    this.patchBuffer.reset();
  }

  read(buffer: Buffer, offset: number, length: number) {

    let isTouched = this.patchOffsets.has(offset);
    if (isTouched) {
      return this.readPatch(buffer, offset, length);
    } else {
      let bytesRead = fs.readSync(this.fd, buffer, 0, length, offset);
      if (bytesRead < length) {
        buffer.fill(0, bytesRead);
      }
    }
  }

  readPatch(buffer: Buffer, fileOffset: number, length: number) {
    let meta = this.patchOffsets.get(fileOffset);
    if (!meta)
      throw new Error("PatchFile: trying to read the patch at the wrong index");

    if (length > meta.length)
      throw new Error("PatchFile: the reading length exceeds the max length of the record");

    return this.patchBuffer.copy(buffer, meta.offsetPatch, length);
  }

  write(fileOffset: number, data: Buffer) {

    let meta = this.patchOffsets.get(fileOffset);
    if (!meta || meta.length < data.byteLength) {
      meta = Superblock.create(OFFSETS_STRUCTURE);
      meta.offsetFile = fileOffset;
      meta.length = data.length;
      meta.offsetPatch = this.writePosPatch;
      this.writePosPatch += data.length;
      this.patchOffsets.set(fileOffset, meta);
    }

    if (fileOffset == this.writePosFile) {
      this.writePosFile += data.length;
    }

    this.patchBuffer.write(data, meta.offsetPatch);
  }

  writeEnd(data: Buffer) {
    this.write(this.writePosFile, data);
  }



  async commit() {
    if (!this.patchOffsets.size) return;
    CommitQueue.start(this.commitQueueId);
    if (this.patchBuffer.usingMemory) {
      let buffer = this.patchBuffer.memory!;

      for (const [offsetFile, { offsetPatch, length }] of this.patchOffsets) {
        await new Promise(resolve => {
          fs.write(this.fd, buffer, offsetPatch, length, offsetFile, resolve);
        });
      }
    } else {
      let p = this.path + ".patch";

      for (const [offsetFile, { offsetPatch, length }] of this.patchOffsets) {
        const readStream = fs.createReadStream(p, {
          start: offsetPatch,
          end: offsetPatch + length - 1
        });
        const writeStream = fs.createWriteStream(this.path, {
          flags: "r+",
          start: offsetFile,
        });
        await pipeline(readStream, writeStream);
      }
    }

    await new Promise(resolve => fs.fsync(this.fd, resolve));

    this.patchOffsets.clear();
    this.writePosPatch = 0;
    let stat = fs.statSync(this.path);
    this.writePosFile = stat.size;
    CommitQueue.end(this.commitQueueId);
  }
}