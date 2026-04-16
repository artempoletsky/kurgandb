import fs from "fs";
import CommitQueue from "./CommitQueue";
import PatchFile from "./PatchFile";

type HeapDumbRecord = {
  length: number;
  maxLenght: number;
  position: number;
};

type Hole = {
  start: number;
  end: number;
}

export default class HeapDumb {
  static dataFile: PatchFile;
  static holes: PatchFile;
  static init(path: string) {
    this.dataFile = new PatchFile(path);
    this.holes = new PatchFile(path + ".holes");
  }

  static reset() {
    this.dataFile.reset();
  }

  static async commit() {
    await this.dataFile.commit();
    await this.holes.commit();
  }

  static findHole(length: number): Hole | null {
    return {
      start: 0,
      end: length,
    };
    return null;
  }

  static shrinkHole(hole: Hole, dataLength: number) {

  }

  static create(maxLength: number, data: Buffer): HeapDumbRecord {
    let hole = this.findHole(maxLength);
    if (!hole) {
      this.dataFile.writeEnd(data);
    } else {
      this.shrinkHole(hole, data.byteLength);
      this.dataFile.write(hole.start, data);
    }
  }

  static read(position: number, length: number): Buffer {
    return this.dataFile.read(position, length);
  }

  static free(position: number, length: number) {

  }

  static update(position: number, data: Buffer): HeapDumbRecord {
    
  }


}

