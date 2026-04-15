import fs from "fs";
import CommitQueue from "./CommitQueue";

type HeapDumbRecord = {
    length: number;
    maxLenght: number;
    position: number;
};

export default class HeapDumb {
    static pathPatch: string;
    static fdPatch: number;
    static init(path): {

    }

    static create(maxLength: number, data: Buffer): HeapDumbRecord {

    }

    static read(position: number, length: number): Buffer {

    }

    static delete(position: number) {

    }

    static update(position: number, data: Buffer): HeapDumbRecord {

    }

    static commit() {
        fs.readSync(this.fdPatch)
    }

}

