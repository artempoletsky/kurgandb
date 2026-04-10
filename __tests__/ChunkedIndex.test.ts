import ChunkedIndex from "../src/ChunkedIndex";
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";

import fs from "fs";
import path from "path";


describe("ChunkedIndex", () => {

  let index: ChunkedIndex;

  const indexPath = path.join(process.cwd(), "kurgandb_data", "index.bin");

  function removeTestData() {
    try { fs.unlinkSync(indexPath); } catch { }
    // try { fs.unlinkSync(heapPath + ".txt"); } catch { }
  }

  beforeAll(removeTestData);
  // afterAll(removeTestData);

  xtest("writeChunkMeta, findChunkIndex", () => {
    let idx = new ChunkedIndex(indexPath);

    idx.writeChunkMeta({
      length: 0,
      min: 1,
      max: 1,
      page: 0,
    }, 0);
    let { chunkIndex, chunkMeta } = idx.findChunkIndex(1);

    expect(chunkIndex).toBe(0);
    expect(chunkMeta).toEqual({
      length: 0,
      min: 1,
      max: 1,
      page: 0,
    });

  });

  test("commit reset", async () => {
    let idx = new ChunkedIndex(indexPath);
    expect(idx.numberOfRecords).toBe(0);
    expect(idx.numberOfChunks).toBe(0);
    idx.set(1, 1);
    expect(idx.numberOfRecords).toBe(1);
    expect(idx.numberOfChunks).toBe(1);

    expect(idx.maxValue).toBe(1);
    expect(idx.minValue).toBe(1);
    expect(idx.get(1)).toBe(1);
    idx.reset();
    expect(idx.get(1)).toBeUndefined();
    expect(idx.numberOfRecords).toBe(0);

    idx.set(2, 1234);

    expect(idx.minValue).toBe(2);
    expect(idx.maxValue).toBe(2);

    let { chunkIndex, chunkMeta } = idx.findChunkIndex(2);
    expect(chunkIndex).toBe(0);
    expect(chunkMeta).toEqual({
      length: 1,
      min: 2,
      max: 2,
      page: 0,
    });

    expect(idx.get(2)).toBe(1234);
    let meta = idx.readChunkMeta(0);
    expect(meta).toEqual({
      length: 1,
      min: 2,
      max: 2,
      page: 0,
    });

    let page = idx.readPage(0);
    expect(page.readDoubleLE(0)).toBe(2);
    expect(page.readDoubleLE(8)).toBe(1234);

    expect(idx.header.readUInt16LE(0)).toBe(1); // number of chunks
    expect(idx.header.readUInt16LE(2)).toBe(1); // number of records
    expect(idx.header.readUInt16LE(20)).toBe(1); // number of records in chunk 0
    expect(idx.header.readDoubleLE(22)).toBe(2); // min value in chunk 0
    expect(idx.header.readDoubleLE(30)).toBe(2); // max value in chunk 0
    expect(idx.header.readUInt16LE(38)).toBe(0); // page number for chunk 0

    await idx.commit();

    let b = Buffer.allocUnsafe(idx.sizeHeader);
    let fd = fs.openSync(indexPath, "r");
    fs.readSync(fd, b, 0, idx.sizeHeader, 0);

    fs.closeSync(fd);

    expect(b.readUInt16LE(0)).toBe(1); // number of chunks
    expect(b.readUInt16LE(2)).toBe(1); // number of records
    expect(b.readUInt16LE(20)).toBe(1); // number of records in chunk 0
    expect(b.readDoubleLE(22)).toBe(2); // min value in chunk 0
    expect(b.readDoubleLE(30)).toBe(2); // max value in chunk 0
    expect(b.readUInt16LE(38)).toBe(0); // page number for chunk 0

    let p2 = Buffer.allocUnsafe(0x2000);
    fd = fs.openSync(indexPath, "r");
    fs.readSync(fd, p2, 0, 0x2000, idx.sizeHeader);

    expect(p2.readDoubleLE(0)).toBe(2);
    expect(p2.readDoubleLE(8)).toBe(1234);

    meta = idx.readChunkMeta(0);
    expect(meta).toEqual({
      length: 1,
      min: 2,
      max: 2,
      page: 0,
    });
    page = idx.readPage(0);

    expect(page.readDoubleLE(0)).toBe(2);
    expect(page.readDoubleLE(8)).toBe(1234);

    expect(idx.get(2)).toBe(1234);
  });
});