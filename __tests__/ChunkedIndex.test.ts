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
    expect(idx.get(1)).toBe(1);
    idx.reset();
    expect(idx.get(1)).toBeUndefined();
    idx.set(1, 2);
    expect(idx.get(1)).toBe(2);
    await idx.commit();
    expect(idx.get(1)).toBe(2);
  });
});