import fs from "fs";
import path from "path";
import LogicalMemoryHeap from "../src/LogicalMemoryHeap";
import { describe, expect, test, beforeAll, afterAll } from "./test-setup";
import { he } from "@faker-js/faker";

describe("LogicalMemoryHeap", () => {
  const heapPath = path.join(__dirname, "heap.bin");

  function removeTestData() {
    try { fs.unlinkSync(heapPath); } catch { }
    // try { fs.unlinkSync(heapPath + ".txt"); } catch { }
  }

  beforeAll(removeTestData);
  // afterAll(removeTestData);

  test("simple commit reset", () => {
    const heap = new LogicalMemoryHeap(heapPath);

    let id = heap.addString("initial data");
    expect(id).toBe(1);
    expect(heap.readString(id)).toEqual("initial data");
    expect(heap.__debug.lastId).toBe(1);
    expect(heap.__debug.currentWritePos).toBe(Buffer.byteLength("initial data", "utf-8"));
    let header = heap.serializeHeader();
    expect(header.readUint32LE(0)).toBe(12 + 16);

    heap.reset();
    expect(heap.readString(id)).toBeUndefined();

    id = heap.addString("new data");
    expect(id).toBe(1);
    expect(heap.readString(id)).toEqual("new data");

    header = heap.serializeHeader();
    expect(header.readUint32LE(0)).toBe(12 + 16);
    expect(header.readDoubleLE(12)).toBe(1);
    expect(header.readUint32LE(12 + 8)).toBe(0);
    expect(header.readUint32LE(12 + 12)).toBe(8);

    heap.readHeader(header);
    expect(heap.__debug.lastId).toBe(1);
    expect(heap.__debug.indexMap.get(1)).toEqual(new Uint32Array([0, 8]));
    heap.commit();

    let b = Buffer.allocUnsafe(1024);
    fs.readSync(fs.openSync(heapPath, "r"), b);
    expect(b.readUint32LE(0)).toBe(12 + 16);
    header = heap.serializeHeader();
    expect(header.readUint32LE(0)).toBe(12 + 16);
    expect(heap.__debug.indexMap.get(1)).toEqual(new Uint32Array([0, 8]));
    expect(heap.readString(id)).toEqual("new data");

  });

});