import PagesManager from "../src/PagesManager";
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";

import fs from "fs";
import path from "path";


describe("PagesManager", () => {

  

  const pagesPath = path.join(process.cwd(), "kurgandb_data", "pages.bin");

  function removeTestData() {
    try { fs.unlinkSync(pagesPath); } catch { }
    // try { fs.unlinkSync(heapPath + ".txt"); } catch { }
  }

  beforeAll(removeTestData);
  afterAll(removeTestData);


  test("getWritingPage", async () => {
    const idx = new PagesManager({
      path: pagesPath,
    });
    let p = idx.getWritingPage(0);
    p.writeDoubleLE(123, 32);

    p = idx.getWritingPage(1);
    expect(p.readDoubleLE(32)).not.toBe(123);
    p.writeDoubleLE(321, 32);

    p = idx.readPage(0);
    expect(p.readDoubleLE(32)).toBe(123);
    p = idx.readPage(1);
    expect(p.readDoubleLE(32)).toBe(321);

    expect(idx.__debug.memoryPatch?.readDoubleLE(32)).toBe(123);
    expect(idx.__debug.writingPages.get(1)?.readDoubleLE(32)).toBe(321);

    idx.__debug.writePage(1, idx.__debug.writingPages.get(1)!);
    idx.__debug.writingPages.clear();

    expect(idx.__debug.memoryPatch?.readDoubleLE(32 + 0x2000)).toBe(321);
    // expect(idx.__debug.memoryPatch?.readDoubleLE(0x2000 + 32)).toBe(321);

    let b = Buffer.alloc(0x2000);
    await idx.readPatchAsync(b, 0);
    expect(b.readDoubleLE(32)).toBe(123);
    await idx.readPatchAsync(b, 0x2000);
    expect(b.readDoubleLE(32)).toBe(321);

    await idx.commit();

    let fd = fs.openSync(pagesPath, "r");
    
    fs.readSync(fd, b, 0, b.byteLength, 0);
    expect(b.readDoubleLE(32)).toBe(123);

    fs.readSync(fd, b, 0, b.byteLength, 0 + 0x2000);
    expect(b.readDoubleLE(32)).toBe(321);
    
    fs.closeSync(fd);

    p = idx.readPage(0);
    expect(p.readDoubleLE(32)).toBe(123);
    p = idx.readPage(1);
    expect(p.readDoubleLE(32)).toBe(321);
  });

});