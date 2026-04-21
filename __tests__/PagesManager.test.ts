import CommitQueue from "../src/CommitQueue";
import PagesManager, { SEMATARY } from "../src/PagesManager";
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";

import fs, { fsync } from "fs";
import path from "path";


describe("PagesManager", () => {



  const pagesPath = path.join(process.cwd(), "kurgandb_data", "test_pages.bin");

  function removeTestData() {
    try { fs.unlinkSync(pagesPath); } catch { }
    // try { fs.unlinkSync(heapPath + ".txt"); } catch { }
  }

  beforeAll(removeTestData);
  // afterAll(removeTestData);

  test("remove page trivial", async () => {
    // await CommitQueue.ready();
    removeTestData();
    let p = new PagesManager({
      path: pagesPath,
    });
    let sem = SEMATARY;
    sem.pagesManager = p;
    
    expect(p.getFreePageId()).toBe(1);
    expect(p.getFreePageId()).toBe(2);

    expect(sem.read(0).sb.lastPage).toBe(2);

    expect(sem.pagesManager.path).toBe(pagesPath);
    await p.commit();
    expect(sem.pagesManager.path).toBe(pagesPath);
    p.reset();
    expect(sem.pagesManager.path).toBe(pagesPath);

    let fd =fs.openSync(pagesPath, "r"); 
    let p0Raw = Buffer.alloc(0x2000);

    fs.readSync(fd, p0Raw, 0, 0x2000, 0);
    expect(p0Raw.readUint32LE(0x2000 - 12)).toBe(2);

    p.readPage(p0Raw, 0);
    expect(p0Raw.readUint32LE(0x2000 - 12)).toBe(2);

    
    expect(sem.read(0).sb.lastPage).toBe(2);
    let stat = fs.statSync(pagesPath);

    expect(stat.size).toBe(0x2000 * 3);


    expect(p.getFreePageId()).toBe(3);
    p.deletePage(2);
    await p.commit();
    p.reset();

    expect(p.getFreePageId()).toBe(2);

    p.deletePage(2);
    await p.commit();

    expect(p.getFreePageId()).toBe(2);

    await p.commit();

    expect(p.getFreePageId()).toBe(4);

    p.deletePage(2);
    p.deletePage(3);
    p.deletePage(4);
    p.deletePage(1);

    expect(p.getFreePageId()).toBe(1);

    expect(() => p.deletePage(2)).toThrow();
    expect(() => p.deletePage(3)).toThrow();
    expect(() => p.deletePage(4)).toThrow();
    expect(() => p.deletePage(1)).not.toThrow();
    expect(() => p.deletePage(1)).toThrow();
    expect(() => p.deletePage(0)).toThrow();

    await p.commit();


    stat = fs.statSync(pagesPath);
    expect(stat.size).toBe(0x2000 * 5);
    // expect(() => p.deletePage(123)).toThrow();

    expect(SEMATARY.read(0).sb.buriedHere).toBe(4);


  });

});