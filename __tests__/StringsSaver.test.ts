import * as fs from "fs";
import * as path from "path";

import { describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";
import FilePatchRecord from "../src/FilePatchRecord";
import { rimraf } from "rimraf";
import StringsSaver from "../src/StringsSaver";


describe("StringsSaver", () => {

  const cwd = "/_stringssavertest/"

  rimraf.sync(process.cwd() + "/kurgandb_data" + cwd);
  fs.mkdirSync(process.cwd() + "/kurgandb_data" + cwd, { recursive: true });

  let page = Buffer.alloc(0x2000);
  let fpr = new FilePatchRecord({
    pathPage: cwd + "page.bin",
    sizePage: 0x2000,
    pathHeap: cwd + "heap.bin",
  });

  fpr.readPage(0, page);

  let s = new StringsSaver({
    bufferPage: page,
    stringsMetaStart: 15,
    stringsTailStart: 150,
    stringsNum: 3,
    fpr,
  });

  let longString = "a".repeat(500);


  test("simple write and read", () => {
    s.readPage();

    expect(s.getString(0)).toBe("");
    expect(s.getString(1)).toBe("");
    expect(s.getString(2)).toBe("");

    s.setString(0, "test");
    s.setString(1, longString);
    s.setString(2, "test3");

    expect(s.getString(0)).toBe("test");
    expect(s.getString(1)).toBe(longString);
    expect(s.getString(2)).toBe("test3");
    s.save();
    fpr.writePage(0, page);
    fpr.commit();

    fpr.readPage(0, page);
    s.readPage();
    expect(s.getString(0)).toBe("test");
    expect(s.getString(1)).toBe(longString);
    expect(s.getString(2)).toBe("test3");

  });

});