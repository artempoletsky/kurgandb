import * as fs from "fs";
import * as path from "path";
import { IndexOneString } from "../src/IndexOneString";

import { describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";
import { text } from "stream/consumers";


describe("IndexOneString", () => {
  const idxPath = path.join(__dirname, "strings");

  function removeTestData() {
    try { fs.unlinkSync(idxPath); } catch { }
    try { fs.unlinkSync(idxPath + ".txt"); } catch { }
  }

  beforeAll(removeTestData);
  afterAll(removeTestData);

  test("compareStringBuffer", () => {
    const str1 = "hello";

    const buffer = Buffer.allocUnsafe(20);
    buffer.write(str1, 4, "utf-8");
    expect(IndexOneString.compareStringBuffer(str1, buffer, 4, Buffer.byteLength(str1, "utf-8"))).toBe(0);
  });

  test("set get", () => {
    const idx = new IndexOneString(idxPath);
    expect(idx.get("hello")).toBe(-1);
    idx.set("hello", 123);

    let variableBuffer = idx.getVariableBuffer();
    expect(variableBuffer.length).not.toBe(0);
    let idPos = variableBuffer.indexOf("hello");
    // console.log(variableBuffer.toString("utf-8", 0, variableBuffer.length));

    expect(idPos).toBe(0);
    expect(IndexOneString.compareStringBuffer("hello", variableBuffer, idPos, Buffer.byteLength("hello", "utf-8"))).toBe(0);


    expect(idx.getFixedBufferLength()).toBe(12);
    expect(idx.getVariableBufferLength()).toBe(16);

    let search = idx.binarySearchString("hello");

    expect(search.idPos).toBe(0);
    expect(search.pos).toBe(0);
    expect(search.found).toBe(true);


    expect(idx.get("hello")).toBe(123);

    idx.reset();
  });

  test("delete", () => {
    const idx = new IndexOneString(idxPath);
    idx.set("hello1", 1234);
    expect(idx.get("hello1")).toBe(1234);
    idx.delete("hello1");
    expect(idx.get("hello1")).toBe(-1);
    idx.reset();
  });


  test("compact", () => {
    const idx = new IndexOneString(idxPath);
    idx.set("hello", 123);

    let rec = idx.readFixedRecord(0);

    expect(rec.offset).toBe(123);
    expect(rec.idLen).toBe(5);
    expect(rec.idMaxLen).toBe(16);
    expect(rec.idPos).toBe(0);

    idx.compact();


    let fixedBuffer = idx.getFixedBuffer();
    expect(fixedBuffer.length).not.toBe(0);
    expect(fixedBuffer.readUInt32BE(0)).toBe(123);


    expect(idx.getVariableBufferLength()).toBe(16);
    expect(idx.getFixedBufferLength()).toBe(12);

    rec = idx.readFixedRecord(0);

    expect(rec.offset).toBe(123);
    expect(rec.idLen).toBe(5);
    expect(rec.idMaxLen).toBe(16);
    expect(rec.idPos).toBe(0);
  });

  test("save", () => {
    const idx = new IndexOneString(idxPath);
    idx.set("hello", 123);

    expect(idx.getVariableBufferLength()).toBe(16);

    idx.save();

    const savedBuffer = fs.readFileSync(idxPath);
    expect(savedBuffer.length).toBe(12);
    const variableBuffer = fs.readFileSync(idxPath + ".txt", "utf-8");
    expect(variableBuffer.length).toBe(16);
  });


  function createArray(length: number) {
    let rawData = Array.from({ length }, (_, i) => {
      return { key: String(i), offset: i * 10 };
    });
    rawData = rawData.sort((a, b) => a.key.localeCompare(b.key));
    return rawData
  }

  test("fast fill", () => {
    removeTestData();
    const idx = new IndexOneString(idxPath);

    let rawData = createArray(2);

    idx.fastFill(rawData, 5);

    // expect(idx.getFixedBufferLength()).toBe(24);
    // expect(idx.getVariableBufferLength()).toBe(28);

    let rec1 = idx.readFixedRecord(0);
    expect(rec1.idLen).toBe(1);
    expect(rec1.idMaxLen).toBe(16);
    expect(rec1.offset).toBe(0);
    expect(rec1.idPos).toBe(0);
    idx.save();
  });

});
