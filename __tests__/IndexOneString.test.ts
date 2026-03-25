import * as fs from "fs";
import * as path from "path";
import { IndexOneString } from "../src/IndexOneString";

import { describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";
import { text } from "stream/consumers";


describe("IndexOneString", () => {
  const idxPath = path.join(__dirname, "strings");


  beforeAll(() => {
    try { fs.unlinkSync(idxPath); } catch { }
    try { fs.unlinkSync(idxPath + ".txt"); } catch { }
  });
  afterAll(() => {
    try { fs.unlinkSync(idxPath); } catch { }
    try { fs.unlinkSync(idxPath + ".txt"); } catch { }
  });

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


  xtest("setOffset getOffset (strings)", () => {
    const idx = new IndexOneString(idxPath);

    let rawData = Array.from({ length: 1_000_000 }, (_, i) => {
      return { key: String(i), offset: i * 10 };
    });

    rawData.splice(1234, 1);



    console.time("insert");
    idx.fastFill(rawData, 10);
    console.timeEnd("insert");

    expect(rawData.find((d) => d.key === "123")?.offset).toBe(1230);

    console.time("getExistent");
    expect(idx.get("123")).toBe(1230);
    console.timeEnd("getExistent");


    console.time("getNonExistent");
    expect(idx.get("1234")).toBe(-1);
    console.timeEnd("getNonExistent");

    console.time("deleteNonExistent");
    idx.delete("1234");
    console.timeEnd("deleteNonExistent");


    console.time("setWithShift");
    idx.set("1234", 1230);
    console.timeEnd("setWithShift");


    console.time("rewriteSet");
    idx.set("1234", 12300);
    console.timeEnd("rewriteSet");


    console.time("deleteExistent");
    idx.delete("1234");
    console.timeEnd("deleteExistent");


    console.time("getWithTombstone");
    expect(idx.get("1234")).toBe(-1);
    console.timeEnd("getWithTombstone");


    console.time("compact");
    idx.compact();
    console.timeEnd("compact");

    idx.save();
  });

});
