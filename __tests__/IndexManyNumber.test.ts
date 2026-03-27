import * as fs from "fs";
import * as path from "path";
import { IndexManyNumber } from "../src/IndexManyNumber";

import { describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";


describe("IndexManyNumber", () => {
  const idxPath = path.join(__dirname, "many_numbers");

  function removeTestData() {
    try { fs.unlinkSync(idxPath); } catch {}
    try { fs.unlinkSync(idxPath + "_var"); } catch {}
  }

  beforeAll(removeTestData);
  // afterAll(removeTestData);

  xtest("setArray get", () => {
    const idx = new IndexManyNumber(idxPath);
    expect(idx.get(42)).toEqual([]);
    idx.setArray(42, new Uint32Array([100, 200]));

    expect(idx.getFixedBufferLength()).toBe(10);
    expect(idx.getVariableBufferLength()).toBe(8);

    let rec = idx.readFixedRecord(0);

    expect(rec.len).toBe(2);
    expect(rec.value).toBe(42);
    expect(rec.start).toBe(0);

    let arr = idx.readOffsetsAtPositionInFixedBuffer(0);

    expect(arr).toEqual(new Uint32Array([100, 200]));


    expect(idx.get(42)).toEqual(new Uint32Array([100, 200]));

    
    idx.reset();
  });

  xtest("delete", () => {
    const idx = new IndexManyNumber(idxPath);
    idx.setArray(7, new Uint32Array([700]));
    expect(idx.get(7)).toEqual(new Uint32Array([700]));
    idx.delete(7);
    expect(idx.get(7)).toEqual(new Uint32Array([]));
    idx.reset();
  });


  xtest("compact", () => {
    const idx = new IndexManyNumber(idxPath);
    idx.setArray(5, new Uint32Array([123]));

    let rec = idx.readFixedRecord(0);

    expect(rec.len).toBe(1);

    idx.compact();

    let fixedBuffer = idx.getFixedBuffer();
    expect(fixedBuffer.length).not.toBe(0);

    expect(idx.getVariableBufferLength()).toBe(4);
    expect(idx.getFixedBufferLength()).toBe(10);

    rec = idx.readFixedRecord(0);

    expect(rec.len).toBe(1);
  });

  xtest("save", () => {
    const idx = new IndexManyNumber(idxPath);
    idx.setArray(1, [11]);

    expect(idx.getVariableBufferLength()).toBe(4);

    idx.save();

    const savedBuffer = fs.readFileSync(idxPath);
    expect(savedBuffer.length).toBe(10);
    const variableBuffer = fs.readFileSync(idxPath + "_var");
    expect(variableBuffer.length).toBe(4);
  });


  function createArray(length: number) {
    let rawData = Array.from({ length }, (_, i) => {
      return { key: i, offsets: [i * 10] };
    });
    rawData = rawData.sort((a, b) => a.key - b.key);
    return rawData
  }

  xtest("fast fill", () => {
    removeTestData();
    const idx = new IndexManyNumber(idxPath);

    let rawData = createArray(2000);

    idx.fastFill(rawData, 5);

    let rec1 = idx.readFixedRecord(0);
    expect(rec1.len).toBe(1);
    expect(rec1.start).toBe(0);
    idx.save();
  });

});
