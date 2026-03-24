import * as fs from "fs";
import * as path from "path";
import { IndexOneNumber } from "../src/IndexOneNumber";

import { describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";


describe("Index", () => {
  const numbersPath = path.join(__dirname, "numbers");


  afterAll(() => {
    // try { fs.unlinkSync(numbersPath); } catch {}
  });

  test("setOffset getOffset", () => {
    const idx = new IndexOneNumber(numbersPath);
    // for (let i = 0; i < 10 * 1000 * 1000; i++) {
    //   idx.setOffset(i, i * 10);
    // }

    let rawData = Array.from({ length: 10_000_000 }, (_, i) => {
      return { key: i, offset: i * 10 };
    });

    rawData.splice(1234, 1);

    // rawData = _.shuffle(rawData);

    // console.time("sort");
    // rawData = rawData.sort((a, b) => a.key - b.key);
    // console.timeEnd("sort");


    // console.time("insertPredicate");
    // idx.fastFill((i) => {
    //   const buf = Buffer.allocUnsafe(8);
    //   buf.writeInt32BE(i, 0);
    //   buf.writeInt32BE(i * 10, 4);
    //   return buf;
    // }, 10 * 1000 * 1000, 15 * 1000 * 1000);
    // console.timeEnd("insertPredicate");


    console.time("insert");
    idx.fastFill(rawData, 15_000_000 * 16);
    console.timeEnd("insert");

    console.time("getExistent");
    expect(idx.get(123)).toBe(1230);
    console.timeEnd("getExistent");


    console.time("getNonExistent");
    expect(idx.get(1234)).toBe(-1);
    console.timeEnd("getNonExistent");

    console.time("deleteNonExistent");
    idx.delete(1234);
    console.timeEnd("deleteNonExistent");


    console.time("setWithShift");
    idx.set(1234, 1230);
    console.timeEnd("setWithShift");


    console.time("rewriteSet");
    idx.set(1234, 12300);
    console.timeEnd("rewriteSet");

    console.time("deleteExistent");
    idx.delete(1234);
    console.timeEnd("deleteExistent");

    console.time("getWithTombstone");
    expect(idx.get(1234)).toBe(-1);
    console.timeEnd("getWithTombstone");

    console.time("compact");
    idx.compact();
    console.timeEnd("compact");

    idx.save();
  });

});
