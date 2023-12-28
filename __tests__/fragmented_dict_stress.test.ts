import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog, rfs, existsSync, wfs, rmie, perfEndLog } from "../src/utils";
import FragmentedDictionary, { FragDictMeta, PartitionMeta } from "../src/fragmented_dictionary";
import SortedDictionary from "../src/sorted_dictionary";
import vfs, { allIsSaved } from "../src/virtual_fs";
import { rimraf } from "rimraf";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };




describe("Fragmented dictionary", () => {
  let numbers: FragmentedDictionary<number, number>;
  const NUM = "/data/jest_dict_numbers_stress/";
  const partitionLength = 100 * 1000;

  const portion = Array.from(Array(partitionLength)).map((und, index) => index);

  const RESET = true;
  beforeAll(() => {
    if (RESET) {
      rmie(NUM);
      numbers = FragmentedDictionary.init({
        directory: NUM,
        keyType: "int",
        maxPartitionLenght: partitionLength,
      });
    } else {
      numbers = FragmentedDictionary.open(NUM);
    }

    
  });

  test("created", () => {
    if (RESET) {
      expect(numbers.length).toBe(0);
    }

    expect(portion.length).toBe(partitionLength);
    expect(portion[0]).toBe(0);
    expect(portion[partitionLength - 1]).toBe(partitionLength - 1);
  });


  test("Inserts millions of records", async () => {
    const insertTimes = 1 * 1;
    for (let i = 0; i < insertTimes; i++) {
      numbers.insertArray(portion);
      if (i % 5 == 0) {
        await allIsSaved();
      }
    }
    // expect(numbers.length).toBe(insertTimes * partitionLength);
    await allIsSaved();

  }, 60000);

  test("reads from a big dict", () => {
    
    perfStart("getOne");
    expect(numbers.getOne(99999)).toBeDefined();
    perfEndLog("getOne");
    expect(perfDur("getOne")).toBeLessThan(100);
  });


  afterAll(async () => {
    await allIsSaved();
    rmie(NUM);
  })
});