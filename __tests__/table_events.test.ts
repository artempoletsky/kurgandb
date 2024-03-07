import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";

import { standAloneQuery as query } from "../src/client";
import { DataBase } from "../src/db";
import { Table, getMetaFilepath, packEventListener } from "../src/table";
import FragmentedDictionary from "../src/fragmented_dictionary";
import { allIsSaved, existsSync } from "../src/virtual_fs";
import { constructFunction } from "../src/function";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table_0";

describe("Table events", () => {


  type TestWord = {
    id: number;
    level: string;
  }
  type TestWordsMeta = {
    levelsLen: Record<string, number>
  }

  let test_words: Table<string, TestWord, TestWordsMeta>;
  beforeAll(async () => {
    await allIsSaved();

    DataBase.init(process.cwd() + "/test_data");

    if (DataBase.doesTableExist("test_words")) {
      DataBase.removeTable("test_words");
    }


    test_words = DataBase.createTable<string, TestWord, TestWordsMeta>({
      name: "test_words",
      fields: {
        id: "number",
        level: "string",
      },
      tags: {
        id: ["primary", "autoinc"],
        level: ["index"],
      }
    });


    test_words.insertMany([
      { level: "a1", },
      { level: "a1", },
      { level: "a1", },
      { level: "a1", },
      { level: "a2", },
      { level: "a2", },
      { level: "b1", },
      { level: "c1", },
    ] as any)
  });

  test("packEventListener", () => {

    let packed = packEventListener(({ table, meta }, { $, _, db }) => {
      meta.test = "123";
    });

    // expect(packed[0]).toBe("{ table, meta }");

    let unpacked = constructFunction(packed);
    // console.log(unpacked(123));

    expect(unpacked).toThrowError();
    const arg: any = { meta: {} };

    unpacked(arg, {});
    expect(arg.meta.test).toBe("123");
  });

  test("tableOpen", () => {
    // test_words.where("level", .);


    test_words.registerEventListener("levelsLen", "tableOpen", ({ table, meta, $ }) => {
      const keys = table.indexKeys<string>("level");
      meta.levelsLen = $.dictFromKeys(keys, level => table.indexIds("level", level).length);
    });

    expect(test_words.meta).toHaveProperty("levelsLen");
    expect(test_words.meta.levelsLen.a1).toBe(4);
    expect(test_words.meta.levelsLen.a2).toBe(2);
    expect(test_words.meta.levelsLen.b1).toBe(1);
    expect(test_words.meta.levelsLen.c1).toBe(1);
  });


  test("recordsInsert recordsRemove", () => {
    // test_words.where("level", .);

    test_words.registerEventListener("levelsLen", "recordsInsert", ({ records, meta, $ }) => {
      const levels = $.reduceDictionary(records, (levels: Record<string, number>, rec) => {
        levels[rec.level] = (levels[rec.level] || 0) + 1;
      });
      $.aggregateDictionary(meta.levelsLen, levels);
    });

    test_words.registerEventListener("levelsLen", "recordsRemove", ({ records, meta, $ }) => {
      const levels = $.reduceDictionary(records, (levels: Record<string, number>, rec) => {
        levels[rec.level] = (levels[rec.level] || 0) + 1;
      });
      $.aggregateDictionary(meta.levelsLen, levels, true);
    });

    const id = test_words.insert({ level: "a1" } as any);

    expect(test_words.meta.levelsLen.a1).toBe(5);

    test_words.where("id", id).delete();
    expect(test_words.meta.levelsLen.a1).toBe(4);

    test_words.insert({ level: "a1" } as any);
    expect(test_words.meta.levelsLen.a1).toBe(5);

  })

  test("recordsChange", () => {

    test_words.registerEventListener("levelsLen", "recordsChange", ({ field, oldValue, newValue, meta }) => {
      if (field == "level") {
        meta.levelsLen[oldValue]--;
        meta.levelsLen[newValue] = meta.levelsLen[newValue] ? meta.levelsLen[newValue] + 1 : 1;
      }
    });

    test_words.where("id", "car").update(doc => {
      doc.level = "a2";
    });

    expect(test_words.meta.levelsLen.a1).toBe(4);
    expect(test_words.meta.levelsLen.a2).toBe(1);
  });

  afterAll(async () => {
    await allIsSaved();
    // t.closePartition();
    DataBase.removeTable("test_words");
    await allIsSaved();
  });
});
