import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";

import { standAloneQuery as query } from "../src/client";
import { DataBase } from "../src/db";
import { Table, EventTableOpen, getMetaFilepath, packEventListener } from "../src/table";
import FragmentedDictionary from "../src/fragmented_dictionary";
import { allIsSaved, existsSync } from "../src/virtual_fs";
import { constructFunction, parseFunction } from "../src/function";
import z from "zod";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table_0";

describe("Table events", () => {


  type TestWord = {
    id: number;
    level: string;
  }

  type TestWordInsert = Omit<TestWord, "id">;

  type TestWordsMeta = {
    levelsLen: Record<string, number>
  }

  let test_words: Table<TestWord, number, TestWordsMeta, TestWordInsert, TestWord, TestWord>;
  beforeAll(async () => {
    await allIsSaved();

    DataBase.init(process.cwd() + "/test_data");

    if (DataBase.doesTableExist("test_words")) {
      DataBase.removeTable("test_words");
    }


    test_words = DataBase.createTable<TestWord, number, TestWordsMeta>({
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
      { level: "a1" },
      { level: "a1" },
      { level: "a1" },
      { level: "a1" },
      { level: "a2" },
      { level: "a2" },
      { level: "b1" },
      { level: "c1" },
    ])
  });

  test("empty meta", () => {
    expect(Object.keys(test_words.meta).length).toBe(0);
    const meta: any = test_words.meta;
    meta.foo = 1;
    expect(meta.foo).toBe(1);
    delete meta.foo;
    expect(meta.foo).toBe(undefined);
  })

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


  const tableOpenCallback = ({ table, meta, $ }: EventTableOpen<TestWord, number, TestWordsMeta, TestWordInsert, TestWord, TestWord>) => {
    const keys = table.indexKeys<string>("level");
    meta.levelsLen = $.dictFromKeys(keys, level => table.indexIds("level", level).length);
  };

  test("tableOpen", () => {
    // test_words.where("level", .);

    test_words.registerEventListener("levelsLen", "tableOpen", tableOpenCallback);

    expect(test_words.meta).toHaveProperty("levelsLen");
    expect(test_words.meta.levelsLen.a1).toBe(4);
    expect(test_words.meta.levelsLen.a2).toBe(2);
    expect(test_words.meta.levelsLen.b1).toBe(1);
    expect(test_words.meta.levelsLen.c1).toBe(1);

    let listeners = test_words.getRegisteredEventListeners();

    expect(listeners.levelsLen.tableOpen.body).toBeDefined();

    test_words.unregisterEventListener("levelsLen", "tableOpen");
    listeners = test_words.getRegisteredEventListeners();
    expect(listeners.levelsLen).toBeUndefined();

    test_words.registerEventListener("levelsLen", "tableOpen", tableOpenCallback);
  });


  test("recordsInsert recordsRemove", () => {
    // test_words.where("level", .);

    test_words.registerEventListener("levelsLen", "recordsInsert", ({ records, meta, $ }) => {
      const levels = $.reduceDictionary(records, (levels: Record<string, number>, rec) => {
        levels[rec.level] = (levels[rec.level] || 0) + 1;
      });
      $.aggregateDictionary(meta.levelsLen, levels);
    });

    test_words.registerEventListener("levelsLen", "recordsRemoveLight", ({ records, meta, $ }) => {
      const levels = $.reduceDictionary(records, (levels: Record<string, number>, rec) => {
        levels[rec.level] = (levels[rec.level] || 0) + 1;
      });
      $.aggregateDictionary(meta.levelsLen, levels, true);
    });

    expect(test_words.meta.levelsLen.a1).toBe(4);

    const id = test_words.insert({ level: "a1" });

    expect(test_words.meta.levelsLen.a1).toBe(5);

    test_words.where("id", id).delete();
    expect(test_words.meta.levelsLen.a1).toBe(4);

    test_words.insert({ level: "a1" } as any);
    expect(test_words.meta.levelsLen.a1).toBe(5);

  })

  test("recordsChange", () => {

    test_words.registerEventListener<string>("levelsLen", "recordChange:level", ({ oldValue, newValue, meta }) => {
      meta.levelsLen[oldValue]--;
      meta.levelsLen[newValue]++;
    });

    expect(test_words.meta.levelsLen.a1).toBe(5);
    expect(test_words.meta.levelsLen.a2).toBe(2);
    expect(test_words.hasEventListener("recordChange:level")).toBe(true);

    test_words.where("id", 4).update(rec => {
      rec.level = "a2";
    });

    expect(test_words.meta.levelsLen.a1).toBe(4);
    expect(test_words.meta.levelsLen.a2).toBe(3);
  });

  test("events browsing", async () => {
    const events = await query(({ test_words }: { test_words: Table<TestWord, number, TestWordInsert> }, { }, { }) => {
      return test_words.getRegisteredEventListeners();
    }, {});
    // console.log(events);
    expect(events.levelsLen.tableOpen.args[0]).toBe("{ table, meta, $ }");
    expect(events.levelsLen.recordsRemoveLight.args[0]).toBe("{ records, meta, $ }");
    expect(events.levelsLen.recordsInsert.args[0]).toBe("{ records, meta, $ }");

    // expect(events[])
  });

  test("unsubscribing", () => {
    test_words.unregisterEventListener("levelsLen");

    expect(test_words.indexIds("level", "a1").length).toBe(4);
    expect(test_words.indexIds("level", "a2").length).toBe(3);

    test_words.where("id", 4).update(rec => {
      rec.level = "a1";
    });

    expect(test_words.meta.levelsLen.a1).toBe(4);
    expect(test_words.meta.levelsLen.a2).toBe(3);

    expect(test_words.indexIds("level", "a1").length).toBe(5);
    expect(test_words.indexIds("level", "a2").length).toBe(2);
  });

  test("register parsed", () => {

    const parsed = parseFunction(tableOpenCallback);
    test_words.registerEventListenerParsed("levelsLen", "tableOpen", parsed);

    expect(test_words.meta.levelsLen.a1).toBe(5);
    expect(test_words.meta.levelsLen.a2).toBe(2);
  });

  afterAll(async () => {
    await allIsSaved();
    // t.closePartition();
    DataBase.removeTable("test_words");
    await allIsSaved();
  });
});
