import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";

import { standAloneQuery as query } from "../src/client";
import { DataBase } from "../src/db";
import { Table, getMetaFilepath, packEventListener } from "../src/table";
import FragmentedDictionary from "../src/fragmented_dictionary";
import { allIsSaved, existsSync } from "../src/virtual_fs";
import { constructFunction } from "../src/function";
import { rimrafSync } from "rimraf";
import TableUtils from "../src/table_utilities";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "simple";

describe("Table utils", () => {


  type TestWord = {
    id: number;
    word: string;
    level: string;
  }


  let test_words: Table<TestWord, number, any>;
  let utils: TableUtils<TestWord, number>;

  const testInsert: TestWord[] = [
    { id: 2, level: "a1", word: "b" },
    { id: 1, level: "a1", word: "a" },
    { id: 3, level: "a1", word: "c" },
    { id: 4, level: "a1", word: "d" },
    { id: 5, level: "a2", word: "f" },
    { id: 6, level: "a2", word: "g" },
    { id: 7, level: "b1", word: "h" },
    { id: 8, level: "c1", word: "i" },
  ];
  beforeAll(async () => {
    await allIsSaved();

    rimrafSync(process.cwd() + "/test_data");
    DataBase.init(process.cwd() + "/test_data");



    test_words = DataBase.createTable<TestWord, number, any>({
      name: "test_words",
      fields: {
        id: "number",
        word: "string",
        level: "string",
      },
      tags: {
        id: ["primary"],
        word: ["unique"],
        level: ["index"],
      }
    });

    utils = TableUtils.fromTable(test_words);
  });

  test("buildIndexDataForRecords", () => {

    const data = utils.buildIndexDataForRecords(testInsert);

    expect(data.word).toBeDefined();
    expect(data.level).toBeDefined();

    const wordMap = data.word as Map<string, number[]>;

    for (const [word, ids] of wordMap) {
      expect(ids.length).toBe(1);
    }
    expect(wordMap.size).toBe(8);
    expect((<any>wordMap.get("d"))[0]).toBe(4);

    const levelMap = data.level as Map<string, number[]>;

    expect(levelMap.get("a1")?.length).toBe(4);
    expect(levelMap.get("a2")?.length).toBe(2);
    expect(levelMap.get("b1")?.length).toBe(1);
    expect(levelMap.get("c1")?.length).toBe(1);
    expect(levelMap.size).toBe(4);

    expect(() => {
      utils.buildIndexDataForRecords([
        { id: 1, word: "a", level: "a", },
        { id: 2, word: "a", level: "a", }
      ]);
    }).toThrowError();

    expect(() => {
      utils.buildIndexDataForRecords([
        { id: 1, word: "a", level: "a", },
        { id: 1, word: "b", level: "a", }
      ]);
    }).toThrowError();
  });


  test("insert", () => {

    const wordCol = testInsert.map(r => r.word);
    expect(utils.canInsertUnique("word", wordCol)).toBe(true);

    expect(() => {
      utils.canInsertUnique("word", wordCol, true)
    }).not.toThrow();

    test_words.insertMany(testInsert);
    expect(test_words.length).toBe(8);
    expect(test_words.indexIds("level", "a1").length).toBe(4);
    expect(test_words.indexIds("level", "a1", "a2").length).toBe(6);

    const a1IDs = test_words.indexIds("level", "a1");

    expect(a1IDs[0]).toBe(1);
    expect(a1IDs[1]).toBe(2);
    expect(a1IDs[2]).toBe(3);
    expect(a1IDs[3]).toBe(4);

    expect(test_words.indexIds("word", "a")[0]).toBe(1);
  });


  test("delete", () => {

    let a2IndexIds = test_words.indexIds("level", "a2");
    expect(a2IndexIds.length).toBe(2);

    expect(a2IndexIds[0]).toBe(5);
    expect(a2IndexIds[1]).toBe(6);

    let g = test_words.where("word", "g").select()[0];
    expect(g.id).toBe(6);
    expect(g.level).toBe("a2");
    expect(g.word).toBe("g");

    let a2Select = test_words.where("level", "a2").select();
    expect(a2Select[0].id).toBe(5);
    expect(a2Select[1].id).toBe(6);

    expect(a2Select.length).toBe(2);

    g = test_words.where("word", "g").delete()[0];

    expect(g.id).toBe(6);
    expect(g.level).toBe("a2");
    expect(g.word).toBe("g");

    expect(test_words.indexIds("word", "g").length).toBe(0);


    // debugger;
    a2IndexIds = test_words.indexIds("level", "a2");
    expect(a2IndexIds.length).toBe(1);
    expect(a2IndexIds[0]).toBe(5);


    const whereA2Query = test_words.where("level", "a2");

    const [idFilter] = whereA2Query.getQueryFilters();
    expect(idFilter).toBeDefined(); if (!idFilter) return;
    expect(idFilter(5)).toBe(true);

    a2Select = test_words.where("level", "a2").select();

    expect(a2Select.length).toBe(1);

    const a2 = a2Select[0];

    expect(a2.level).toBe("a2");
    expect(a2.id).toBe(5);
    expect(a2.word).toBe("f");

  });

  test("update", () => {

    test_words.where("word", "h").update(rec => {
      rec.word = "g";
    });
    expect(test_words.indexIds("word", "h").length).toBe(0);

    // debugger;


    expect(() => {
      test_words.where("level", "a1").update(rec => {
        rec.word = "z";
      });
    }).toThrow();

    expect(test_words.where("word", "z").select().length).toBe(0);

    expect(test_words.where("level", "a1").select().length).toBe(4);

    test_words.where("level", "a2").update(rec => {
      rec.level = "a1";
    });

    expect(test_words.where("level", "a2").select().length).toBe(0);
    expect(test_words.where("level", "a1").select().length).toBe(5);

  });

  type SimpleType = {
    id: number;
    date: Date | string | number;
    bool: boolean;
    name: string;
    light: string[];
    heavy: null | {
      bar: number
    };
  };

  test("indices", async () => {

    const t = DataBase.createTable<SimpleType, number>({
      name: "simple",
      fields: {
        id: "number",
        bool: "boolean",
        date: "date",
        name: "string",
        light: "json",
        heavy: "json",
      },
      tags: {
        id: ["primary", "autoinc"],
        name: ["index"],
      }
    });

    const utils = TableUtils.fromTable(t);


    for (let i = 0; i < 10; i++) {
      t.insert({
        date: new Date(),
        bool: false,
        name: "foo",
        light: ["1", "2", "3"],
        heavy: {
          bar: Math.random()
        }
      });
    }

    expect(utils.fieldHasAnyTag("name", "index")).toBe(true);
    let indexDict = FragmentedDictionary.open<string, number[]>(utils.getIndexDictDir("name"));


    indexDict.setOne("blablabla", [123]);
    const arr = indexDict.getOne("blablabla");
    expect(arr).toBeDefined();
    if (!arr) return;
    expect(arr[0]).toBe(123);
    indexDict.remove("blablabla");
    expect(indexDict.getOne("blablabla")).toBe(undefined);


    utils.storeIndexValue("name", 456, "foo123");
    indexDict = FragmentedDictionary.open<string, number[]>(utils.getIndexDictDir("name"));
    expect(indexDict.getOne("foo123")).toBeDefined();
    indexDict.remove("foo123");

    const foos = indexDict.getOne("foo");
    expect(foos).toBeDefined();
    if (!foos) return;
    expect(foos[0]).toBe(1);


    t.insertMany([{
      bool: true,
      date: new Date(),
      light: ["1", "2", "3"],
      heavy: null,
      name: "bar",
    }, {
      bool: true,
      date: new Date(),
      light: ["1", "2", "3"],
      heavy: null,
      name: "bar",
    }]);

    const bars = t.where("name", "bar").select();

    expect(bars.length).toBe(2);
  });



  test("removes and creates index", async () => {
    const t = DataBase.getTable<SimpleType, number>("simple");
    const utils = TableUtils.fromTable(t);
    t.removeIndex("name");
    expect(utils.fieldHasAnyTag("name", "index", "unique")).toBe(false);

    let bars = t.where("name", "bar").select();

    expect(bars.length).toBe(2);

    expect(() => {
      t.createIndex("name", true);
    }).toThrow(`Unique value 'foo' for field '${TestTableName}[id].name' already exists`);

    expect(utils.fieldHasAnyTag("name", "index", "unique")).toBe(false);

    t.createIndex("name", false);
    expect(utils.fieldHasAnyTag("name", "index")).toBe(true);

    bars = t.where("name", "bar").select();

    expect(bars.length).toBe(2);

    const lastBarID = t.insert({
      ...t.getRecordDraft(),
      name: "bar"
    });

    bars = t.where("name", "bar").select();
    expect(bars.length).toBe(3);

    t.where("id", lastBarID).delete();

    bars = t.where("name", "bar").select();
    expect(bars.length).toBe(2);

    const indexDict = FragmentedDictionary.open<string, number[]>(utils.getIndexDictDir("name"));

    const arr = indexDict.getOne("bar");
    expect(arr).toBeDefined();
    if (!arr) return;
    expect(arr.length).toBe(2);
    expect(arr.indexOf(lastBarID)).toBe(-1);
  });

  afterAll(async () => {
    await allIsSaved();
    // t.closePartition();
    // if (DataBase.doesTableExist("test_words"))
      DataBase.removeTable("test_words");
    // if (DataBase.doesTableExist("simple"))
      DataBase.removeTable("simple");
    // await allIsSaved();
  });
});
