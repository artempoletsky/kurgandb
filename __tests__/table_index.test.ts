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

describe("Table index", () => {


  type TestWord = {
    id: number;
    word: string;
    level: string;
  }


  let test_words: Table<number, TestWord, any>;

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

    DataBase.init(process.cwd() + "/test_data");

    if (DataBase.doesTableExist("test_words")) {
      DataBase.removeTable("test_words");
    }


    test_words = DataBase.createTable<number, TestWord, any>({
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
  });

  test("buildIndexDataForRecords", () => {

    const data = test_words.buildIndexDataForRecords(testInsert);

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
      test_words.buildIndexDataForRecords([
        { id: 1, word: "a", level: "a", },
        { id: 2, word: "a", level: "a", }
      ]);
    }).toThrowError();

    expect(() => {
      test_words.buildIndexDataForRecords([
        { id: 1, word: "a", level: "a", },
        { id: 1, word: "b", level: "a", }
      ]);
    }).toThrowError();
  });


  test("insert", () => {

    const wordCol = testInsert.map(r => r.word);
    expect(test_words.canInsertUnique("word", wordCol)).toBe(true);

    expect(() => {
      test_words.canInsertUnique("word", wordCol, true)
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
    console.log(a2);

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

  afterAll(async () => {
    await allIsSaved();
    // t.closePartition();
    DataBase.removeTable("test_words");
    await allIsSaved();
  });
});
