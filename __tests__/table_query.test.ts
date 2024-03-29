import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { perfEnd, perfStart, perfDur, perfLog, rfs } from "../src/utils";
// import { star as query } from "../src/api";
import { standAloneQuery as query } from "../src/client";
import { faker } from "@faker-js/faker";
import { DataBase } from "../src/db";
import { Table, getMetaFilepath } from "../src/table";
import FragmentedDictionary from "../src/fragmented_dictionary";
import { allIsSaved, existsSync } from "../src/virtual_fs";

import { TRecord } from "../src/record";
import { rimraf } from "rimraf";
import TableUtils from "../src/table_utilities";
import { PlainObject } from "../src/globals";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table_0";

describe("Table", () => {

  type SimpleType = {
    id: number,
    date: Date,
    bool: boolean,
    name: string,
  }
  type SimpleTypeInsert = Omit<SimpleType, "id">;
  let t: Table<SimpleType, number, any, SimpleTypeInsert>;
  beforeAll(() => {
    // DataBase.createTable({
    //   name: "posts",
    //   "fields": {
    //     "userid": "number",
    //     "title": "string",
    //     "text": "Text",
    //     "date": "date"
    //   },
    //   "settings": {}
    // });

    DataBase.init(process.cwd() + "/test_data");

    if (DataBase.doesTableExist(TestTableName)) {
      DataBase.removeTable(TestTableName);
    }
    t = DataBase.createTable<SimpleType, number>({
      name: TestTableName,
      fields: {
        id: "number",
        bool: "boolean",
        date: "date",
        name: "string",
      },
      tags: {
        id: ["primary", "autoinc"],
      }
    });

    const items: SimpleTypeInsert[] = [];
    for (let i = 0; i < 5; i++) {
      items.push({
        bool: false,
        date: new Date(),
        name: i % 3 ? "bar" : "foo",
      });
    }
    t.insertMany(items);
  });

  test("where", () => {
    const oneThree = t.where<number>("id", 1, 3).select();
    expect(oneThree[0].id).toBe(1);
    expect(oneThree[1].id).toBe(3);

    const notBars = t.where<string>("name", name => name != "bar").select();
    expect(notBars[0].name).toBe("foo");

    const bars = t.where("name", "bar").select();
    expect(bars[0].name).toBe("bar");
  });

  test("async query", async () => {
    let result = await query(async ({ }, { }, { }) => {
      const p = new Promise<string>((resolve) => {
        setTimeout(() => {
          resolve("foo");
        }, 100);
      });
      return await p;
    }, {});
    expect(result).toBe("foo");

    result = await query(async ({ }, { }, { db }) => {
      return await db.npmInstall("is-odd");
    }, {});
    
    expect(result.trim().startsWith("added 2 packages")).toBe(true);

    result = await query(async ({ }, { }, { db }) => {
      return await db.npmUninstall("is-odd");
    }, {});

    expect(result.trim().startsWith("removed 2 packages")).toBe(true);

  }, 20000);

  test("filters", () => {
    const bars = t.filter(doc => doc.name == "bar").select();
    expect(bars.length).toBe(3);
  });


  test("limit", () => {
    // t.removeIndex("name");
    for (let i = 0; i < 4; i++) {
      t.insert({
        bool: false,
        date: new Date(),
        name: "John"
      });
    }

    let johns = t.where("name", "John").select();
    expect(johns.length).toBeGreaterThan(2);
    johns = t.where("name", "John").limit(2).select();
    expect(johns.length).toBe(2);
  });

  test("paginate", () => {
    t.all().delete();
    t.insertMany(Array.from(Array(100)).map((und, i) => ({
      bool: false,
      date: new Date(),
      name: `Item ${i}`,
    })));

    expect(t.length).toBe(100);
    const page1 = t.all().paginate(1, 10).select();
    const page2 = t.all().paginate(2, 10).select();
    expect(page1.length).toBe(10);
    expect(page2.length).toBe(10);
    expect(page1[0].name).toBe("Item 0");
    expect(page2[0].name).toBe("Item 10");
    expect(page2[9].name).toBe("Item 19");
  });

  type SimpleFloat = {
    float: number
  };
  let t2: Table<SimpleFloat, number>;

  test("reset db", async () => {
    await allIsSaved();
    DataBase.removeTable(TestTableName);

    t2 = DataBase.createTable<SimpleFloat, number>({
      fields: {
        float: "number",
      },
      tags: {
        float: ["index"],
      },
      name: TestTableName
    });
    await allIsSaved();
  });

  test("0 index insert", () => {
    for (let i = 0; i < 100; i++) {
      const rand = Math.random();
      t2.insert({
        float: rand < 0.5 ? 0 : rand
      });
    }



    const zeros = t2.where("float", 0).select();

    expect(zeros.length).toBeGreaterThan(0);

  });

  test("order by", () => {
    t2.all().delete();

    const data: SimpleFloat[] = [];
    for (let i = 0; i < 100; i++) {
      data.push({
        float: i
      });
    }

    t2.insertMany(data);

    const evens = t2
      .all()
      .orderBy("float", "DESC")
      .paginate(2, 10)
      .filter(doc => (doc.float % 2 == 0))
      .select(doc => doc.float);

    expect(evens.length).toBe(10);
    expect(evens[0]).toBe(78);
    expect(evens[9]).toBe(60);

    const odds = t2
      .whereRange("float", 10, undefined)
      .orderBy("float")
      .paginate(1, 10)
      .filter(doc => (doc.float % 2 == 1))
      .select(doc => doc.float);

    expect(odds.length).toBe(10);
    expect(odds[0]).toBe(11);
    expect(odds[9]).toBe(29);
  });

  type TestWord = {
    id: string
    word: string
    part: string
    level: string
    oxfordLevel: string
  }


  test("update", async () => {


    const test_words: Table<TestWord, string> = DataBase.createTable<TestWord, string>({
      name: "test_words",
      fields: {
        "id": "string",
        "word": "string",
        "part": "string",
        "level": "string",
        "oxfordLevel": "string",
      },
      tags: {
        id: ["primary"]
      }
    });

    test_words.insert({
      id: "a",
      word: "a",
      part: "other",
      level: "x",
      oxfordLevel: "a1",
    });


    type PayloadType = Record<string, Record<string, string>>;
    const payload: PayloadType = {
      "a": {
        "level": "a1"
      }
    };


    const res = await query(({ test_words }: { test_words: Table<TestWord, string> }, { payload }, { }) => {
      test_words.where("id", ...Object.keys(payload)).update(doc => {
        const fields = payload[doc.id];

        for (const f in fields) {
          doc.$set(<any>f, fields[f]);
        }

      });
      return test_words.at("a");

    }, { payload });

    expect(test_words.at("a").level).toBe("a1");
    expect(res.id).toBe("a");
  });


  test("change id", () => {
    const test_words = DataBase.getTable<TestWord, string>("test_words");

    const utils = TableUtils.fromTable(test_words);
    expect(utils.mainDict.getOne("a")).toBeDefined();

    test_words.where("id", "a").update(rec => {
      rec.id = "new_a";
    });

    expect(utils.mainDict.getOne("new_a")).toBeDefined();
    expect(utils.mainDict.getOne("a")).toBe(undefined);
    expect(test_words.has("a")).toBe(false);
    expect(test_words.has("new_a")).toBe(true);
    const new_a = test_words.at("new_a");
    expect(new_a.part).toBe("other");
  });

  test("change id 2", async () => {

    const id = "new_a";
    const record: TestWord & PlainObject = {
      id: "new_new_a",
      level: "b1",
      oxfordLevel: "b1",
      part: "verb",
      word: "a",
    };

    const updated: TestWord & PlainObject = await query(({ test_words }: { test_words: Table<TestWord, string> }, { id, record }) => {
      test_words.where(<any>test_words.primaryKey, id).update(doc => {
        for (const key in record) {
          const newValue = record[key];
          if (doc.$get(key) != newValue) {
            doc.$set(key as any, newValue);
          }
        }
      });
      return test_words.at(record.id);
    }, { id, record });

    for (const key in record) {
      expect((updated)[key]).toBe(record[key]);
    }

    const test_words = DataBase.getTable<TestWord, string>("test_words");

    test_words.insert({
      ...record,
      id: "a",
    });

    let q = query(({ test_words }: { test_words: Table<TestWord, string> }, { id, record }: { id: string, record: TestWord & PlainObject }) => {
      test_words.where(<any>test_words.primaryKey, id).update(doc => {
        for (const key in record) {
          const newValue = record[key];
          if (doc.$get(key) != newValue) {
            doc.$set(key as any, newValue);
          }
        }
      });
      return test_words.at(record.id);
    }, {
      id: record.id,
      record: {
        ...record,
        id: "a",
      }
    });

    expect(q).rejects.toHaveProperty("message", "Primary key value 'a' on 'test_words[id]' already exists");
  });

  afterAll(async () => {
    await allIsSaved();
    rimraf.sync(process.cwd() + "/test_data");
  });
});
