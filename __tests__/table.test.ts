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

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table_0";

xdescribe("loading index", () => {
  const meta = rfs(getMetaFilepath(TestTableName));
  xtest("can load index", async () => {
    // await Table.loadIndex(TestTableName, meta);
  });
  test("getTable", () => {
    const t = DataBase.getTable(TestTableName);
    console.log(t.length);
  })
});

describe("Table", () => {

  type SimpleType = {
    id: number,
    date: Date | string | number,
    bool: boolean,
    name: string,
    light: string[],
    heavy: null | {
      bar: number
    }
  }
  type SimpleInsert = Omit<SimpleType, "id">;
  let t: Table<SimpleType, number, any, SimpleInsert>;
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
        light: "json",
        heavy: "json",
      },
      tags: {
        id: ["primary", "autoinc"],
        name: ["index"],
        heavy: ["heavy"],
      }
    });


  });

  test("scheme check", () => {
    expect(t.scheme.fieldsOrder.includes("heavy")).toBe(false);
    expect(t.scheme.fieldsOrderUser.includes("heavy")).toBe(true);
    expect(t.scheme.fieldsOrder.length).toBe(4);
    expect(t.scheme.fieldsOrderUser.length).toBe(6);
    expect(t.scheme.fieldsOrderUser.includes("id")).toBe(true);
    expect(t.scheme.fieldsOrder.includes("id")).toBe(false);

    for (const fieldName of t.scheme.fieldsOrderUser) {
      expect(t.scheme.tags[fieldName]).toHaveProperty("length");
    }
  });

  test("makes objects storable", () => {
    const storable = t.makeObjectStorable({
      name: "foo",
      bool: true,
      date: new Date(),
      light: ["1", "2", "3"],
      heavy: {
        bar: 123
      }
    });

    expect(storable.name).toBe("foo");
    expect(storable).toHaveProperty("heavy.bar");
    expect(storable.heavy.bar).toBe(123);

    expect(storable.bool).toBe(1);
    expect(typeof storable.date).toBe("number");
  });

  test("adds a document", () => {


    const idOffset = t.getLastIndex();
    const lenghtOffset = t.length;

    const i1 = t.insert({
      date: new Date(),
      bool: false,
      name: "foo",
      light: ["1", "2", "3"],
      heavy: {
        bar: Math.random()
      }
    });

    expect(i1).toBe(idOffset + 1);

    // d1.get("heavy")
    const i2 = t.insert({
      date: new Date(),
      bool: true,
      name: "bar",
      light: ["1", "2", "3"],
      heavy: {
        bar: Math.random()
      }
    });


    expect(t.length).toBe(lenghtOffset + 2);

    expect(i2).toBe(idOffset + 2);

    const d1 = t.at(i1);


    expect(d1.name).toBe("foo");
    expect(t.at(i2)?.name).toBe("bar");

  });

  test("has tag functions", () => {
    expect(Table.tagsHasFieldNameWithAnyTag(t.scheme.tags, "name", "index", "unique")).toBe(true);
    expect(t.fieldHasAnyTag("name", "index", "unique")).toBe(true);

    expect(t.fieldHasAllTags("name", "index", "unique")).toBe(false);
    expect(t.fieldHasAnyTag("name", "primary")).toBe(false);

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

  test("arrays", () => {
    type VariedArrays = {
      name: string
      data: any[]
    };
    const t = DataBase.createTable<VariedArrays, string>({
      name: "arrays",
      fields: {
        "data": "json",
        "name": "string"
      },
      tags: {
        name: ["primary"],
        data: ["heavy"],
      }
    });
    const toInsert: VariedArrays = {
      "data": [],
      "name": "blacklist"
    };

    expect(t.indexFieldName.length).toBe(0);

    const flattened = t.flattenObject(toInsert);


    expect(flattened.length).toBe(0);

    expect(t.primaryKey).toBe("name");

    const id = t.insert(toInsert);

    expect(existsSync("/arrays/heavy/data/blacklist.json")).toBe(true);

    expect(id).toBe("blacklist");

    expect(() => {
      t.insert(toInsert);
    }).toThrow("Primary key value 'blacklist' on 'arrays[name]' already exists");

    t.where("name", "blacklist").update(doc => {
      const arr = doc.data;
      arr.push(1, 2, 3);
      doc.data = arr;
    });

    const blacklist = t.at("blacklist");
    expect(blacklist).toBeDefined();
    if (!blacklist) return;
    expect(blacklist.data[0]).toBe(1);
    expect(blacklist.data[1]).toBe(2);
    expect(blacklist.data[2]).toBe(3);
    expect(blacklist.name).toBe("blacklist");
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

  test("ResponseError throw", async () => {


    const q = query(({ test_words }: { test_words: Table<TestWord, string> }, { }, { $ }) => {
      return test_words.at("foo1231");
    }, {});

    await expect(q).rejects.toHaveProperty("message", "id 'foo1231' doesn't exists at 'test_words[id]'");
  });

  test("getFreeId", () => {
    const test_words = DataBase.getTable<TestWord, string>("test_words");
    expect(test_words.getFreeId()).toBe("new_id");
    type Numeric = { id: number; }

    const num_id = DataBase.createTable<Numeric, number>({
      name: "num_id",
      fields: {
        id: "number",
      },
      tags: {
        id: ["primary", "autoinc"],
      }
    });

    expect(num_id.getFreeId()).toBe(1);

    let id = num_id.insert({});
    expect(id).toBe(1);
    expect(num_id.getFreeId()).toBe(2);
    id = num_id.insert({ id: 5 });
    expect(id).toBe(2);

    expect(num_id.getFreeId()).toBe(3);
    num_id.where("id", 2).delete();
    expect(num_id.getFreeId()).toBe(3);
  });


  afterAll(async () => {
    await allIsSaved();
    // t.closePartition();
    DataBase.removeTable(TestTableName);
    DataBase.removeTable("arrays");
    DataBase.removeTable("test_words");
    DataBase.removeTable("num_id");
    await allIsSaved();

  });
});
