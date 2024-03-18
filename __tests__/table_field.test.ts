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
import TableUtils from "../src/table_utilities";

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

describe("Table fields", () => {

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

    t.insert({
      bool: true,
      date: new Date(),
      name: "foo",
      light: ["foo"],
      heavy: null,
    });

  });

  test("rename", () => {
    expect(Object.keys(t.scheme.fields).length).toBe(6);
    t.renameField("heavy", "foo");
    expect(t.scheme.fieldsOrderUser.includes("heavy")).toBe(false);
    expect(t.scheme.fieldsOrderUser.includes("foo")).toBe(true);
    expect(t.scheme.fieldsOrder.includes("heavy")).toBe(false);
    expect(t.scheme.fieldsOrder.includes("foo")).toBe(false);


    expect(t).not.toHaveProperty("scheme.fields.heavy");
    expect(t).toHaveProperty("scheme.fields.foo");
    expect(t.scheme.fields.foo).toBe("json");

    expect(() => {
      t.renameField("name", "foo");
    }).toThrow(`Field 'foo' already exists`);

    expect(t).toHaveProperty("scheme.fields.name");

    t.renameField("name", "bar");
    expect(t.scheme.fieldsOrderUser.includes("name")).toBe(false);
    expect(t.scheme.fieldsOrderUser.includes("bar")).toBe(true);

    expect(t.scheme.fieldsOrder.includes("name")).toBe(false);
    expect(t.scheme.fieldsOrder.includes("bar")).toBe(true);

    expect(t.scheme.tags).not.toHaveProperty("name");
    expect(t.scheme.tags.bar.includes("index")).toBe(true);
    expect(Object.keys(t.scheme.fields).length).toBe(6);
  });

  test("remove", async () => {
    const utils = TableUtils.fromTable(t);
    let dict = FragmentedDictionary.open<number, any[]>(utils.getMainDictDir());
    let first = dict.getOne(1);
    expect(first?.length).toBe(4);

    t.removeField("bar");
    expect(t.scheme.fields).not.toHaveProperty("bar");
    expect(t.scheme.tags).not.toHaveProperty("bar");
    expect(t.scheme.fieldsOrder.includes("bar")).toBe(false);
    expect(t.scheme.fieldsOrderUser.includes("bar")).toBe(false);

    expect(Object.keys(t.scheme.fields).length).toBe(5);
    await allIsSaved();

    dict = FragmentedDictionary.open<number, any[]>(utils.getMainDictDir());

    first = dict.getOne(1);
    expect(first?.length).toBe(3);

    const d = t.at(1);
    expect(d.light[0]).toBe("foo");


  });

  test("add", async () => {
    t.addField("123", "number", false, e => Math.random());
    expect(t.scheme.fields).toHaveProperty("123");
    
    const d: any = t.at(1);
    expect(d["123"]).toBeLessThan(1);
    expect(d.light[0]).toBe("foo");
  });


  test("changeFieldIndex", () => {
    let item = t.at(1);

    let iOf1 = t.scheme.fieldsOrderUser.indexOf("id");
    expect(iOf1).toBe(0);

    let iOf2 = Object.keys(item).indexOf("id");
    expect(iOf2).toBe(1);

    t.changeFieldIndex("id", 1);

    item = t.at(1);

    iOf1 = t.scheme.fieldsOrderUser.indexOf("id");
    iOf2 = Object.keys(item).indexOf("id");
    expect(iOf1).toBe(1);
    expect(iOf2).toBe(2);
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


  afterAll(async () => {
    await allIsSaved();
    DataBase.removeTable(TestTableName);
    await allIsSaved();
  });
});
