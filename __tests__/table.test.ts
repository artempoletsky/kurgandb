import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog, rfs } from "../src/utils";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { faker } from "@faker-js/faker";
import { DataBase } from "../src/db";
import { Table, getMetaFilepath } from "../src/table";
import { Document, FieldType } from "../src/document";
import FragmentedDictionary from "../src/fragmented_dictionary";
import { allIsSaved } from "../src/virtual_fs";


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
    date: Date | string | number,
    bool: boolean,
    name: string,
    heavy: null | {
      bar: number
    }
  }
  let t: Table<number, SimpleType>;
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


    if (DataBase.isTableExist(TestTableName)) {
      DataBase.removeTable(TestTableName);
    }
    t = DataBase.createTable<number, SimpleType>({
      name: TestTableName,
      fields: {
        bool: "boolean",
        date: "date",
        name: "string",
        // light: "json",
        heavy: "JSON",
      },
      tags: {
        name: ["index"]
      }
    });
  });

  test("makes objects storable", () => {
    const storable = t.makeObjectStorable({
      name: "foo",
      bool: true,
      date: new Date(),
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
      heavy: {
        bar: Math.random()
      }
    });


    expect(t.length).toBe(lenghtOffset + 2);

    expect(i2).toBe(idOffset + 2);

    const d1 = t.at(i1);

    expect(d1).toBeTruthy();
    if (!d1) return;

    expect(d1.name).toBe("foo");
    expect(t.at(i2)?.name).toBe("bar");

  });

  test("has tag functions", () => {
    expect(Table.tagsHasFieldNameWithAnyTag(t.scheme.tags, "name", "index", "unique")).toBe(true);
    expect(t.fieldHasAnyTag("name", "index", "unique")).toBe(true);

    expect(t.fieldHasAllTags("name", "index", "unique")).toBe(false);
    expect(t.fieldHasAnyTag("name", "primary")).toBe(false);

  });

  test("indices", () => {
    expect(t.fieldHasAnyTag("name", "index")).toBe(true);
    let indexDict = FragmentedDictionary.open<string, number[]>(t.getIndexDictDir("name"));

    indexDict.setOne("blablabla", [123]);
    const arr = indexDict.getOne("blablabla");
    expect(arr).toBeDefined();
    if (!arr) return;
    expect(arr[0]).toBe(123);
    indexDict.remove(["blablabla"]);
    expect(indexDict.getOne("blablabla")).toBe(undefined);


    t.storeIndexValue("name", 456, "foo123");
    indexDict = FragmentedDictionary.open<string, number[]>(t.getIndexDictDir("name"));
    expect(indexDict.getOne("foo123")).toBeDefined();
    indexDict.remove(["foo123"]);

    const foos = indexDict.getOne("foo");
    expect(foos).toBeDefined();
    if (!foos) return;
    expect(foos[0]).toBe(1);


    t.insert({
      bool: true,
      date: Date.now(),
      heavy: null,
      name: "bar",
    });

    const bars = t.where("name", "bar").select();

    expect(bars.length).toBe(2);
  });

  test("filters", () => {
    const bars = t.filter(doc => doc.name == "bar").select();
    expect(bars.length).toBe(2);
  });

  test("removes and creates index", () => {
    t.removeIndex("name");
    expect(t.fieldHasAnyTag("name", "index", "unique")).toBe(false);

    let bars = t.where("name", "bar").select();

    expect(bars.length).toBe(2);

    expect(() => {
      t.createIndex("name", true);
    }).toThrow(`Attempting to create a duplicate in the unique field '${TestTableName}[id].name'`);
    expect(t.fieldHasAnyTag("name", "index", "unique")).toBe(false);

    t.createIndex("name", false);
    expect(t.fieldHasAnyTag("name", "index")).toBe(true);

    bars = t.where("name", "bar").select();

    expect(bars.length).toBe(2);

    const lastBarID = t.insert({
      ...t.createDefaultObject(),
      name: "bar"
    });

    bars = t.where("name", "bar").select();
    expect(bars.length).toBe(3);

    t.where("id", lastBarID).delete();

    bars = t.where("name", "bar").select();
    expect(bars.length).toBe(2);

    const indexDict = FragmentedDictionary.open<string, number[]>(t.getIndexDictDir("name"));

    const arr = indexDict.getOne("bar");
    expect(arr).toBeDefined();
    if (!arr) return;
    expect(arr.length).toBe(2);
    expect(arr.indexOf(lastBarID)).toBe(-1);
  });

  test("limit", () => {
    // t.removeIndex("name");
    for (let i = 0; i < 4; i++) {
      t.insert({
        bool: false,
        date: Date.now(),
        heavy: null,
        name: "John"
      });
    }

    let johns = t.where("name", "John").select();
    expect(johns.length).toBeGreaterThan(2);
    johns = t.where("name", "John").select(2);
    expect(johns.length).toBe(2);
  });

  test("renames a field", () => {
    expect(Object.keys(t.scheme.fields).length).toBe(4);
    t.renameField("heavy", "foo");

    // console.log(t.scheme.fields);
    expect(t).not.toHaveProperty("scheme.fields.heavy");
    expect(t).toHaveProperty("scheme.fields.foo");

    expect(() => {
      t.renameField("name", "foo");
    }).toThrow(`Field 'foo' already exists`);

    expect(t).toHaveProperty("scheme.fields.name");

    t.renameField("name", "bar");
    expect(t.scheme.tags).not.toHaveProperty("name");
    expect(t.scheme.tags.bar.includes("index")).toBe(true);
    expect(Object.keys(t.scheme.fields).length).toBe(4);
  });

  test("removes a field", () => {
    t.removeField("bar");
    expect(t.scheme.fields).not.toHaveProperty("bar");
    expect(t.scheme.tags).not.toHaveProperty("bar");
    expect(Object.keys(t.scheme.fields).length).toBe(3);
  });

  test("adds a field", () => {
    t.addField("random", "number", e => Math.random());
    expect(t.scheme.fields).toHaveProperty("random");
    const d: any = t.at(1);
    expect(d.random).toBeLessThan(1);
  });



  afterAll(async () => {
    await allIsSaved();
    // t.closePartition();
    DataBase.removeTable(TestTableName);
  });
});
