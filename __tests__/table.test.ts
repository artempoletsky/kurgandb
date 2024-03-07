import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { perfEnd, perfStart, perfDur, perfLog, rfs } from "../src/utils";
// import { star as query } from "../src/api";
import { standAloneQuery as query } from "../src/client";
import { faker } from "@faker-js/faker";
import { DataBase } from "../src/db";
import { Table, getMetaFilepath, packEventListener } from "../src/table";
import FragmentedDictionary from "../src/fragmented_dictionary";
import { allIsSaved, existsSync } from "../src/virtual_fs";
import { constructFunction } from "../src/function";


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
    light: string[],
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

    DataBase.init(process.cwd() + "/test_data");

    if (DataBase.doesTableExist(TestTableName)) {
      DataBase.removeTable(TestTableName);
    }
    t = DataBase.createTable<number, SimpleType>({
      name: TestTableName,
      fields: {
        bool: "boolean",
        date: "date",
        name: "string",
        light: "json",
        heavy: "json",
      },
      tags: {
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
      light: ["1", "2", "3"],
      heavy: null,
      name: "bar",
    });

    const bars = t.where("name", "bar").select();

    expect(bars.length).toBe(2);
  });

  test("where", () => {
    const oneThree = t.where<number>("id", 1, 3).select();
    expect(oneThree[0].id).toBe(1);
    expect(oneThree[1].id).toBe(3);

    const notBars = t.where<string>("name", name => name != "bar").select();
    expect(notBars[0].name).toBe("foo");

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
    }).toThrow(`Unique value 'bar' for field '${TestTableName}[id].name' already exists`);

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
        light: ["1", "2", "3"],
        heavy: null,
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
      date: Date.now(),
      light: ["1", "2", "3"],
      heavy: null,
      name: `Item ${i}`
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

  test("renames a field", () => {
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

  test("removes a field", async () => {
    let dict = FragmentedDictionary.open<number, any[]>(t.getMainDictDir());
    let first = dict.getOne(1);
    expect(first?.length).toBe(4);

    t.removeField("bar");
    expect(t.scheme.fields).not.toHaveProperty("bar");
    expect(t.scheme.tags).not.toHaveProperty("bar");
    expect(t.scheme.fieldsOrder.includes("bar")).toBe(false);
    expect(t.scheme.fieldsOrderUser.includes("bar")).toBe(false);

    expect(Object.keys(t.scheme.fields).length).toBe(5);
    await allIsSaved();

    dict = FragmentedDictionary.open<number, any[]>(t.getMainDictDir());

    first = dict.getOne(1);
    expect(first?.length).toBe(3);

    const d = t.at(1);
    expect(d.light[1]).toBe("2");


  });

  test("adds a field", async () => {
    t.addField("123", "number", false, e => Math.random());
    expect(t.scheme.fields).toHaveProperty("123");
    await allIsSaved();
    const d: any = t.at(1);
    expect(d["123"]).toBeLessThan(1);
    expect(d.light[0]).toBe("1");
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
  let t2: Table<number, SimpleFloat>;
  test("reset db", async () => {
    await allIsSaved();
    DataBase.removeTable(TestTableName);

    t2 = DataBase.createTable<number, SimpleFloat>({
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

  test("arrays", () => {
    type VariedArrays = {
      name: string
      data: any[]
    };
    const t = DataBase.createTable<string, VariedArrays>({
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
  type TestWordsMeta = {
    levelsLen: Record<string, number>
  }

  test("update", async () => {


    const test_words = DataBase.createTable<string, TestWord>({
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

    const res = await query(({ test_words }, { payload }, { }) => {
      test_words.where("id", ...Object.keys(payload)).update(doc => {
        const fields = payload[doc.id];

        for (const f in fields) {
          doc.set(<any>f, fields[f]);
        }

      });
      return test_words.at("a");
    }, { payload });


    expect(test_words.at("a").level).toBe("a1");
    expect(res.id).toBe("a");
  });


  test("ResponseError throw", async () => {

    const q = query(({ test_words }: { test_words: Table<string, TestWord> }, { }, { }) => {
      return test_words.at("foo1231");
    }, {});

    await expect(q).rejects.toHaveProperty("message", "id 'foo1231' doesn't exists at 'test_words[id]'");
  });


  afterAll(async () => {
    await allIsSaved();
    // t.closePartition();
    DataBase.removeTable(TestTableName);
    DataBase.removeTable("arrays");
    DataBase.removeTable("test_words");

  });
});
