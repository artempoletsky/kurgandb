import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog, rfs } from "../src/utils";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { before } from "node:test";
import { DataBase } from "../src/db";
import { PartitionMeta, Table, getMetaFilepath } from "../src/table";
import { Document, FieldType } from "../src/document";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table";

xdescribe("loading index", () => {
  const meta = rfs(getMetaFilepath(TestTableName));
  xtest("can load index", async () => {
    // await Table.loadIndex(TestTableName, meta);
  });
  test("getTable", ()=>{
    const t = DataBase.getTable(TestTableName);
    console.log(t.length);
  })
});

xdescribe("Table", () => {


  let t: Table<any>;
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
    t = DataBase.createTable({
      name: TestTableName,
      fields: {
        name: "string",
        // light: "json",
        heavy: "JSON",
      },
      indices: ["name"]
    });
  });

  test("adds a document", () => {


    const idOffset = t.getLastIndex();
    const lenghtOffset = t.length;

    const d1 = t.insert({
      name: "foo",
      heavy: {
        bar: Math.random()
      }
    });

    // d1.get("heavy")
    const d2 = t.insert({
      name: "bar",
      heavy: {
        bar: Math.random()
      }
    });


    expect(t.length).toBe(lenghtOffset + 2);
    expect(d1.id).toBe(idOffset + 1);
    expect(d2.id).toBe(idOffset + 2);

    expect(d1.name).toBe("foo");
    expect(d2.name).toBe("bar");

  });
  // return
  test("renames a field", () => {
    t.renameField("heavy", "foo");

    // console.log(t.scheme.fields);
    expect(t).not.toHaveProperty("scheme.fields.heavy");
    expect(t).toHaveProperty("scheme.fields.foo");

    try {
      t.renameField("name", "foo");
    } catch (error: any) {
      expect(error.message).toBe(`Field 'foo' already exists`);
    } finally {
      expect(t).toHaveProperty("scheme.fields.name");
    }

    t.renameField("name", "bar");
    expect(t.scheme.indices.includes("name")).toBe(false);
    expect(t.scheme.indices.includes("bar")).toBe(true);
  });

  test("removes a field", () => {
    t.removeField("bar");
    expect(t.scheme.fields).not.toHaveProperty("bar");
    expect(t.scheme.indices.includes("bar")).toBe(false);
  });

  test("adds a field", () => {
    t.addField("random", "number", e => Math.random());
    expect(t.scheme.fields).toHaveProperty("random");
    const d = t.at(1);
    expect(d.random).toBeLessThan(1);
  });

  afterAll(() => {
    t.closePartition();
    // DataBase.removeTable(tableName);
  });
});

describe("Rich table", () => {
  type RichType = {
    name: string
    lastTweet: string
    status: string
    isAdmin: boolean
    isModerator: boolean
    pageSlug: string
    login: string
    email: string
    phoneNumber: string

    registrationDate: Date | string
    lastActive: Date | string
    birthday: Date | string

    password: string
    blog_posts: number[]
    favoriteQuote: string

    favoriteRandomNumber: number
  }

  const RichTypeFields: Record<keyof RichType, FieldType> = {
    "name": "string",
    "lastTweet": "string",
    "status": "string",
    "isAdmin": "boolean",
    "isModerator": "boolean",
    "registrationDate": "date",
    "lastActive": "date",
    "birthday": "date",
    "password": "password",
    "blog_posts": "json",
    "favoriteQuote": "string",
    "favoriteRandomNumber": "number",
    "pageSlug": "string",
    "login": "string",
    "email": "string",
    "phoneNumber": "string",
  };

  const today = (new Date()).toJSON();
  const RichTypeRecord: RichType = {
    name: "John Doe",
    lastActive: today,
    birthday: today,
    registrationDate: today,
    pageSlug: "asdasdasdasd",
    login: "johndoe",
    email: "johndoe@example.com",
    phoneNumber: "+435345345345",
    favoriteQuote: "Poor and content is rich and rich enough",
    status: "The wolf is weaker than the lion and the tiger but it won't perform in the circus.",
    lastTweet: "The wolf is weaker than the lion and the tiger but it won't perform in the circus.",
    isAdmin: false,
    isModerator: false,
    password: "qwerty123",
    blog_posts: [343434, 234823742, 439785345, 34583475345, 3453845734, 3458334535, 1231248576],
    favoriteRandomNumber: 0,
  };
  let t: Table<RichType>;
  let row: [any[], any[]];
  xdescribe("filling", () => {

    beforeAll(() => {
      // DataBase.createTable({
      //   name: "users",
      //   "fields": {

      //   },
      //   "settings": {}
      // });

      if (DataBase.isTableExist(TestTableName)) {
        DataBase.removeTable(TestTableName);
      }

      t = DataBase.createTable({
        name: TestTableName,
        fields: RichTypeFields,
        indices: ["lastActive", "birthday", "pageSlug", "login", "email", "phoneNumber"]
      });

      row = t.flattenObject(RichTypeRecord);
    })

    test("Document.validate data ", () => {
      const invalidReason = Document.validateData(RichTypeRecord, t.scheme);
      expect(invalidReason).toBe(false)
    });


    test("moderate fill", () => {
      const squareSize = 10 * 1000 - 1;
      const square = Array.from(Array(squareSize)).map(() => row);
      const initialLenght = t.length;

      expect(square.length).toBe(squareSize);
      expect(square[123][0][0]).toBe("John Doe");

      t.insertSquare(square);

      expect(t.length).toBe(initialLenght + square.length);

      t.insertSquare(square);
      t.closePartition();
      t.saveIndexPartitions();

      perfStart("at");
      t.at(123);
      perfEnd("at");
      perfLog("at");
      expect(perfDur("at")).toBeLessThan(30);
    });


    test("proliferate fill", () => {
      const squaresToAdd = 1 * 1;

      const squareSize = 10 * 1000 * 1000;
      const square = Array.from(Array(squareSize)).map(() => row);

      // type Rec = {
      //   random: number
      //   name: string
      // }
      const initialLenght = t.length;
      for (let i = 0; i < squaresToAdd; i++) {
        t.insertSquare(square);
      }
      expect(t.length).toBe(initialLenght + squaresToAdd * square.length);
    });


    xtest("ifdo", () => {
      perfStart("ifdo");
      t.ifDo(doc => doc.favoriteRandomNumber == 0, doc => doc.favoriteRandomNumber = Math.random());
      perfEnd("ifdo");
      perfLog("ifdo");
    });


    afterAll(() => {
      t.closePartition();
    })
  });



  describe("reading and modifying", () => {

    beforeAll(() => {
      t = DataBase.getTable(TestTableName);
    })

    test("at", () => {

      perfStart("query");
      clientQuery(({ jest_test_table }, { db }) => {
        return jest_test_table.at(9);
      });
      perfEnd("query");

      console.log(t.at(123123));

      perfStart("at");
      t.at(9);
      perfEnd("at");

      perfLog("query");
      perfLog("at");
      expect(perfDur("query")).toBeLessThan(30)
      expect(perfDur("at")).toBeLessThan(30)

    });

    test("insert", () => {
      const last = t.getLastIndex();
      const doc = t.insert(RichTypeRecord);
      expect(doc.id).toBe(last + 1);
    });

    xtest("create index", () => {
      t.createIndex("favoriteRandomNumber");
      const doc = t.at(123);
      expect(doc).toBeTruthy();
      if (!doc) return;
      // expect(doc.favoriteRandomNumber)
    });


    afterAll(() => {
      t.closePartition();
      t.saveIndexPartitions();
    })
  });

});


describe("Misc", () => {
  test("String id genetation consistancy", () => {
    const generate = (num: number) => {
      return Table.idNumber(Table.idString(num));
    }


    expect(generate(1)).toBe(1);
    expect(generate(1 * 100)).toBe(1 * 100);
    expect(generate(1000 * 1000)).toBe(1000 * 1000);
    expect(generate(10 * 1000 * 1000)).toBe(10 * 1000 * 1000);
    expect(generate(1000 * 1000 * 1000)).toBe(1000 * 1000 * 1000);
  });

  test("Partition ID search", () => {
    const partitions: PartitionMeta[] = [];
    const lenght = 1000 * 1000 * 1000;
    const partitionSize = 100 * 1000;
    const numPartitions = lenght / partitionSize;
    let end = partitionSize;
    for (let i = 0; i < numPartitions; i++) {
      partitions.push({
        length: partitionSize,
        end,
      });
      end += partitionSize;
    }

    const id = Math.floor(Math.random() * lenght);

    perfStart("partition search");
    const partitionId = Table.findPartitionForId(id, partitions, lenght);
    perfEnd("partition search");

    // perfLog("partition search")
    expect(partitionId).not.toBe(false);
    expect(perfDur("partition search")).toBeLessThan(20);
  });
});