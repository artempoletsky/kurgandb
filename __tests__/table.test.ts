import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog, rfs } from "../src/utils";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { faker } from "@faker-js/faker";
import { DataBase } from "../src/db";
import { Table, getMetaFilepath } from "../src/table";
import { Document, FieldType } from "../src/document";
import FragmentedDictionary from "../src/fragmented_dictionary";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table";

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

xdescribe("Table", () => {

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
    }).toThrow("Attempting to create a duplicate in the unique field 'jest_test_table[id].name'");
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

  test("renames a field", () => {
    expect(Object.keys(t.scheme.fields).length).toBe(4);
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

  afterAll(() => {
    // t.closePartition();
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
    salary: number
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
    "salary": "number",
  };

  const today = (new Date()).toJSON();
  const RichTypeRecord: RichType = {
    name: "John Doe",
    lastActive: today,
    birthday: today,
    registrationDate: today,
    salary: 0,
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

  function fakerGenerateUser(): RichType {
    return {
      name: faker.person.fullName(),
      login: faker.internet.userName(),
      birthday: faker.date.birthdate(),
      blog_posts: [],
      email: faker.internet.email(),
      phoneNumber: faker.phone.number(),
      favoriteQuote: faker.lorem.sentence(),
      status: faker.lorem.sentence(),
      lastTweet: faker.lorem.sentence(),
      pageSlug: faker.lorem.word(),
      lastActive: faker.date.recent(),
      registrationDate: faker.date.past(),
      isAdmin: false,
      isModerator: faker.datatype.boolean(),
      password: faker.internet.password(),
      favoriteRandomNumber: faker.number.float(),
      salary: faker.number.float({
        max: 10000,
        precision: 0.01
      }),
    }
  }
  let t: Table<number, RichType>;
  let row: any[];
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
        tags: {
          lastActive: ["index"],
          birthday: ["index"],
          pageSlug: ["index"],
          // login: ["unique"],
          // email: ["unique"],
          // phoneNumber: ["unique"],
        },
      });

      row = t.flattenObject(RichTypeRecord);
    })

    test("Document.validate data ", () => {
      const invalidReason = Document.validateData(RichTypeRecord, t.scheme);
      expect(invalidReason).toBe(false)
    });

    test("moderate insert fill", () => {
      const docsToInsert = 10 * 1000;
      const initialLenght = t.length;




      for (let i = 0; i < docsToInsert; i++) {
        // t.insert(RichTypeRecord);
        t.insert(fakerGenerateUser());
      }

      expect(t.length).toBe(initialLenght + docsToInsert);




      perfStart("at");
      t.at(123);
      perfEnd("at");
      perfLog("at");
      expect(perfDur("at")).toBeLessThan(30);
    });


    xtest("moderate square fill", () => {
      const squareSize = 10 * 1000 - 1;
      const square = Array.from(Array(squareSize)).map(() => t.flattenObject(fakerGenerateUser()));
      const initialLenght = t.length;

      expect(square.length).toBe(squareSize);
      // expect(square[123][0]).toBe("John Doe");

      t.insertSquare(square);

      expect(t.length).toBe(initialLenght + square.length);
      t.insertSquare(square);



      perfStart("at");
      t.at(123);
      perfEnd("at");
      perfLog("at");
      expect(perfDur("at")).toBeLessThan(30);
    });


    xtest("proliferate fill", () => {
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


    afterAll(() => {
      // t.closePartition();
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

      console.log(t.at(700));

      perfStart("at");
      t.at(9);
      perfEnd("at");

      console.log(t.at(9));

      perfLog("query");
      perfLog("at");
      expect(perfDur("query")).toBeLessThan(30)
      expect(perfDur("at")).toBeLessThan(30)

    });

    test("where", () => {
      if (!t.fieldHasAnyTag("salary", "index")) {
        perfStart("salaryIndex");
        t.createIndex("salary", false);
        perfEnd("salaryIndex");

        perfLog("salaryIndex");
      }

      perfStart("whereSalary");
      const rich = t.whereRange("salary", 900, 1000).select();
      perfEnd("whereSalary");

      perfLog("whereSalary");


      perfStart("whereBirthday");
      const sameBirthday = t.where("birthday", rich[1].birthday).select();
      perfEnd("whereBirthday");

      console.log(sameBirthday.length);
      
      perfLog("whereBirthday");
      // console.log(rich.length);
      
      
    });

  });

});
