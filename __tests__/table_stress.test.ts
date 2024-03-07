import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { perfEnd, perfStart, perfDur, perfLog, perfEndLog } from "../src/utils";
import { standAloneQuery as query } from "../src/client";
import { faker } from "@faker-js/faker";
import { DataBase } from "../src/db";
import { Table, getMetaFilepath } from "../src/table";
import { FieldType } from "../src/globals";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

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
  "password": "string",
  "blog_posts": "json",
  "favoriteQuote": "string",
  "favoriteRandomNumber": "number",
  "pageSlug": "string",
  "login": "string",
  "email": "string",
  "phoneNumber": "string",
  "salary": "number",
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


const TestTableName = "jest_test_table";
xdescribe("foo", () => {
  test("foo", () => {
    expect(1).toBe(1);
  });
});

describe("Rich table", () => {


  // const today = (new Date()).toJSON();
  // const RichTypeRecord: RichType = {
  //   name: "John Doe",
  //   lastActive: today,
  //   birthday: today,
  //   registrationDate: today,
  //   salary: 0,
  //   pageSlug: "asdasdasdasd",
  //   login: "johndoe",
  //   email: "johndoe@example.com",
  //   phoneNumber: "+435345345345",
  //   favoriteQuote: "Poor and content is rich and rich enough",
  //   status: "The wolf is weaker than the lion and the tiger but it won't perform in the circus.",
  //   lastTweet: "The wolf is weaker than the lion and the tiger but it won't perform in the circus.",
  //   isAdmin: false,
  //   isModerator: false,
  //   password: "qwerty123",
  //   blog_posts: [343434, 234823742, 439785345, 34583475345, 3453845734, 3458334535, 1231248576],
  //   favoriteRandomNumber: 0,
  // };

  let t: Table<RichType, number>;
  let row: any[];
  describe("filling", () => {

    beforeAll(() => {
      // DataBase.createTable({
      //   name: "users",
      //   "fields": {

      //   },
      //   "settings": {}
      // });
      DataBase.init(process.cwd() + "/test_data");
      if (DataBase.doesTableExist(TestTableName)) {
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

      // t = DataBase.getTable(TestTableName);

      // row = t.flattenObject(RichTypeRecord);
    })

    test("moderate insert fill", () => {
      const docsToInsert = 10 * 1000;
      const initialLenght = t.length;

      const docs: RichType[] = [];
      perfStart("fakerGeneration");
      for (let i = 0; i < docsToInsert; i++) {
        docs.push(fakerGenerateUser());
      }
      perfEndLog("fakerGeneration");

      perfStart("insertMany");
      t.insertMany(docs);
      perfEndLog("insertMany");

      expect(t.length).toBe(initialLenght + docsToInsert);

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
        // t.insertSquare(square);
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

    test("at", async () => {

      perfStart("query");
      const rec = await query(({ jest_test_table }: { jest_test_table: Table<RichType, number> }, { }, { db }) => {
        return jest_test_table.at(9);
      }, undefined);
      perfEnd("query");


      perfStart("at");
      t.at(9);
      perfEnd("at");

      // console.log(t.at(9));

      perfLog("query");
      perfLog("at");
      expect(perfDur("query")).toBeLessThan(30)
      expect(perfDur("at")).toBeLessThan(30)

    });

    test("where", () => {
      if (!t.fieldHasAnyTag("salary", "index")) {
        perfStart("salaryIndex");
        t.createIndex("salary", false);
        perfEndLog("salaryIndex");

      }

      perfStart("whereSalary");
      const rich = t.whereRange("salary", 900, 1000).limit(0).select();
      perfEndLog("whereSalary");

      console.log(rich.length);


      perfStart("whereBirthday");
      const sameBirthday = t.where("birthday", rich[1].birthday as string).select();
      perfEnd("whereBirthday");

      console.log(sameBirthday.length);

      perfLog("whereBirthday");
      // console.log(rich.length);


    });

  });

});
