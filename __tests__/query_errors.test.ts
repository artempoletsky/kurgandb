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
import { ResponseError } from "@artempoletsky/easyrpc";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };


describe("Query errors", () => {

  type TestWord = {
    word: string;
  };

  let t: Table<TestWord, number, any>;
  beforeAll(() => {

    DataBase.init(process.cwd() + "/test_data");

    t = DataBase.createTable<TestWord, number>({
      name: "test_words",
      fields: {
        word: "string",
      },
      tags: {
        word: ["primary"],
      }
    });
  });

  test("400 error", async () => {
    const q = query(({ }, { }, { $ }) => {
      throw new $.ResponseError("Foo");
    }, {});
    await expect(q).rejects.toHaveProperty("message", "Foo");
    await expect(q).rejects.toHaveProperty("statusCode", 400);
  });


  test("500 error", async () => {
    const q = query(({ }, { }, { $ }) => {
      throw new Error("Foo");
    }, {});
    await expect(q).rejects.toHaveProperty("message", "Query has failed with error: Error: Foo");
    await expect(q).rejects.toHaveProperty("statusCode", 500);
  });


  test("Wrong table name", async () => {

    try {
      await query(({ }, { }, { db }) => {
        db.getTable("foo");
      }, {});
    } catch (e: any) {
      const err: ResponseError = e;
      expect(err.message).toBe("Table {...} doesn't exist");
      expect(err.response.statusCode).toBe(404);
    }
  });


  test("Wrong record ID", async () => {
    let result;
    try {
      result = await query(({ test_words }: { test_words: Table<TestWord, string> }, { }, { $ }) => {
        return test_words.at("foo1231");
      }, {});
    } catch (e: any) {
      const err: ResponseError = e;
      expect(err.message).toBe("id 'foo1231' doesn't exists at 'test_words[word]'");
      expect(err.response.message).toBe("id 'foo1231' doesn't exists at 'test_words[word]'");
      expect(err.response.statusCode).toBe(404);
    }
    expect(result).toBe(undefined)
  });


  test("Eval catch", async () => {
    let result;

    try {
      result = await query(({ test_words }: { test_words: Table<TestWord, string> }, { }, { $ }) => {
        const queryString = "variable.foo = 1";
        try {
          eval(queryString)
        } catch (err) {
          throw new $.ResponseError(`Query string contains errors: {...}`, [err + ""]);
        }
        return 1;
      }, {});
    } catch (e: any) {
      const err: ResponseError = e;
      expect(err.message).toBe("Query string contains errors: {...}");
      expect(err.response.message).toBe("Query string contains errors: {...}");
      expect(err.response.args[0]).toBe("ReferenceError: variable is not defined");
      expect(err.response.statusCode).toBe(400);
    }
    expect(result).toBe(undefined)

  });



  afterAll(async () => {
    await allIsSaved();
    rimraf.sync(process.cwd() + "/test_data");
  });
});
