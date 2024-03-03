import { describe, expect, test } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { rfs } from "../src/utils";
import { standAloneQuery as query } from "../src/client";
import { SchemeFile, SCHEME_PATH, DataBase } from "../src/db";
import { existsSync } from "fs";
import { rimraf } from "rimraf";
import { allIsSaved } from "../src/virtual_fs";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

DataBase.init(process.cwd() + "/test_data");

describe("Predicate parser", () => {

  test("parses predicates", () => {
    const q1 = predicateToQuery<any, any, void>((tables, { payloadArg }, { db }) => { "hello world"; });

    expect(q1.predicateBody).toBe('"hello world";');
    expect(q1.predicateArgs.length).toBe(3);
    expect(q1.predicateArgs[0]).toBe("tables");
    expect(q1.predicateArgs[1]).toBe("{ payloadArg }");
    expect(q1.predicateArgs[2]).toBe("{ db }");

    const q2 = predicateToQuery<any, any, void>(function ({ users, posts }) { });
    expect(q2.predicateArgs[0]).toBe("{ users, posts }");
    expect(q2.predicateArgs[1]).toBe(undefined);


    const q3 = predicateToQuery<any, any, void>(async (tables, payload, { db }) => {
      await db;
    });
    expect(q3.isAsync).toBe(true);


    const q4 = predicateToQuery<any, any, any>(({ }, { }, { db: e }) => Object.keys(e.getTables()));
    expect(q4.isAsync).toBe(false);
    expect(q4.predicateArgs[0]).toBe("{}");
    expect(q4.predicateArgs[1]).toBe("{}");
    expect(q4.predicateArgs[2]).toBe("{ db: e }");
    expect(q4.predicateBody).toBe("return Object.keys(e.getTables())");

    const constructorArgs = [...q4.predicateArgs, q4.predicateBody];
    const fn = new Function(...constructorArgs);

    expect(fn({}, {}, {
      db: DataBase
    })).toBeDefined();
  });
});

describe("db", () => {
  const tableName = "jest_test_table_1";
  const expectedDir = process.cwd() + "/test_data/" + tableName;
  beforeAll(async () => {
    rimraf.sync(expectedDir);
    await allIsSaved();
  });

  test("creates a table", async () => {
    const result = await query(({ }, { tableName }, { db }) => {
      db.createTable({
        name: tableName,
        fields: {
          test: "string"
        },
      });
    }, { tableName });
    const scheme: SchemeFile = rfs(SCHEME_PATH);
    expect(scheme.tables).toHaveProperty(tableName);
    expect(scheme.tables[tableName].fields.test).toBe("string");
    expect(existsSync(expectedDir)).toBe(true);
    await allIsSaved();
  });

  test("removes a table", async () => {
    await query(({ }, { tableName }, { db }) => {
      db.removeTable(tableName);
    }, { tableName });

    const scheme: SchemeFile = rfs(SCHEME_PATH);
    expect(scheme.tables).not.toHaveProperty(tableName);

    expect(existsSync(expectedDir)).toBe(false);
    await allIsSaved();
  });
});