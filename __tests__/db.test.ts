import { describe, expect, test } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { rfs } from "../src/utils";
import { queryUnsafe } from "../src/api";
import { standAloneQuery as query } from "../src/client";
import { SchemeFile, SCHEME_PATH, DataBase } from "../src/db";
import { existsSync } from "fs";
import { rimraf } from "rimraf";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

DataBase.init(process.cwd() + "/test_data");

describe("Predicate parser", () => {

  test("parses predicates", () => {
    const q1 = predicateToQuery<any, any>(({ }, { db, payload }) => { "hello world"; }, {});
    expect(q1.predicateBody).toBe('"hello world";')
    expect(q1.tables.length).toBe(0)
    const q2 = predicateToQuery<any, any>(({ users }, { db, payload }) => { }, {});
    expect(q2.tables[0]).toBe("users");
    expect(q2.tables[1]).toBeUndefined();
  });
});

describe("db", () => {
  const tableName = "jest_test_table";
  const expectedDir = process.cwd() + "/test_data/" + tableName;
  beforeAll(() => {
    rimraf.sync(expectedDir);
  });

  test("creates a table", async () => {
    const result = await query(({ }, { db, payload }) => {
      db.createTable({
        name: payload.tableName,
        fields: {
          test: "string"
        },
      });
    }, {
      tableName
    });
    const scheme: SchemeFile = rfs(SCHEME_PATH);
    expect(scheme.tables).toHaveProperty(tableName);
    expect(scheme.tables.jest_test_table.fields.test).toBe("string");
    expect(existsSync(expectedDir)).toBe(true);
  });

  test("removes a table", async () => {
    await query(({ }, { db, payload }) => {
      db.removeTable(payload.tableName);
    }, {
      tableName
    });

    const scheme: SchemeFile = rfs(SCHEME_PATH);
    expect(scheme.tables).not.toHaveProperty(tableName);

    expect(existsSync(expectedDir)).toBe(false);
  });
});