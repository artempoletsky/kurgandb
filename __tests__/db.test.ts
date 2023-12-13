import { describe, expect, test } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject, rfs } from "../src/utils";
import { queryUnsafe } from "../src/api";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { SCHEME_PATH } from "../src/table";
import { SchemeFile } from "../src/db";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };



describe("Predicate parser", () => {

  test("parses predicates", () => {
    const q1 = predicateToQuery(({ }, { db, payload }) => { "hello world"; }, {});
    expect(q1.predicateBody).toBe('"hello world";')
    expect(q1.tables.length).toBe(0)
    const q2 = predicateToQuery(({ users }, { db, payload }) => { }, {});
    expect(q2.tables[0]).toBe("users");
    expect(q2.tables[1]).toBeUndefined();
  });
});

xdescribe("db", () => {
  const tableName = "jest_test_table";
  test("creates a table", async () => {
    const result = await clientQuery(({ }, { db, payload }) => {
      db.createTable({
        name: payload.tableName,
        fields: {
          test: "string"
        },
        indices: [],
      });
    }, {
      tableName
    });
    const scheme: SchemeFile = rfs(SCHEME_PATH);
    expect(scheme.tables).toHaveProperty(tableName);
    expect(scheme.tables.jest_test_table.fields.test).toBe("string");
  });

  test("removes a table", async () => {
    await clientQuery(({ }, { db, payload }) => {
      db.removeTable(payload.tableName);
    }, {
      tableName
    });

    const scheme: SchemeFile = rfs(SCHEME_PATH);
    expect(scheme.tables).not.toHaveProperty(tableName);
  });
});