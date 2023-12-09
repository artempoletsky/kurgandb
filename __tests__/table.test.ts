import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject } from "../src/utils";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { before } from "node:test";
import { DataBase } from "../src/db";
import { Table } from "../src/table";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };


describe("Table", () => {
  const tableName = "jest_test_table";

  beforeAll(() => {
    if (DataBase.isTableExist(tableName)) {
      DataBase.removeTable(tableName);
    }
    DataBase.createTable({
      name: tableName,
      fields: {
        name: "string",
        // light: "json",
        heavy: "JSON",
      }
    });
  });

  test("adds a document", () => {

    const t = new Table(tableName);
    const idOffset = t.getLastIndex();
    const lenghtOffset = t.length;

    const d1 = t.insert({
      name: "foo",
      heavy: {
        bar: Math.random()
      }
    });

    const d2 = t.insert({
      name: "bar",
      heavy: {
        bar: Math.random()
      }
    });

    
    expect(t.length).toBe(lenghtOffset + 2);
    expect(d1.id).toBe(idOffset + 0);
    expect(d2.id).toBe(idOffset + 1);

    expect(t.getDocumentData(d1.getStringID())[0]).toBe("foo");
    expect(t.getDocumentData(d2.getStringID())[0]).toBe("bar");
    t.closePartition();
  });

  test("renames a field", () => {

    // const t = new Table(tableName);
    // t.renameField("heavy", "foo");
  });

  afterAll(() => {
    // DataBase.removeTable(tableName);
  });
});