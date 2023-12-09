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
  let t: Table;
  beforeAll(() => {

    // DataBase.createTable({
    //   name: "users",
    //   "fields": {
    //     "name": "string",
    //     "isAdmin": "boolean",
    //     "isModerator": "boolean",
    //     "registrationDate": "date",
    //     "posts": "JSON",
    //     "password": "password",
    //     "blog_posts": "json"
    //   },
    //   "settings": {}
    // });
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
    t = new Table(tableName);
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

  });

  test("renames a field", () => {
    t.renameField("heavy", "foo");
    // console.log(t.scheme.fields);
    expect(t).not.toHaveProperty("scheme.fields.heavy");
    expect(t).toHaveProperty("scheme.fields.foo");
  });


  test("removes a field", () => {
    t.removeField("foo");
    expect(t.scheme.fields).not.toHaveProperty("foo");
    expect(t.at(0)?.name).toBe("foo");
  });

  test("adds a field", () => {
    t.addField("random", "number", e => Math.random());
    expect(t.scheme.fields).toHaveProperty("random");  
    expect(t.at(0)?.random).toBeLessThan(1);
  });

  afterAll(() => {
    t.closePartition();
    // DataBase.removeTable(tableName);
  });
});