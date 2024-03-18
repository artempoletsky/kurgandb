import { describe, expect, test } from "@jest/globals";

import { standAloneQuery as query } from "../src/client";
import { DataBase } from "../src/db";
import { $ } from "../src/utils";
import { allIsSaved } from "../src/virtual_fs";
import { writeIntoLogFile } from "../src/utils";
import { Table } from "../src/globals";
import { parseFunction } from "../src/function";
import { ZodRawShape, z } from "zod";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };


describe("Utility functions", () => {

  const tableName = "test_utils_table";
  type TestType = {
    id: string
    name: string
    password: string
  };

  beforeAll(() => {
    DataBase.init(process.cwd() + "/test_data");

    const t = DataBase.createTable<TestType, string>({
      name: tableName,
      fields: {
        id: "string",
        name: "string",
        password: "string",
      },
      tags: {
        id: ["primary"],
        password: ["hidden"],
      }
    });

    t.insert({
      id: "1",
      name: "foo",
      password: "qwerty",
    });
  });


  test("randomIndex", () => {
    let rand = $.randomIndex(10);
    expect(rand).toBeLessThan(10);
    expect(typeof rand).toBe("number");

    // rand = $.randomIndex(3, new Set([0, 1]));
    expect($.randomIndex(3, new Set([1, 2]))).toBe(0);
    expect($.randomIndex(3, new Set([0, 2]))).toBe(1);
    expect($.randomIndex(3, new Set([0, 1]))).toBe(2);
    expect($.randomIndex(3, new Set([0]))).not.toBe(0);
    expect($.randomIndex(3, new Set([1]))).not.toBe(1);
    expect($.randomIndex(3, new Set([2]))).not.toBe(2);

    rand = $.randomIndex(3, new Set([1, 2, 3]));
    expect(rand).toBeLessThan(3)
  });

  test("randomIndices", () => {
    let rand = $.randomIndices(10, 3);
    expect(rand.length).toBe(3);
    rand = $.randomIndices(10, 10);
    expect(rand.length).toBe(10);

    // console.log(rand);
  });

  test("primary", async () => {
    const id = await query(({ test_utils_table }: { test_utils_table: Table<TestType, string> }, { }, { $ }) => {
      return test_utils_table.at("1", $.primary);
    }, {});

    expect(id).toBe("1");
  });

  xtest("log", async () => {
    writeIntoLogFile("Hello log", `asdasd asd
    asdlaskdasd
    asdkalsjdaksd
    asdjkasldj
    asdklasjdl;asd
    `);
  });

  afterAll(async () => {
    await allIsSaved();
    DataBase.removeTable(tableName);
    await allIsSaved();
  });
});

describe("Function parser/constructor", () => {

  test("no arguments", () => {
    let p = parseFunction(() => { });
    expect(p.args.length).toBe(0);
  });

  test("parse", () => {
    let p = parseFunction((self: any) => {
      const shape: ZodRawShape = {}
      for (const fieldName in self.scheme.fields) {
        const type = self.scheme.fields[fieldName];
        let rule: any;

        switch (type) {
          case "boolean": rule = z.boolean(); break;
          case "date": rule = z.union([z.date(), z.number(), z.string()]); break;
          case "json": rule = z.any(); break;
          case "number": rule = z.number(); break;
          case "string": rule = z.string(); break;
        }

        shape[fieldName] = rule;
      }

      return z.object(shape);
    });

    expect(p.args.length).toBe(1);
    expect(p.args[0]).toBe("self");


    p = parseFunction((self: any, { z }: any) => {
      const shape: ZodRawShape = {}
      for (const fieldName in self.scheme.fields) {
        const type = self.scheme.fields[fieldName];
        let rule: any;

        switch (type) {
          case "boolean": rule = z.boolean(); break;
          case "date": rule = z.date(); break;
          case "json": rule = z.any(); break;
          case "number": rule = z.number(); break;
          case "string": rule = z.string(); break;
        }

        shape[fieldName] = rule;
      }

      return z.object(shape);
    });

    expect(p.args.length).toBe(2);
    expect(p.args[0]).toBe("self");
    expect(p.args[1]).toBe("{ z }");
  })
})