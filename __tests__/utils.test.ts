import { describe, expect, test } from "@jest/globals";

import { standAloneQuery as query } from "../src/client";
import { DataBase } from "../src/db";
import { $ } from "../src/utils";
import { allIsSaved } from "../src/virtual_fs";
import { writeIntoLogFile } from "../src/utils";
import { Table } from "../src/globals";

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
