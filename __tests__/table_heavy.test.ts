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

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table_0";

describe("Table heavy", () => {

  type Heavy = {
    id: number;
    heavyStr: string;
    heavyJson: string;
  };
  type HeavyInsert = Omit<Heavy, "id">;
  let t: Table<Heavy, number, any, HeavyInsert>;
  beforeAll(() => {

    DataBase.init(process.cwd() + "/test_data");

    if (DataBase.doesTableExist(TestTableName)) {
      DataBase.removeTable(TestTableName);
    }
    t = DataBase.createTable<Heavy, number>({
      name: TestTableName,
      fields: {
        id: "number",
        heavyStr: "string",
        heavyJson: "json",
      },
      tags: {
        id: ["primary", "autoinc"],
        heavyStr: ["heavy"],
        heavyJson: ["heavy"],
      }
    });

  });

  test("insert string", () => {
    const id = t.insert({
      heavyStr: "foo",
      heavyJson: "foo",
    });
    const rec = t.at(id);
    expect(rec.heavyStr).toBe("foo");
    expect(rec.heavyJson).toBe("foo");
  });

  afterAll(async () => {
    await allIsSaved();
    rimraf.sync(process.cwd() + "/test_data");
  });
});
