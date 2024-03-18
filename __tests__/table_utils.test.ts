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
import TableUtils from "../src/table_utilities";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table_0";


describe("Table utils", () => {

  type SimpleType = {
    id: number,
    date: Date | string | number,
    bool: boolean,
    name: string,
    light: string[],
    heavy: null | {
      bar: number
    }
  }
  type SimpleInsert = Omit<SimpleType, "id">;
  let t: Table<SimpleType, number, any, SimpleInsert>;
  let utils: TableUtils<SimpleType, number>;
  beforeAll(() => {
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

    DataBase.init(process.cwd() + "/test_data");

    if (DataBase.doesTableExist(TestTableName)) {
      DataBase.removeTable(TestTableName);
    }
    t = DataBase.createTable<SimpleType, number>({
      name: TestTableName,
      fields: {
        id: "number",
        bool: "boolean",
        date: "date",
        name: "string",
        light: "json",
        heavy: "json",
      },
      tags: {
        id: ["primary", "autoinc"],
        name: ["index"],
        heavy: ["heavy"],
      }
    });

    utils = TableUtils.fromTable(t);
  });

  test("makeObjectStorable", () => {
    const storable = utils.makeObjectStorable({
      id: 1,
      name: "foo",
      bool: true,
      date: new Date(),
      light: ["1", "2", "3"],
      heavy: {
        bar: 123
      }
    });

    expect(storable.name).toBe("foo");
    expect(storable).toHaveProperty("heavy.bar");
    expect(storable.heavy.bar).toBe(123);

    expect(storable.bool).toBe(1);
    expect(typeof storable.date).toBe("number");
  });


  test("has tag functions", () => {
    expect(TableUtils.tagsHasFieldNameWithAnyTag(utils.scheme.tags, "name", "index", "unique")).toBe(true);
    expect(utils.fieldHasAnyTag("name", "index", "unique")).toBe(true);

    expect(utils.fieldHasAllTags("name", "index", "unique")).toBe(false);
    expect(utils.fieldHasAnyTag("name", "primary")).toBe(false);

  });

  afterAll(async () => {
    await allIsSaved();
    rimraf.sync(process.cwd() + "/test_data");
  });
});
