import * as fs from "fs";
import * as path from "path";
import { IndexOneNumber } from "../src/IndexOneNumber";

import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";
import { Table } from "../src/table";
import { DataBase } from "../src/db";
import { rimrafSync } from "rimraf";
// import { ByteRecord } from "../src/ByteRecord";
const TestTableName = "jest_test_table_1";


describe("ByteRecord.$serialize", () => {
    const heapPath = path.join(process.cwd(), "kurgandb_data", "heap.bin");

    function removeTestData() {
        try { fs.unlinkSync(heapPath); } catch { }
        // try { fs.unlinkSync(heapPath + ".txt"); } catch { }
    }

    beforeAll(removeTestData);
    // afterAll(removeTestData);

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
        rimrafSync(process.cwd() + "/test_data");

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

        t.insert({
            bool: true,
            date: new Date(),
            name: "foo",
            light: ["foo"],
            heavy: null,
        });

    });

    test("$serialize for basic types", () => {


    });

});