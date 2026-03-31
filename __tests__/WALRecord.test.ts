import * as fs from "fs";
import * as path from "path";

import { describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";
import FilePatchRecord from "../src/FilePatchRecord";


describe("FilePatchRecord", () => {

    const cwd = "/kurgandb_data/patchtest/"

    if (!fs.existsSync(process.cwd() + cwd))
        fs.mkdirSync(process.cwd() + cwd);


    let r = new FilePatchRecord({
        pathPage: cwd + "page.bin",
        sizePage: 0x2000,
        pathHeap: cwd + "heap.bin",
    });


    test("read write commit page", () => {

        r.writePage(5, Buffer.from("test"));

        let readBuff = Buffer.allocUnsafe(r.pageSize);
        r.readPage(5, readBuff);
        expect(readBuff.toString().startsWith("test")).toBe(true);
        r.commit();
        r.readPage(5, readBuff);
        expect(readBuff.toString().startsWith("test")).toBe(true);
        // const br = new ByteRecord()
    });

    test("read write commit heap", () => {

        r.writeHeap(Buffer.from("heap test"), 0xFFFD);

        let readBuff = Buffer.allocUnsafe(0xFFFD);
        r.readPage(5, readBuff);
        expect(readBuff.toString().startsWith("test")).toBe(true);
        r.commit();
        r.readPage(5, readBuff);
        expect(readBuff.toString().startsWith("test")).toBe(true);
        // const br = new ByteRecord()
    });

});