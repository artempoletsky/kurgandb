import * as fs from "fs";
import * as path from "path";

import { describe, expect, test, xdescribe, xtest } from "./test-setup";
import _ from "lodash";
import FilePatchRecord from "../src/FilePatchRecord";
import { rimraf } from "rimraf";


describe("FilePatchRecord", () => {

    const cwd = "/kurgandb_data/_patchtest/"

    rimraf.sync(process.cwd() + cwd);
    fs.mkdirSync(process.cwd() + cwd, { recursive: true });


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

        let data = "heap test";
        let meta = r.writeHeap(Buffer.from(data), 0xFFFD);
        expect(meta.sizeCurrent).toBe(Buffer.byteLength(data));
        expect(meta.sizeMax).toBe(0xFFFD);
        expect(meta.offsetHeap).toBe(0);
        expect(meta.offsetWal).toBe(0);

        let readBuff = r.readHeap(0, Buffer.byteLength(data));
        expect(readBuff.toString()).toBe(data);

        r.commit();
        readBuff = r.readHeap(0, Buffer.byteLength(data));
        expect(readBuff.toString()).toBe(data);

        data = "heap test2";
        meta = r.writeHeap(Buffer.from(data), 0xFFFD);
        expect(meta.sizeCurrent).toBe(Buffer.byteLength(data));
        expect(meta.sizeMax).toBe(0xFFFD);
        expect(meta.offsetHeap).toBe(0xFFFD);
        expect(meta.offsetWal).toBe(0);

        readBuff = r.readHeap(0xFFFD, Buffer.byteLength(data));

        expect(readBuff.toString()).toBe(data);

        r.commit();
        readBuff = r.readHeap(0xFFFD, Buffer.byteLength(data));
        expect(readBuff.toString()).toBe("heap test2");

        readBuff = r.readHeap(0, Buffer.byteLength("heap test"));
        
        expect(readBuff.toString()).toBe("heap test");

        meta = r.writeHeap(Buffer.from("3heap test3"), 0xFFFF, 0);

        readBuff = r.readHeap(0xFFFD, Buffer.byteLength("heap test2"));
        expect(readBuff.toString()).toBe("heap test2");

        readBuff = r.readHeap(0, Buffer.byteLength("heap test"));
        expect(readBuff.toString()).toBe("heap test");

        expect(meta.sizeCurrent).toBe(Buffer.byteLength("3heap test3"));
        expect(meta.sizeMax).toBe(0xFFFF);
        expect(meta.offsetHeap).toBe(0);
        expect(meta.offsetWal).toBe(0);

    });

});