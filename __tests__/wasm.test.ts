import { describe, expect, test, xdescribe, xtest } from "./test-setup";

// src/server.ts
import fs from "fs";
import loader from "@assemblyscript/loader";


describe("wasm", () => {
    test("hello world", () => {
        const wasmModule: any = loader.instantiateSync(fs.readFileSync("./wasmbuild/release.wasm"), {});
        console.log("2 + 3 =", wasmModule.exports.add(2, 3));

        expect(wasmModule.exports.add(2, 3)).toBe(5);
    });
});

