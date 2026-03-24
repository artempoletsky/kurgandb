import { describe, expect, test, xdescribe, xtest } from "./test-setup";

// src/server.ts

import * as wasm from "../src/wasm";

describe("wasm", () => {
    
    test("hello world", () => {
        
        expect(wasm.add(2, 3)).toBe(5);
    });


    test("compareStrings", () => {
        
        expect(wasm.compareStrings("11", "12")).toBe(1);
        expect(wasm.compareStrings("11", "10")).toBe(-1);
        expect(wasm.compareStrings("11", "11")).toBe(0);
    });
});

