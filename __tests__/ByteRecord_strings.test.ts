

import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";
import { readStrings, writeStrings } from "../src/ByteRecord";
// import { ByteRecord } from "../src/ByteRecord";
const TestTableName = "jest_test_table_1";


describe("ByteRecord; strings functions", () => {

  test("write read cycle pre 255", () => {
    let source = new Map([
      ["key1", { value: "hello", heapId: -1 }],
      ["key2", { value: "привет мир", heapId: -1 }],
      ["asdasd", { value: "fasdasdasdasd asd asd", heapId: -1 }],
    ]);
    let page = Buffer.alloc(0x2000);
    let tailEnd = writeStrings(page, source, 123, 126);
    expect(tailEnd).toBe(171);

    let target = readStrings(page, ["key1", "key2", "asdasd"], 123, 126);

    expect(target).toEqual(source);
  });

    test("write read cycle post 255", () => {
    let source = new Map([
      ["key1", { value: "hello", heapId: -1 }],
      ["key2", { value: "привет мир", heapId: -1 }],
      ["asdasd", { value: "fasdasdasdasd asd asd".repeat(100), heapId: -1 }],
    ]);
    let page = Buffer.alloc(0x2000);
    let tailEnd = writeStrings(page, source, 123, 126);
    expect(tailEnd).toBe(154);

    let target = readStrings(page, ["key1", "key2", "asdasd"], 123, 126);

    expect(target).toEqual(source);
  });

});