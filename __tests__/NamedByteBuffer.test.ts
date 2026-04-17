
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";
import NamedByteBuffer, { calculateLength, createOffsetsConst } from "../src/BytePageView";


describe("NamedByteBuffer", () => {
  type HEADER_KEY =
    "numberofChunks"
    | "numberOfRecords"
    | "random1Byte"
    | "minValue"
    | "maxValue";

  const HEADER_STRUCTURE = new Map<HEADER_KEY, number>([
    ["numberofChunks", 2],
    ["numberOfRecords", 4],
    ["random1Byte", 1],
    ["minValue", 8],
    ["maxValue", 8],
  ]);


  test("utility functions", () => {
    let offsets = createOffsetsConst(HEADER_STRUCTURE);

    expect(offsets).toEqual({
      numberofChunks: 0,
      numberOfRecords: 2,
      random1Byte: 6,
      minValue: 7,
      maxValue: 15
    });

    let length = calculateLength(HEADER_STRUCTURE);
    expect(length).toBe(23);
  });

  test("header", () => {
    let header = NamedByteBuffer.createSuperblock(HEADER_STRUCTURE);

    header.numberofChunks = 123;
    header.numberOfRecords = 12345678;
    header.random1Byte = 42;
    header.minValue = -123;
    header.maxValue = 12345;

    expect(header.numberofChunks).toBe(123);
    expect(header.numberOfRecords).toBe(12345678);
    expect(header.random1Byte).toBe(42);
    expect(header.minValue).toBe(-123);
    expect(header.maxValue).toBe(12345);


    let b = header.$getBuffer();
    expect(b.readUint16LE(0)).toBe(123);
    expect(b.readUint32LE(2)).toBe(12345678);
    expect(b[6]).toBe(42);
    expect(b.readDoubleLE(7)).toBe(-123);
    expect(b.readDoubleLE(15)).toBe(12345);


    expect(header.$getBuffer().byteLength).toBe(23);
  });



  type HEADER_ENTRY_KEY =
    "page"
    | "length"
    | "minValue"
    | "maxValue";

  const HEADER_ENTRY_STRUCTURE = new Map<HEADER_ENTRY_KEY, number>([
    ["page", 2],
    ["length", 4],
    ["minValue", 8],
    ["maxValue", 8],
  ]);

  test("array", () => {
    let page = NamedByteBuffer.createArray(HEADER_ENTRY_STRUCTURE, 1000);
    page.length.set(0, 123);
    page.length.set(10, 321);
    page.minValue.set(0, -123);
    page.maxValue.set(0, 12345);

    expect(page.length.get(0)).toBe(123);
    expect(page.length.get(10)).toBe(321);


    expect(() => {
      page.length.set(-123, 0);
    }).toThrow();

    expect(page.$sizeEntry).toBe(22);
    expect(page.$getBuffer().byteLength).toBe(1000 * 22);
    expect(page.$capacityArray).toBe(1000);
  });

  type ENTRY_KEY = "value" | "id";
  const ENTRY_STRUCTURE = new Map<ENTRY_KEY, number>([
    ["value", 8],
    ["id", 8],
  ]);

  test("shift", () => {
    let page = NamedByteBuffer.createArray(ENTRY_STRUCTURE, 1000);

    for (let i = 0; i < 100; i++) {
      page.value.set(i, i * 10);
      page.id.set(i, i);
    }

    expect(page.value.get(11)).toBe(110);
    expect(page.id.get(11)).toBe(11);
    page.$shiftRight(100, 10);
    expect(page.value.get(10)).toBe(100);
    expect(page.value.get(11)).toBe(100);
    expect(page.id.get(10)).toBe(10);
    expect(page.id.get(11)).toBe(10);

    expect(page.value.get(100)).toBe(990);
    expect(page.id.get(100)).toBe(99);

    expect(page.value.get(101)).toBe(0);
    expect(page.id.get(101)).toBe(0);

    page.$shiftLeft(101, 5);

    expect(page.value.get(5)).toBe(60);
    expect(page.value.get(6)).toBe(70);

    expect(page.id.get(5)).toBe(6);
    expect(page.id.get(6)).toBe(7);

    expect(page.value.get(99)).toBe(990);
    expect(page.id.get(99)).toBe(99);

    expect(page.$canShiftRight(1000)).toBe(false);
    expect(page.$canShiftRight(999)).toBe(true);
  });

  test("get set 16", () => {
    type key = "sortingKey" | "arg1" | "arg2";
    const struct = new Map<key, number>([
      ["sortingKey", 16],
      ["arg1", 2],
      ["arg2", 4],
    ]);
    let page = NamedByteBuffer.createArray(struct, 100);

    let b = Buffer.alloc(16);
    b.write("aaaa aaaa a");
    expect(b.toString("utf-8").startsWith("aaaa aaaa a")).toBe(true);
    page.sortingKey.set16(0, b);

    let b2 = Buffer.alloc(16);
    page.sortingKey.get16(0, b2)
    expect(b2).toEqual(b);

    page.sortingKey.set16(1, Buffer.from("aaaa aaaa c"));

    let toFind = Buffer.alloc(16);
    toFind.write("aaaa aaaa b");

    let f = page.sortingKey.binarySearchSortKey(toFind, 2);
    expect(f.found).toBe(false);
    expect(f.pos).toBe(1);
    page.$shiftRight(2, 1);
    page.sortingKey.set16(1, toFind);

    f = page.sortingKey.binarySearchSortKey(toFind, 3);
    expect(f.found).toBe(true);
    expect(f.pos).toBe(1);

  });
});