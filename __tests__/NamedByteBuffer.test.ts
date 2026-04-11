
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";
import NamedByteBuffer, { calculateLength, createOffsetsConst } from "../src/NamedByteBuffer";


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
    let header = NamedByteBuffer.createHeader(HEADER_STRUCTURE);

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
});