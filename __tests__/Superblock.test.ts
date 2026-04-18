
import Superblock, { calculateLength, createOffsetsConst } from "../src/Superblock";
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";


describe("Superblock", () => {
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
    let header = Superblock.create(HEADER_STRUCTURE);

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

  test("write read page", () => {
    let p = Buffer.alloc(0x2000);
    let sb = Superblock.create(HEADER_STRUCTURE);

    sb.numberofChunks = 123;
    sb.numberOfRecords = 12345678;
    sb.random1Byte = 42;
    sb.minValue = -123;
    sb.maxValue = 12345;

    sb.$writeToPage(p);

    // expect(b.readDoubleLE(15)).toBe(12345);
    expect(p.readDoubleLE(0x2000 - 8)).toBe(12345);

    let sb2 = Superblock.create(HEADER_STRUCTURE);
    sb2.$readFromPage(p);

    expect(sb2.numberofChunks).toBe(123);
    expect(sb2.numberOfRecords).toBe(12345678);
    expect(sb2.random1Byte).toBe(42);
    expect(sb2.minValue).toBe(-123);
    expect(sb2.maxValue).toBe(12345);
  });

  type key16 = "sortingKey1" | "sortingKey2" | "arg1" | "arg2";
  const struct16 = new Map<key16, number>([
    ["sortingKey1", 16],
    ["sortingKey2", 16],
    ["arg1", 2],
    ["arg2", 4],
  ]);

  test("get set 16", () => {

    let sb = Superblock.create(struct16);

    let b = Buffer.alloc(16);
    b.write("aaaa aaaa a");

    let b1 = Buffer.from(b);

    sb.$set16(b, "sortingKey2");
    sb.$get16(b, "sortingKey1");
    expect(b).not.toEqual(b1);
    sb.$get16(b, "sortingKey2");
    expect(b).toEqual(b1);
  });



  test("compare 16", () => {
    let sb = Superblock.create(struct16);

    let a = Buffer.alloc(16);
    a.write("aaaa aaaa a");

    let b = Buffer.alloc(16);
    b.write("aaaa aaaa b");

    sb.$set16(a, "sortingKey2");

    expect(sb.$compare16(b, "sortingKey2")).toEqual(a.compare(b));
  });


});