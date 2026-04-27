
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";
import BytePageView from "../src/PageViewArray";


describe("BytePageView", () => {

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
    let page = BytePageView.create(HEADER_ENTRY_STRUCTURE);
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
    expect(page.$getBuffer().byteLength).toBe(0x2000);
    expect(page.$capacityArray).toBe(372);
  });

  type ENTRY_KEY = "value" | "id";
  const ENTRY_STRUCTURE = new Map<ENTRY_KEY, number>([
    ["value", 8],
    ["id", 8],
  ]);

  test("shift", () => {
    let page = BytePageView.create(ENTRY_STRUCTURE, 34);

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

    expect(page.$capacityArray).toBe(509);
    expect(page.$canShiftRight(509)).toBe(false);
    expect(page.$canShiftRight(508)).toBe(true);
  });

  test("get set 16", () => {
    type key = "sortingKey" | "arg1" | "arg2";
    const struct = new Map<key, number>([
      ["sortingKey", 16],
      ["arg1", 2],
      ["arg2", 4],
    ]);
    let page = BytePageView.create(struct, 100);

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

  test("$getEntry", () => {
    type key = "id" | "some" | "user" | "data";
    let ar = BytePageView.create<key>(new Map([
      ["id", 8],
      ["some", 4],
      ["user", 4],
      ["data", 4],
    ]));

    let entry = ar.$getEntry(5);
    entry.id = 12345;
    entry.some = 1;
    entry.user = 2;
    entry.data = 3;

    expect(ar.id.get(5)).toBe(12345);
    expect(ar.some.get(5)).toBe(1);
    expect(ar.user.get(5)).toBe(2);
    expect(ar.data.get(5)).toBe(3);

    ar.id.set(10, 54321);
    ar.some.set(10, 1);
    ar.user.set(10, 2);
    ar.data.set(10, 3);

    ar.$getEntry(10);

    expect(entry.id).toBe(54321);
    expect(entry.some).toBe(1);
    expect(entry.user).toBe(2);
    expect(entry.data).toBe(3);

  });


});