import { describe, expect, test } from "@jest/globals";

import { $ } from "../src/utils";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };


describe("Utility functions", () => {

  test("randomIndex", () => {
    let rand = $.randomIndex(10);
    expect(rand).toBeLessThan(10);
    expect(typeof rand).toBe("number");

    // rand = $.randomIndex(3, new Set([0, 1]));
    expect($.randomIndex(3, new Set([1, 2]))).toBe(0);
    expect($.randomIndex(3, new Set([0, 2]))).toBe(1);
    expect($.randomIndex(3, new Set([0, 1]))).toBe(2);
    expect($.randomIndex(3, new Set([0]))).not.toBe(0);
    expect($.randomIndex(3, new Set([1]))).not.toBe(1);
    expect($.randomIndex(3, new Set([2]))).not.toBe(2);

    rand = $.randomIndex(3, new Set([1, 2, 3]));
    expect(rand).toBeLessThan(3)
  });

  test("randomIndices", () => {
    let rand = $.randomIndices(10, 3);
    expect(rand.length).toBe(3);
    rand = $.randomIndices(10, 10);
    expect(rand.length).toBe(10);

    // console.log(rand);
  });
});
