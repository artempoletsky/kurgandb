import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import SortedDictionary from "../src/sorted_dictionary";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };


describe("Sorted dictionary", () => {

  test("from keys uniqueness check", () => {
    const lett1 = SortedDictionary.fromKeys<string>(["b", "c", "x", "y", "x"]);
    expect(lett1.length).toBe(4);
    expect(lett1.lastKey).toBe("y");
    expect(lett1.get("x")).toBe("x");
  });

  test("splits", () => {
    const even1 = SortedDictionary.fromLenght(6);
    const even2 = even1.splitByKey(4);

    expect(even1.length).toBe(3);
    expect(even1.firstKey).toBe(1);
    expect(even1.lastKey).toBe(3);

    expect(even2.length).toBe(3);
    expect(even2.firstKey).toBe(4);
    expect(even2.lastKey).toBe(6);

    const odd1 = SortedDictionary.fromLenght(7);
    const odd2 = odd1.splitByKey(4);

    expect(odd1.length).toBe(3);
    expect(odd1.firstKey).toBe(1);
    expect(odd1.lastKey).toBe(3);

    expect(odd2.length).toBe(4);
    expect(odd2.firstKey).toBe(4);
    expect(odd2.lastKey).toBe(7);

    const lett1 = SortedDictionary.fromKeys<string>(["b", "c", "x", "y", "z"]);

    const lett2 = lett1.splitByKey("p");

    expect(lett1.length).toBe(2);
    expect(lett1.firstKey).toBe("b");
    expect(lett1.lastKey).toBe("c");

    expect(lett2.length).toBe(3);
    expect(lett2.firstKey).toBe("x");
    expect(lett2.lastKey).toBe("z");
    expect(lett2.get("y")).toBe("y");
  });

  test("transform", () => {
    const letters = SortedDictionary.fromLenght(26).transform(index => (index + 9).toString(36));

    expect(letters.get(1)).toBe("a");
    expect(letters.get(18)).toBe("r");
    expect(letters.get(20)).toBe("t");
    expect(letters.get(5)).toBe("e");
    expect(letters.get(13)).toBe("m");

    expect(letters.get(26)).toBe("z");

    const lettersReverse = SortedDictionary.fromKeys(letters.values()).transform(letter => parseInt(letter, 36) - 9);

    expect(lettersReverse.get("a")).toBe(1);
    expect(lettersReverse.get("r")).toBe(18);
    expect(lettersReverse.get("t")).toBe(20);
    expect(lettersReverse.get("e")).toBe(5);
    expect(lettersReverse.get("m")).toBe(13);

    expect(lettersReverse.get("z")).toBe(26);

  });

  test("drain", () => {
    const d1 = SortedDictionary.fromLenght(20);
    const d2 = SortedDictionary.fromArray(d1.values(), 20 + 1).transform(key => key);

    expect(d2.get(21)).toBe(21);
    expect(d2.get(40)).toBe(40);
    expect(d2.length).toBe(20);

    d1.drain(d2);
    expect(d1.length).toBe(40);
    expect(d1.get(35)).toBe(35);
    expect(d1.firstKey).toBe(1);

    expect(d1.lastKey).toBe(40);

    expect(d2.length).toBe(0);
  });
});