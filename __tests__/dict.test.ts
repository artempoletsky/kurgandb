import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog, rfs, existsSync, wfs } from "../src/utils";
import FragmentedDictionary, { FragDictMeta, PartitionMeta } from "../src/fragmented_dictionary";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };




describe("Fragmented dictionary", () => {
  let numbers: FragmentedDictionary<number>;
  let letters: FragmentedDictionary<number>;
  let partitions: FragmentedDictionary<number>;
  const PART = "/data/jest_dict_partititions/";
  const LETT = "/data/jest_dict_letters/";
  const NUM = "/data/jest_dict_numbers/";
  beforeAll(() => {
    numbers = FragmentedDictionary.init({
      directory: NUM,
      keyType: "int",
      maxPartitionLenght: 3,
    });

    partitions = FragmentedDictionary.init({
      directory: PART,
    });


    letters = FragmentedDictionary.init({
      directory: LETT,
      keyType: "string",
      maxPartitionLenght: 3,
    });
  });

  test("Partition ID search", () => {
    const partitions: PartitionMeta[] = [];
    const length = 1000 * 1000 * 1000;
    const dictMeta: FragDictMeta = {
      length,
      partitions,
      start: 0,
    };

    const partitionSize = 100 * 1000;
    const numPartitions = length / partitionSize;
    let end = partitionSize;
    for (let i = 0; i < numPartitions; i++) {
      partitions.push({
        length: partitionSize,
        end,
      });
      end += partitionSize;
    }

    const id = Math.floor(Math.random() * length);

    perfStart("partition search");
    const partitionId = FragmentedDictionary.findPartitionForId(id, dictMeta);
    perfEnd("partition search");

    // perfLog("partition search")
    expect(partitionId).not.toBe(false);
    expect(perfDur("partition search")).toBeLessThan(20);
  });

  test("Partition ID search 2", () => {
    const dictMeta: FragDictMeta = {
      length: 9,
      partitions: [{ end: "c", length: 3 }, { end: "f", length: 3 }, { end: "z", length: 3 }],
      start: "a",
    };
    let id = FragmentedDictionary.findPartitionForId("d", dictMeta);
    expect(id).toBe(1);
    id = FragmentedDictionary.findPartitionForId("p", dictMeta);
    expect(id).toBe(2);
    id = FragmentedDictionary.findPartitionForId("b", dictMeta);
    expect(id).toBe(0);
    id = FragmentedDictionary.findPartitionForId("aaaa", dictMeta);
    expect(id).toBe(0);
    id = FragmentedDictionary.findPartitionForId("zzz", dictMeta);
    expect(id).toBe(2);
  });

  test("creates partitions", async () => {
    expect(partitions.numPartitions).toBe(0);
    expect(existsSync(PART + "part0.json")).toBe(false);
    partitions.createNewPartition(0);
    expect(existsSync(PART + "part0.json")).toBe(true);

    expect(existsSync(PART + "part1.json")).toBe(false);
    partitions.createNewPartition(1);
    expect(existsSync(PART + "part1.json")).toBe(true);
    expect(partitions.numPartitions).toBe(2);

    wfs(PART + "part1.json", { hello_test: "test" });
    expect(partitions.openPartition(1).hello_test).toBe("test");

    partitions.createNewPartition(0);
    expect(existsSync(PART + "part2.json")).toBe(true);
    expect(partitions.numPartitions).toBe(3);
    expect(partitions.openPartition(1).hello_test).toBe(undefined);
    expect(partitions.openPartition(2).hello_test).toBe("test");

    partitions.createNewPartition(partitions.numPartitions);
    expect(existsSync(PART + "part3.json")).toBe(true);
    expect(existsSync(PART + "part4.json")).toBe(false);
    expect(partitions.numPartitions).toBe(4);
  });

  test("adds items at the tail", async () => {
    numbers.insertArray([1]);
    expect(numbers.lenght).toBe(1);
    expect(numbers.lastID).toBe(1)
    expect(numbers.meta.start).toBe(1)

    numbers.insertArray([2, 3, 4]);
    expect(numbers.lenght).toBe(4);

    let p1 = numbers.openPartition(1);
    expect(p1[4]).toBe(4);

    expect(numbers.numPartitions).toBe(2);
    expect(numbers.lastID).toBe(4);
  });


  test("adds items at the center", async () => {
    const initial = ["a", "b", "c", "x", "y", "z",];
    const getLetterPosition = (k: string) => (parseInt(k, 36) - 10 + 1);

    expect(getLetterPosition("a")).toBe(1);
    expect(getLetterPosition("c")).toBe(3);

    letters.insertMany(initial, initial.map(getLetterPosition));

    expect(letters.numPartitions).toBe(2);
    expect(letters.openPartition(1).z).toBe(26);
    expect(letters.openPartition(0).a).toBe(1);
    expect(letters.meta.start).toBe("a");
    expect(letters.lastID).toBe("z");

    const ptk = ["p"].sort();
    letters.insertMany(ptk, ptk.map(getLetterPosition));

    expect(letters.numPartitions).toBe(3);
    expect(letters.openPartition(1).p).toBe(16);
  });

  afterAll(() => {
    letters.destroy();
    numbers.destroy();
    partitions.destroy();
  })
});