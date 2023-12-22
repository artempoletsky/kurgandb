import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog, rfs, existsSync, wfs } from "../src/utils";
import FragmentedDictionary, { FragDictMeta, PartitionMeta } from "../src/fragmented_dictionary";
import SortedDictionary from "../src/sorted_dictionary";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };




describe("Fragmented dictionary", () => {
  let numbers: FragmentedDictionary<number, number>;
  let letters: FragmentedDictionary<string, number>;
  let partitions: FragmentedDictionary<number, number>;
  let index: FragmentedDictionary<string, number[]>;
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
      keyType: "int"
    });


    letters = FragmentedDictionary.init({
      directory: LETT,
      keyType: "string",
      maxPartitionLenght: 3,
    });

    index = FragmentedDictionary.init({
      keyType: "string",
      directory: "/data/test_array_index/",
      maxPartitionSize: 0,
      maxPartitionLenght: 100 * 1000,
    });
  });

  test("Partition ID search", () => {
    const partitions: PartitionMeta<number>[] = [];
    const length = 1000 * 1000 * 1000;
    const dictMeta: FragDictMeta<number> = {
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
    const dictMeta: FragDictMeta<string> = {
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

  test("inserts sorted dict", () => {
    numbers = FragmentedDictionary.reset(numbers);

    numbers.insertSortedDict(SortedDictionary.fromLenght(15));
    expect(numbers.lenght).toBe(15);
    expect(numbers.start).toBe(1);
    expect(numbers.end).toBe(15);
    expect(numbers.getOne(15)).toBe(15);
    expect(numbers.getOne(7)).toBe(7);
    expect(numbers.getOne(1)).toBe(1);

  });

  test("splits partitions", () => {
    const initial = ["b", "c", "x", "y", "z",];
    letters.insertMany(initial, initial.map(l => Math.random()));

    letters.splitPartition(0, "p");
    let p0 = letters.openPartition(0);
    let p1 = letters.openPartition(1);
    expect(p0.b).toBeDefined();
    expect(p0.c).toBeDefined();
    expect(p0.x).toBeUndefined();
    expect(p1.b).toBeUndefined();
    expect(p1.c).toBeUndefined();
    expect(p1.x).toBeDefined();
    let m0 = letters.meta.partitions[0];
    let m1 = letters.meta.partitions[1];
    expect(m0.length).toBe(2);
    expect(m0.end).toBe("c");
    expect(m1.length).toBe(1);
    expect(m1.end).toBe("x");

    letters = FragmentedDictionary.reset(letters);
  });

  test("adds items at the tail", async () => {
    numbers = FragmentedDictionary.reset(numbers);
    numbers.insertArray([1]);
    expect(numbers.lenght).toBe(1);
    expect(numbers.end).toBe(1)
    expect(numbers.meta.start).toBe(1)

    numbers.insertArray([2, 3, 4]);
    expect(numbers.lenght).toBe(4);

    let p1 = numbers.openPartition(1);
    expect(p1[4]).toBe(4);

    expect(numbers.numPartitions).toBe(2);
    expect(numbers.end).toBe(4);
  });


  test("adds items at the center", async () => {
    const initial = ["b", "c", "x", "y", "z",];
    const getLetterPosition = (k: string) => (parseInt(k, 36) - 10 + 1);

    expect(getLetterPosition("a")).toBe(1);
    expect(getLetterPosition("c")).toBe(3);


    letters.insertMany(initial, initial.map(getLetterPosition));
    expect(letters.start).toBe("b");
    expect(letters.end).toBe("z");
    expect(letters.meta.partitions[0].end).toBe("x");
    expect(letters.meta.partitions[0].length).toBe(3);
    expect(letters.meta.partitions[1].end).toBe("z");
    expect(letters.meta.partitions[1].length).toBe(2);
    expect(letters.lenght).toBe(5);

    expect(letters.findPartitionForId("z")).toBe(1);
    expect(letters.findPartitionForId("b")).toBe(0);

    expect(letters.getOne("z")).toBe(26);
    expect(letters.getOne("b")).toBe(2);


    // expect(letters.findPartitionForId("p")).toBe(0);
    const ptk = ["p"].sort();
    letters.insertMany(ptk, ptk.map(getLetterPosition));


    expect(letters.openPartition(1)).toHaveProperty("p");
    expect(letters.numPartitions).toBe(3);
    expect(letters.getOne("p")).toBe(16);


    letters.insertMany({
      a: getLetterPosition("a")
    });
    // console.log(letters.openPartition(0));

    expect(letters.start).toBe("a");
    expect(letters.meta.partitions[0].end).toBe("c");
    // console.log(letters.meta.partitions);

    expect(letters.numPartitions).toBe(3);
    expect(letters.findPartitionForId("b")).toBe(0);

  });


  test("edits items", async () => {
    letters.edit(["b", "a"], (id, value) => value * 10);
    // console.log(letters.openPartition(0));
    expect(letters.findPartitionForId("a")).toBe(0)
    expect(letters.findPartitionForId("b")).toBe(0)
    expect(letters.getOne("a")).toBe(10);
    expect(letters.getOne("b")).toBe(20);
  });

  test("removes items", async () => {
    letters.remove(["b", "a"]);
    expect(letters.getOne("a")).toBe(undefined);
    expect(letters.getOne("b")).toBe(undefined);
  });

  test("insert array", async () => {
    numbers = FragmentedDictionary.reset(numbers);
    const ids = numbers.insertArray([5, 6, 7]);
    expect(numbers.lenght).toBe(3);
    expect(ids[0]).toBe(1);
    expect(ids[1]).toBe(2);
    expect(ids[2]).toBe(3);
    expect(numbers.getOne(1)).toBe(5);
    expect(numbers.getOne(2)).toBe(6);
    expect(numbers.getOne(3)).toBe(7);
  });


  test("insert one", async () => {
    numbers = FragmentedDictionary.reset(numbers);
    numbers.insertOne(123, 321);
    expect(numbers.getOne(123)).toBe(321);
  });

  test("array dict", async () => {

    index.insertOne("123", [123]);
    index.insertOne("1234", [12312, 1421354, 54234]);

    const arr = index.getOne("123");
    expect(arr).toBeDefined();
    if (!arr) return;
    expect(arr[0]).toBe(123);

    index.editRanges([[undefined, undefined]], (id, arr) => {
      arr.splice(1, 0, 1000);
      return [...arr];
    });

    // console.log(index.openPartition(0));

    expect((index.getOne("123") as any)[1]).toBe(1000);

  });

  afterAll(() => {
    index.destroy();
    letters.destroy();
    numbers.destroy();
    partitions.destroy();
  })
});