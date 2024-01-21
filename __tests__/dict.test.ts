import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import FragmentedDictionary, { FragDictMeta, PartitionMeta } from "../src/fragmented_dictionary";
import SortedDictionary from "../src/sorted_dictionary";
import vfs, { allIsSaved, rmie, existsSync } from "../src/virtual_fs";
import virtual_fs from "../src/virtual_fs";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };




describe("Fragmented dictionary", () => {
  let numbers: FragmentedDictionary<number, number>;
  let letters: FragmentedDictionary<string, number>;
  let partitions: FragmentedDictionary<number, number>;
  let index: FragmentedDictionary<string, number[]>;
  const PART = "/jest_dict_partititions/";
  const LETT = "/jest_dict_letters/";
  const NUM = "/jest_dict_numbers/";
  const IND = "/test_array_index/";
  
  beforeAll(() => {
    virtual_fs.setRootDirectory(process.cwd() + "/test_data/");
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
      directory: IND,
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
      end: 0,
    };

    const partitionSize = 100 * 1000;
    const numPartitions = length / partitionSize;
    let end = partitionSize;
    let start = 1;
    for (let i = 0; i < numPartitions; i++) {
      partitions.push({
        length: partitionSize,
        end,
        start,
      });
      end += partitionSize;
      start += partitionSize;
    }

    const id = Math.floor(Math.random() * length);

    dictMeta.start = partitions[0].start;
    dictMeta.end = (<any>partitions.at(-1)).end;

    
    console.time("partition search")
    const partitionId = FragmentedDictionary.findPartitionForId(id, dictMeta);
    console.timeEnd("partition search")
    
    expect(partitionId).not.toBe(false);
    // expect(console.timeStamp("partition search")).toBeLessThan(20);
  });

  test("Partition ID search 2", () => {
    const dictMeta: FragDictMeta<string> = {
      length: 6,
      partitions: [{ start: "b", end: "c", length: 2 }, { start: "f", end: "f", length: 1 }, { start: "x", end: "z", length: 3 }],
      start: "b",
      end: "z",
    };

    expect(FragmentedDictionary.findPartitionForId("a", dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId("aaaa", dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId("b", dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId("c", dictMeta)).toBe(0);

    expect(FragmentedDictionary.findPartitionForId("dfff", dictMeta)).toBe(1);
    expect(FragmentedDictionary.findPartitionForId("e", dictMeta)).toBe(1);
    expect(FragmentedDictionary.findPartitionForId("f", dictMeta)).toBe(1);

    expect(FragmentedDictionary.findPartitionForId("p", dictMeta)).toBe(2);
    expect(FragmentedDictionary.findPartitionForId("x", dictMeta)).toBe(2);
    expect(FragmentedDictionary.findPartitionForId("y", dictMeta)).toBe(2);
    expect(FragmentedDictionary.findPartitionForId("zzz", dictMeta)).toBe(2);
  });


  test("Partition ID search 3", () => {
    const dictMeta: FragDictMeta<number> = {
      length: 6,
      partitions: [
        { start: 1, end: 20, length: 3 },
        { start: 30, end: 39, length: 3 },
        { start: 0, end: 0, length: 0 },
      ],
      start: 1,
      end: 39,
    };

    expect(FragmentedDictionary.findPartitionForId(-1, dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId(1, dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId(4, dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId(20, dictMeta)).toBe(0);

    expect(FragmentedDictionary.findPartitionForId(21, dictMeta)).toBe(1);
    expect(FragmentedDictionary.findPartitionForId(30, dictMeta)).toBe(1);
    expect(FragmentedDictionary.findPartitionForId(39, dictMeta)).toBe(1);

    expect(FragmentedDictionary.findPartitionForId(40, dictMeta)).toBe(2);
    expect(FragmentedDictionary.findPartitionForId(400, dictMeta)).toBe(2);

  });


  test("Partition ID search 4", () => {
    const dictMeta: FragDictMeta<number> = {
      length: 6,
      partitions: [
        { start: 0, end: 0, length: 0 },//0
        { start: 0, end: 0, length: 0 },//1
        { start: 10, end: 12, length: 3 },//2
        { start: 0, end: 0, length: 0 },//3
        { start: 0, end: 0, length: 0 },//4
        { start: 20, end: 22, length: 3 },//5
        { start: 0, end: 0, length: 0 },//6
        { start: 0, end: 0, length: 0 },//7
      ],
      start: 10,
      end: 22,
    };

    expect(FragmentedDictionary.findPartitionForId(-1, dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId(1, dictMeta)).toBe(0);
    expect(FragmentedDictionary.findPartitionForId(9, dictMeta)).toBe(0);

    expect(FragmentedDictionary.findPartitionForId(10, dictMeta)).toBe(2);
    expect(FragmentedDictionary.findPartitionForId(11, dictMeta)).toBe(2);
    expect(FragmentedDictionary.findPartitionForId(12, dictMeta)).toBe(2);

    expect(FragmentedDictionary.findPartitionForId(13, dictMeta)).toBe(3);
    expect(FragmentedDictionary.findPartitionForId(16, dictMeta)).toBe(3);
    expect(FragmentedDictionary.findPartitionForId(19, dictMeta)).toBe(3);

    expect(FragmentedDictionary.findPartitionForId(20, dictMeta)).toBe(5);
    expect(FragmentedDictionary.findPartitionForId(21, dictMeta)).toBe(5);
    expect(FragmentedDictionary.findPartitionForId(22, dictMeta)).toBe(5);

    expect(FragmentedDictionary.findPartitionForId(40, dictMeta)).toBe(6);
    expect(FragmentedDictionary.findPartitionForId(400, dictMeta)).toBe(6);

  });

  test("creates partitions", async () => {
    expect(partitions.numPartitions).toBe(0);
    expect(vfs.openFile(PART + "part0_dict.json").exists).toBe(false);

    partitions.createNewPartition(0);

    expect(vfs.openFile(PART + "part0_dict.json").exists).toBe(true);
    expect(vfs.openFile(PART + "part0_keys.json").exists).toBe(true);
    expect(vfs.openFile(PART + "part1_dict.json").exists).toBe(false);

    partitions.createNewPartition(1);

    expect(vfs.openFile(PART + "part1_dict.json").exists).toBe(true);
    expect(partitions.numPartitions).toBe(2);

    vfs.writeFile(PART + "part1_dict.json", { 1123: "test" });
    vfs.writeFile(PART + "part1_keys.json", [1123]);
    expect(partitions.openPartition(1).get(1123)).toBe("test");

    partitions.createNewPartition(0);

    expect(vfs.openFile(PART + "part2_dict.json").exists).toBe(true);

    expect(partitions.numPartitions).toBe(3);
    expect(partitions.openPartition(1).get(1123)).toBe(undefined);
    expect(partitions.openPartition(2).get(1123)).toBe("test");

    partitions.createNewPartition(partitions.numPartitions);

    await allIsSaved();
    expect(existsSync(PART + "part3_dict.json")).toBe(true);
    expect(existsSync(PART + "part3_keys.json")).toBe(true);
    expect(existsSync(PART + "part4_dict.json")).toBe(false);
    expect(partitions.numPartitions).toBe(4);
  });

  test("inserts sorted dict", async () => {
    numbers = FragmentedDictionary.reset(numbers);
    expect(numbers.length).toBe(0);
    expect(numbers.start).toBe(0);
    expect(numbers.end).toBe(0);

    numbers.insertSortedDict(SortedDictionary.fromLenght(3));
    expect(numbers.length).toBe(3);
    expect(numbers.start).toBe(1);
    expect(numbers.end).toBe(3);

    numbers = FragmentedDictionary.reset(numbers);

    numbers.insertSortedDict(SortedDictionary.fromLenght(15));
    expect(numbers.length).toBe(15);
    expect(numbers.start).toBe(1);
    expect(numbers.end).toBe(15);
    expect(numbers.getOne(15)).toBe(15);
    expect(numbers.getOne(7)).toBe(7);
    expect(numbers.getOne(1)).toBe(1);

  });

  test("splits partitions", async () => {
    const initial = ["b", "c", "x", "y", "z",];
    letters.insertMany(initial, initial.map(l => Math.random()));

    letters.splitPartition(0, "p");
    let p0 = letters.openPartition(0);
    let p1 = letters.openPartition(1);
    let p3 = letters.openPartition(2);

    expect(p0.get("b")).toBeDefined();
    expect(p0.get("c")).toBeDefined();

    expect(p1.get("x")).toBeDefined();

    expect(p3.get("y")).toBeDefined();
    expect(p3.get("z")).toBeDefined();


    let m0 = letters.meta.partitions[0];
    let m1 = letters.meta.partitions[1];
    expect(m0.length).toBe(2);
    expect(m0.end).toBe("c");
    expect(m1.length).toBe(1);
    expect(m1.end).toBe("x");

    await allIsSaved();
  });

  test("adds items at the tail", () => {
    numbers = FragmentedDictionary.reset(numbers);
    numbers.insertArray([1]);
    expect(numbers.length).toBe(1);
    expect(numbers.end).toBe(1)
    expect(numbers.meta.start).toBe(1)

    numbers.insertArray([2, 3, 4]);
    expect(numbers.length).toBe(4);

    let p1 = numbers.openPartition(1);
    expect(p1.get(4)).toBe(4);

    expect(numbers.numPartitions).toBe(2);
    expect(numbers.end).toBe(4);
  });


  test("adds items at the center", () => {
    letters = FragmentedDictionary.reset(letters);

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
    expect(letters.length).toBe(5);

    expect(letters.findPartitionForId("z")).toBe(1);
    expect(letters.findPartitionForId("b")).toBe(0);

    expect(letters.getOne("z")).toBe(26);
    expect(letters.getOne("b")).toBe(2);


    // expect(letters.findPartitionForId("p")).toBe(0);
    const ptk = ["p"].sort();

    letters.insertMany(ptk, ptk.map(getLetterPosition));


    expect(letters.openPartition(1).get("p")).toBeDefined();
    expect(letters.numPartitions).toBe(3);
    expect(letters.getOne("p")).toBe(16);


    letters.insertMany({
      a: getLetterPosition("a")
    });

    expect(letters.start).toBe("a");
    expect(letters.meta.partitions[0].end).toBe("c");
    // console.log(letters.meta.partitions);

    expect(letters.numPartitions).toBe(3);
    expect(letters.findPartitionForId("b")).toBe(0);

  });


  test("edits items", () => {
    letters.edit(["b", "a"], (value) => value * 10);
    // console.log(letters.openPartition(0));
    expect(letters.findPartitionForId("a")).toBe(0)
    expect(letters.findPartitionForId("b")).toBe(0)
    expect(letters.getOne("a")).toBe(10);
    expect(letters.getOne("b")).toBe(20);
  });

  test("removes items", () => {
    letters.remove(["b", "a"]);
    expect(letters.getOne("a")).toBe(undefined);
    expect(letters.getOne("b")).toBe(undefined);
  });

  test("insert array", () => {
    numbers = FragmentedDictionary.reset(numbers);
    const ids = numbers.insertArray([5, 6, 7]);
    expect(numbers.length).toBe(3);
    expect(ids[0]).toBe(1);
    expect(ids[1]).toBe(2);
    expect(ids[2]).toBe(3);
    expect(numbers.getOne(1)).toBe(5);
    expect(numbers.getOne(2)).toBe(6);
    expect(numbers.getOne(3)).toBe(7);
  });


  test("insert one", () => {
    numbers = FragmentedDictionary.reset(numbers);
    numbers.setOne(123, 321);
    expect(numbers.getOne(123)).toBe(321);
  });

  test("array dict", () => {

    index.setOne("123", [123]);
    index.setOne("1234", [12312, 1421354, 54234]);

    expect(index.length).toBe(2);
    const arr = index.getOne("123");
    expect(arr).toBeDefined();
    if (!arr) return;
    expect(arr[0]).toBe(123);

    index.editRanges([[undefined, undefined]], arr => {
      arr.splice(1, 0, 1000);
      return [...arr];
    });

    // console.log(index.openPartition(0));

    expect((index.getOne("123") as any)[1]).toBe(1000);

  });


  test("meta start end consistency", () => {
    numbers = FragmentedDictionary.reset(numbers);
    numbers.insertSortedDict(SortedDictionary.fromLenght(40));

    numbers.remove([40, 39, 38, 37, 36]);

    expect(numbers.end).toBe(35);
    expect(numbers.length).toBe(35);

    numbers.remove([1, 2, 3]);

    expect(numbers.start).toBe(4);
    expect(numbers.length).toBe(32);
  });

  test("at index", () => {
    letters = FragmentedDictionary.reset(letters);
    const lettersInverted = SortedDictionary.fromLenght(26).transform(id => (id + 9).toString(36));
    letters.insertSortedDict(SortedDictionary.fromKeys(lettersInverted.values()).transform(letter => parseInt(letter, 36) - 9));

    expect(letters.length).toBe(26);
    // console.log(letters.loadAll().toJSON());

    expect(letters.atindex(0)).toBe(1);
    expect(letters.atindex(17)).toBe(18);
    expect(letters.atindex(19)).toBe(20);
    expect(letters.atindex(4)).toBe(5);
    expect(letters.atindex(12)).toBe(13);

    expect(letters.atindex(25)).toBe(26);
    expect(letters.atindex(26)).toBe(undefined);
    expect(letters.atindex(-1)).toBe(undefined);
  });


  function fillNumbers(length: number) {
    numbers = FragmentedDictionary.reset(numbers);
    numbers.insertSortedDict(SortedDictionary.fromLenght(length));
  }

  test("limit", () => {
    fillNumbers(100);
    const ten = Object.values(numbers.filterSelect([[1, 50]], 0));
    expect(ten.length).toBe(50);

    const three = Object.values(numbers.filterSelect([[undefined, undefined]], 3));
    expect(three.length).toBe(3);

    const five = Object.values(numbers.filterSelect([[undefined, undefined]], 5, val => val % 2 == 0 ? val : undefined));

    expect(five.length).toBe(5);
  });


  test("limit 2", () => {
    fillNumbers(100);
    const tenRes = numbers.iterateRanges({
      filter: val => val % 2 == 0,
      select: val => val,
      limit: 10,
    });

    const ten = Object.values(tenRes[0]);
    expect(ten.length).toBe(10);
    expect(ten[0]).toBe(2);
    expect(ten[9]).toBe(20);

    const fiveRes = numbers.iterateRanges({
      ranges: [[6, undefined]],
      filter: val => val % 2 == 0,
      select: val => val,
      limit: 5,
    })
    const five = Object.values(fiveRes[0]);
    expect(five.length).toBe(5);
    expect(five[0]).toBe(6);
    expect(five[1]).toBe(8);
    expect(five[4]).toBe(14);

    const sevenRes = numbers.iterateRanges({
      ranges: FragmentedDictionary.idsToRanges([1, 2, 30, 4, 25, 90, 6, 7, 8]),
      select: val => val,
      limit: 7,
    });

    const seven = Object.values(sevenRes[0]);
    expect(seven.length).toBe(7)
  });

  test("select 0", () => {
    numbers.insertMany([0.011231, 0, 0.02123123], [123, -123, 123123]);
    // expect(numbers.meta.partitions[0].length).toBe(3);
    // expect(numbers.meta.partitions[0].start).toBe(0);
    // expect(numbers.meta.partitions[0].end).toBe(0.02);
    expect(numbers.start).toBe(0);

    const firstPart = numbers.openPartition(0);
    expect(firstPart.get(0)).toBe(-123);
    const zero1 = numbers.iterateRanges({
      ranges: [[0, 0]],
      select: val => val
    })[0];
    expect(zero1[0]).toBe(-123);
  });

  test("id matches ranges", () => {
    expect(FragmentedDictionary.idMatchesRanges(1, [[undefined, undefined]])).toBe(true);
    expect(FragmentedDictionary.idMatchesRanges(1, [[undefined, undefined]], true)).toBe(false);
    expect(FragmentedDictionary.idMatchesRanges(1, [[1, 1]])).toBe(true);
    expect(FragmentedDictionary.idMatchesRanges(1, [[1, 1]], true)).toBe(false);
    expect(FragmentedDictionary.idMatchesRanges(1, [[1, 10]])).toBe(true);
    expect(FragmentedDictionary.idMatchesRanges(5, [[1, 10]])).toBe(true);
    expect(FragmentedDictionary.idMatchesRanges(10, [[1, 10]])).toBe(true);
    expect(FragmentedDictionary.idMatchesRanges(0, [[1, 10]])).toBe(false);
    expect(FragmentedDictionary.idMatchesRanges(11, [[1, 10]])).toBe(false);
    expect(FragmentedDictionary.idMatchesRanges(11, [[1, 10]])).toBe(false);
    expect(FragmentedDictionary.idMatchesRanges(11, [[1, 10], [11, 11]])).toBe(true);
    expect(FragmentedDictionary.idMatchesRanges(12, [[1, 10], [11, 11]])).toBe(false);
    expect(FragmentedDictionary.idMatchesRanges(12, [[1, 10], [11, 11]], true)).toBe(true);
  });

  test("partition matches ranges", () => {
    expect(FragmentedDictionary.partitionMatchesRanges(0, 0, [[0, 0]])).toBe(true);
    expect(FragmentedDictionary.partitionMatchesRanges(0, 0, [[0, 0]], true)).toBe(false);
    expect(FragmentedDictionary.partitionMatchesRanges(1, 10, [[1, 10]])).toBe(true);
    expect(FragmentedDictionary.partitionMatchesRanges(1, 5, [[1, 10]])).toBe(true);
    expect(FragmentedDictionary.partitionMatchesRanges(4, 5, [[1, 10]])).toBe(true);
    expect(FragmentedDictionary.partitionMatchesRanges(4, 5, [[5, 10]])).toBe(true);
    expect(FragmentedDictionary.partitionMatchesRanges(4, 5, [[6, 10]])).toBe(false);
    expect(FragmentedDictionary.partitionMatchesRanges(11, 12, [[6, 10]])).toBe(false);
    expect(FragmentedDictionary.partitionMatchesRanges(10, 12, [[6, 10]])).toBe(true);
    expect(FragmentedDictionary.partitionMatchesRanges(10, 12, [[6, 9], [13, 13]])).toBe(false);
    expect(FragmentedDictionary.partitionMatchesRanges(10, 12, [[6, 9], [13, 13]], true)).toBe(true);
  });

  test("inverted select", () => {
    const numTickets = 10;
    fillNumbers(numTickets);
    const takenTickets = [1, 4, 5];
    const generatedRandom = 3; //[0..6] rand(numTickets - takenTickets.length)

    const seven = numbers.iterateRanges({
      ranges: takenTickets.map(t => [t, t]),
      invertRanges: true,
      limit: 1,
      offset: generatedRandom,
      select: val => val,
    })[0];

    expect(seven).toHaveProperty("7");
    expect(seven[7]).toBe(7);
  });

  test("limit offset", () => {

    fillNumbers(100);

    const page2 = numbers.iterateRanges({
      limit: 10,
      offset: 10,
      select: val => val,
    })[0];
    const values = Object.values(page2);
    expect(values.length).toBe(10);
    expect(values[0]).toBe(11);
    expect(values[9]).toBe(20);
  });

  afterAll(async () => {
    await allIsSaved();
    rmie(PART);
    rmie(LETT);
    rmie(NUM);
    rmie(IND);
  })
});