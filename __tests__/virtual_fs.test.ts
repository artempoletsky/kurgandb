import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import fs from "../src/virtual_fs";
import { existsSync, mkdirSync } from "fs";
import { rimraf } from "rimraf";

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

describe("Virtual file system", () => {

  const DIR = "/test_virtual_fs";
  const CWD = process.cwd();
  beforeAll(() => {
    rimraf.sync(CWD + "/test_data" + DIR);
    mkdirSync(CWD + "/test_data" + DIR);
    fs.setRootDirectory(CWD + "/test_data");
  });

  test("creates a file", async () => {
    const fileName = DIR + "/test.json";
    const written = fs.writeFile(fileName, { foo: "bar" });

    const read = fs.readFile(fileName);
    expect(read).toHaveProperty("foo");
    expect(read.foo).toBe("bar");

    expect(existsSync(CWD + "/test_data" + fileName)).toBe(false);
    await written;
    expect(existsSync(CWD + "/test_data" + fileName)).toBe(true);
  });

  test("removes a file", async () => {
    const fileName = DIR + "/test.json"
    fs.removeFile(fileName);
    expect(existsSync(CWD + "/test_data" + fileName)).toBe(false);

    const saved = fs.writeFile(fileName, {});

    const read = fs.readFile(fileName);
    expect(read).toBeDefined();
    expect(existsSync(CWD + "/test_data" + fileName)).toBe(false);

    fs.removeFile(fileName);

    expect(fs.readFile(fileName)).toBeUndefined();

    await saved;
    expect(existsSync(CWD + fileName)).toBe(false);
  });

  test("file object", async () => {
    const fileName = DIR + "/test.json"

    const file = fs.openFile(fileName);
    expect(file.exists).toBe(false);
    file.write({ foo: "123" });
    expect(file.isCached).toBe(true);
    expect(file.exists).toBe(true);

    file.rename(DIR + "/test1.json");

    await file.saved;
    expect(file.relativePath).toBe(DIR + "/test1.json");
    expect(existsSync(file.absolutePath)).toBe(true);

  });
});