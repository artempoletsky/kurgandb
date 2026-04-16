
import PatchFile from "../src/PatchFile";
import { afterAll, beforeAll, describe, expect, test, xdescribe, xtest } from "./test-setup";

import fs, { read } from "fs";



describe("PatchFile", () => {
  const path = [process.cwd(), "kurgandb_data", "file"].join("/");
  function removeTestData() {
    try { fs.unlinkSync(path); } catch { }
    try { fs.unlinkSync(path + ".patch"); } catch { }
  }

  beforeAll(removeTestData);
  // afterAll(removeTestData);

  test("commit", async () => {
    let f = new PatchFile(path);

    let data = Buffer.from("asdasdasd");
    let dataLen = data.byteLength;
    f.writeEnd(data);

    let readData = Buffer.alloc(dataLen);
    f.read(readData, 0, dataLen);
    expect(readData).toEqual(data);

    f.reset();
    readData.fill(0);
    f.read(readData, 0, dataLen);
    expect(readData).not.toEqual(data);
    f.writeEnd(data);
    await f.commit();
    f.read(readData, 0, dataLen);
    expect(readData).toEqual(data);
  });
});