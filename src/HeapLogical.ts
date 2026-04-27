import HeapDumb from "./HeapDumb";
import PagesManager from "./PagesManager";
import { PageView } from "./PageView";

import * as Leaf from "./LeafNumber";
import Tree from "./Tree";
import { NUMERIC_ID, PAGE_ADDRES_SIZE } from "./ServerGlobals";
import PatchFile from "./PatchFile";
import { DataBase } from "./db";

type SBKey = "length" | "parentPage";
type ARKey = "id" | "offset" | "length" | "allocated";
const v = new PageView<SBKey, ARKey>([
  ["length", 2],
  ["parentPage", PAGE_ADDRES_SIZE],
], [
  ["id", NUMERIC_ID],
  ["offset", 4],
  ["length", 4],
  ["allocated", 4],
]);

export default class HeapLogical {
  static file: PatchFile;
  static t: Tree<SBKey, ARKey>;
  static init() {
    this.file = new PatchFile(DataBase.workingDirectory + "/heap.bin");
  }
  // static dumb = HeapDumb;

  static createId(): number {

  }

  static get(id: number): Buffer {
    let sb = this.t.findValue(id);
    if (!sb) throw new Error(`HeapLogical: record '${id}' not found`);
    let b = Buffer.allocUnsafe(length);
    this.file.read(b, sb.offset, sb.length);
    return b;
  }

  static set(id: number, data: Buffer) {

  }

  static delete(id: number) {
    HeapDumb.free();
  }

  static getString(id: number): string {
    return this.get(id).toString("utf-8");
  }

  static getJSON(id: number): any {
    return JSON.parse(this.getString(id));
  }

  static setString(id: number, str: string) {
    this.set(id, Buffer.from(str));
  }

  static setJSON(id: number, data: any) {
    this.setString(id, JSON.stringify(data));
  }
}