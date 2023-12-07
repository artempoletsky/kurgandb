import { Document, IDocument } from "./document";
import { PlainObject } from "./utils";



export class Table extends Array<IDocument> {
  constructor(name: string) {
    super();
  }
  static isTableExist(name: string): boolean {
    return false;
  }
}