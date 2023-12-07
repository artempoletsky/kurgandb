import { existsSync } from "fs";
import { FieldType, Document, IDocument } from "./document";
import { PlainObject, rfs } from "./utils";


export type DocumentScheme = {
  fields: Record<string, FieldType>
  settings: Record<string, any>
};

export type SchemeFile = {
  tables: Record<string, DocumentScheme>
};

export type TableMetadata = {
  index: number,
  length: number,
}

export interface ITable {
  length: number
  readonly scheme: DocumentScheme
  each(predicate: (doc: IDocument, id: number, table: ITable) => any): ITable
  map(predicate: (doc: IDocument, id: number, table: ITable) => IDocument): any[]
  save(): void
  find(predicate: (doc: IDocument, id: number, table: ITable) => boolean): IDocument | null
  filter(predicate: (doc: IDocument, id: number, table: ITable) => boolean): ITable
  push(data: PlainObject): IDocument
  delete(id: number): void
  clear(): void
  at(id: number): IDocument
  has(id: number | string): boolean
  getDocumentData(id: string): PlainObject
}

export const SCHEME_PATH = "/data/scheme.json";

export class Table implements ITable {
  protected documents: Map<number, IDocument>;
  protected documentData: PlainObject;
  protected meta: TableMetadata;
  public readonly scheme: DocumentScheme;
  constructor(name: string) {
    const scheme = Table.getScheme(name);
    if (!scheme) {
      throw new Error(`table '${name}' doesn't exist`);
    }

    this.scheme = scheme;

    const data = rfs(Table.getFilepath(name)) || {};
    this.documentData = data.documents;
    this.documents = new Map<number, IDocument>();
    for (const idString in data.documents) {
      const id = Table.idNumber(idString);
      this.documents.set(id, new Document(this, idString));
    }

    this.meta = data.meta;
  }

  public get length(): number {
    return this.meta.length;
  }

  filter(predicate: (doc: IDocument, id: number, table: ITable) => boolean): ITable {
    for (const key of this.documents.keys()) {
      let doc = this.documents.get(key) as IDocument;
      if (!predicate(doc, key, this)) {
        this.delete(key);
      }
    }
    return this;
  }

  find(predicate: (doc: IDocument, id: number, table: ITable) => boolean): IDocument | null {
    for (const key of this.documents.keys()) {
      let doc = this.documents.get(key) as IDocument;
      if (predicate(doc, key, this)) return doc;
    }
    return null;
  }

  clear(): void {
    this.documentData = {};
    this.documents.clear();
  }

  delete(id: number): void {
    delete this.documentData[Table.idString(id)];
    this.documents.delete(id);
  }

  push(data: PlainObject): IDocument {
    const validationError = Document.validateData(data, this.scheme);
    if (validationError) throw new Error("data is invalid; Use 'Document.validateData' before pushing");

    const id = this.meta.index++;
    const idStr = Table.idString(id);
    this.documentData[idStr] = data;
    const doc = new Document(this, idStr);
    this.documents.set(id, doc);
    return doc;
  }

  save(): void {
    this.documentData = {};
    for (const id of this.documents.keys()) {
      let doc = this.documents.get(id) as IDocument;
      let idStr = Table.idString(id);
      this.documentData[idStr] = doc.serialize();
    }
  }

  has(id: string | number): boolean {
    return this.documents.has(typeof id == "number" ? id : Table.idNumber(id));
  }

  map(predicate: (doc: IDocument, id: number, table: ITable) => any): any[] {
    const res: any[] = []
    for (const key of this.documents.keys()) {
      res.push(predicate(this.documents.get(key) as IDocument, key, this));
    }
    return res;
  }

  each(predicate: (doc: IDocument, id: number, table: ITable) => false | undefined): ITable {
    for (const key of this.documents.keys()) {
      if (predicate(this.documents.get(key) as IDocument, key, this) === false) break;
    }
    return this;
  }

  getDocumentData(id: string): PlainObject {
    if (!this.documentData[id]) throw new Error("wrong document id!");
    return this.documentData[id];
  }

  at(id: number): IDocument {
    if (!this.documents.has(id)) throw new Error("wrong document id!");
    return this.documents.get(id) as IDocument;
  }

  static getFilepath(tableName: string): string {
    return process.cwd() + "/data/tables/" + tableName + ".json";
  }

  static isTableExist(tableName: string): boolean {
    return !!this.getScheme(tableName);
  }

  static idString(id: number): string {
    return String.fromCharCode(id);
  }

  static idNumber(id: string): number {
    return id.charCodeAt(0)
  }

  static getScheme(tableName: string): DocumentScheme | undefined {
    const dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme document is invalid! 'tables' missing");
    return dbScheme.tables[tableName];
  }
}