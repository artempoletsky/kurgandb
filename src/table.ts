
import { FieldType, Document, IDocument } from "./document";
import { PlainObject, rfs, wfs, existsSync } from "./utils";


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

export type TableFileContents = {
  documents: Record<string, PlainObject>,
  meta: TableMetadata
}


export type BooleansFileContents = {
  [key: string]: number[]
}


const EmptyTable: TableFileContents = {
  meta: {
    index: 0,
    length: 0,
  },
  documents: {}
};

export interface ITable {
  name: string,
  length: number
  readonly scheme: DocumentScheme
  each(predicate: (doc: IDocument) => any): void
  map(predicate: (doc: IDocument) => any): any[]
  save(): void
  find(predicate: (doc: IDocument) => boolean): IDocument | null
  filter(predicate: (doc: IDocument) => boolean): ITable
  push(data: PlainObject): IDocument
  delete(id: number): void
  clear(): void
  at(id: number): IDocument
  has(id: number | string): boolean
  getDocumentData(id: string): PlainObject
  addField(name: string, type: FieldType, predicate?: (doc: IDocument) => any): void
  removeField(name: string): void
}

export const SCHEME_PATH = "/data/scheme.json";

export function getDefaultValueForType(type: FieldType) {
  switch (type) {
    case "number": return 0;
    case "string": return "";
    case "File": return "";
    case "Text": return "";
    case "date": return new Date();
    case "boolean": return false;
  }
}

export class Table implements ITable {
  protected documents: Map<number, IDocument>;
  protected documentData: PlainObject;
  protected meta: TableMetadata;
  public readonly scheme: DocumentScheme;
  public readonly name: string;
  public readonly booleans: BooleansFileContents;
  constructor(name: string) {
    const scheme = Table.getScheme(name);
    if (!scheme) {
      throw new Error(`table '${name}' doesn't exist`);
    }

    this.name = name;

    this.scheme = scheme;
    const booleansFilepath = Table.getBooleansFilepath(name);
    if (!existsSync(booleansFilepath)) {
      wfs(booleansFilepath, {});
    }
    this.booleans = rfs(booleansFilepath);

    const filepath = Table.getFilepath(name);
    if (!existsSync(filepath)) {
      wfs(filepath, EmptyTable);
    }
    const data: TableFileContents = rfs(filepath);
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


  protected saveScheme() {
    const schemeFile: SchemeFile = rfs(SCHEME_PATH);
    schemeFile.tables[this.name] = this.scheme;
    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });
  }

  removeField(name: string): void {
    const type = this.scheme.fields[name];
    delete this.scheme.fields[name];
    if (type == "boolean") {
      delete this.booleans[name];
    } else {
      this.each(doc => {
        delete doc[name];
      });
    }
    this.saveScheme();
    this.save();
  }

  addField(name: string, type: FieldType, predicate?: (doc: IDocument) => any): void {
    if (this.scheme.fields[name]) {
      throw new Error(`field '${name}' already exists`);
    }

    if (type == "boolean") {
      this.booleans[name] = [];
    }

    this.scheme.fields[name] = type;
    this.each(doc => {
      doc[name] = predicate ? predicate(doc) : getDefaultValueForType(type);
    });

    this.saveScheme();
    this.save();
  }

  filter(predicate: (doc: IDocument) => boolean): ITable {
    for (const key of this.documents.keys()) {
      let doc = this.documents.get(key) as IDocument;
      if (!predicate(doc)) {
        this.delete(key);
      }
    }
    return this;
  }

  find(predicate: (doc: IDocument) => boolean): IDocument | null {
    for (const key of this.documents.keys()) {
      let doc = this.documents.get(key) as IDocument;
      if (predicate(doc)) return doc;
    }
    return null;
  }

  clear(): void {
    this.documentData = {};
    this.documents.clear();
    this.meta.length = 0;
  }

  delete(id: number): void {
    delete this.documentData[Table.idString(id)];
    this.documents.delete(id);
    this.meta.length--;
  }

  push(data: PlainObject): IDocument {
    const validationError = Document.validateData(data, this.scheme);
    if (validationError) throw new Error(`push failed, data is invalid for reason '${validationError}'`);

    const id = this.meta.index++;
    const idStr = Table.idString(id);
    this.documentData[idStr] = data;
    const doc = new Document(this, idStr);
    this.documents.set(id, doc);
    this.meta.length++;
    return doc;
  }

  save(): void {
    const fileData: TableFileContents = {
      documents: {},
      meta: this.meta
    };
    this.documentData = {};
    for (const id of this.documents.keys()) {
      let doc = this.documents.get(id) as IDocument;
      let idStr = Table.idString(id);
      this.documentData[idStr] = doc.serialize();
    }
    fileData.documents = this.documentData;
    wfs(Table.getFilepath(this.name), fileData);
    wfs(Table.getBooleansFilepath(this.name), this.booleans);
  }

  has(id: string | number): boolean {
    return this.documents.has(typeof id == "number" ? id : Table.idNumber(id));
  }

  map(predicate: (doc: IDocument) => any): any[] {
    const res: any[] = []
    for (const key of this.documents.keys()) {
      res.push(predicate(this.documents.get(key) as IDocument));
    }
    return res;
  }

  each(predicate: (doc: IDocument) => any): ITable {
    for (const key of this.documents.keys()) {
      if (predicate(this.documents.get(key) as IDocument) === false) break;
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

  static getBooleansFilepath(tableName: string): string {
    return "/data/tables/" + tableName + "_booleans.json";
  }

  static getFilepath(tableName: string): string {
    return "/data/tables/" + tableName + ".json";
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

  toJSON() {
    return this.map(doc => doc.toJSON());
  }
}