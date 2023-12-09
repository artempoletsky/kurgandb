
import { FieldType, Document, IDocument, HeavyTypes, HeavyType, LightTypes } from "./document";
import { PlainObject, rfs, wfs, existsSync, mkdirSync, mdne, statSync, rmie } from "./utils";



export type TableScheme = {
  fields: Record<string, FieldType>
  settings: {
    largeObjects: boolean
    manyRecords: boolean
    maxPartitionSize: number

  }
};

export type SchemeFile = {
  tables: Record<string, TableScheme>
};

export type TableMetadata = {
  index: number
  length: number
  partitions: PartitionMeta[]
}

export type DocumentData = Record<string, any[]>;
export type Partition = {
  isDirty: boolean
  documents: DocumentData
  fileName: string
  id: number
  size: number
  meta: PartitionMeta
}

export type PartitionMeta = {
  length: number
  end: number
}

export type BooleansFileContents = {
  [key: string]: number[]
}


const EmptyTable: TableMetadata = {
  index: 0,
  length: 0,
  partitions: []
};



export interface ITable {
  name: string,
  length: number
  readonly scheme: TableScheme

  each(predicate: (doc: IDocument) => any): void
  select(predicate: (doc: IDocument) => any, options?: {
    offset?: number,
    limit?: number
  }): IDocument[]
  select(predicate: (doc: IDocument) => any): IDocument[]
  find(predicate: (doc: IDocument) => boolean): IDocument | null
  remove(predicate: (doc: IDocument) => boolean): void
  removeByID(...ids: number[]): void
  push(data: PlainObject): IDocument
  clear(): void

  ids(ids: number[], predicate: (doc: IDocument) => any): void
  at(id: number): IDocument | null

  getDocumentData(id: string): PlainObject // gets raw data from index JSON

  addField(name: string, type: FieldType, predicate?: (doc: IDocument) => any): void
  removeField(name: string): void
  forEachField(predicate: (fieldName: string, type: string) => any): void
}

export const SCHEME_PATH = "/data/scheme.json";

export function getDefaultValueForType(type: FieldType) {
  switch (type) {
    case "number": return 0;
    case "string": return "";
    case "JSON": return null;
    case "json": return null;
    case "Text": return "";
    case "date": return new Date();
    case "boolean": return false;
  }
}


export function getBooleansFilepath(tableName: string): string {
  return `/data/tables/${tableName}.json`;
}

export function getFilepath(tableName: string): string {
  return `/data/tables/${tableName}.json`;
}

export function getPartitionFilePath(tableName: string, id: number): string {
  return `/data/tables/${tableName}_part${id}.json`;
}

export function getFilesDir(tableName: string): string {
  return "/data/tables/" + tableName + "/";
}

export function isHeavyType(type: FieldType): boolean {
  return HeavyTypes.includes(type as HeavyType);
}

export class Table implements ITable {
  protected currentPartition: Partition | undefined;
  protected meta: TableMetadata;
  public readonly fieldNameIndex: Record<string, number>;
  public readonly indexFieldName: string[];

  public readonly scheme: TableScheme;
  public readonly name: string;

  constructor(name: string) {
    const scheme = Table.getScheme(name);
    if (!scheme) {
      throw new Error(`table '${name}' doesn't exist`);
    }
    this.name = name;
    this.scheme = scheme;

    // this.indexFieldName = Object.keys(scheme.fields);
    this.fieldNameIndex = {};
    this.indexFieldName = [];
    let i = 0;
    this.forEachField((key, type) => {
      if (isHeavyType(type)) return;
      this.indexFieldName[i] = key;
      this.fieldNameIndex[key] = i++;
    })

    mdne(getFilesDir(name))
    const filepath = getFilepath(name);

    if (!existsSync(filepath)) {
      wfs(filepath, EmptyTable);
    }

    this.meta = rfs(filepath);

    // this.reopen();
  }

  forEachField(predicate: (fieldName: string, type: FieldType, index: number) => any): void {
    const fields = this.scheme.fields;
    let i = 0;
    for (const key in fields) {
      predicate(key, fields[key], i++);
    }
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
    const isHeavy = isHeavyType(type);
    const index = this.fieldNameIndex[name];
    this.each(doc => {
      if (!this.currentPartition) throw new Error("never");
      this.getDocumentData(doc.getStringID()).splice(index, 1);
      this.markCurrentPartitionDirty();
      if (isHeavy) {
        rmie(this.getHeavyFieldFilepath(doc.id, type as HeavyType, name));
      }
    });

    this.saveScheme();
  }

  getCurrentPartitionID(): number {
    if (!this.currentPartition) throw new Error("current partition is undefined");
    return this.currentPartition.id;
  }

  markCurrentPartitionDirty(): void {
    if (!this.currentPartition) throw new Error("current partition is undefined");
    this.currentPartition.isDirty = true;
  }

  addField(name: string, type: FieldType, predicate?: (doc: IDocument) => any): void {
    if (this.scheme.fields[name]) {
      throw new Error(`field '${name}' already exists`);
    }

    const filesDir = `/data/tables/${this.name}/`;

    if (isHeavyType(type) && !existsSync(filesDir)) {
      mkdirSync(filesDir);
    }

    this.scheme.fields[name] = type;
    this.each(doc => {
      doc.set(name, predicate ? predicate(doc) : getDefaultValueForType(type));
    });

    this.saveScheme();
  }

  // select(predicate: (doc: IDocument) => any): IDocument[]
  select(predicate: (doc: IDocument) => boolean, options?: { offset?: number, limit?: number }): IDocument[] {
    if (!options) options = {};
    const offset = options.offset || 0;
    const limit = options.limit || 0;
    let found = 0;
    const result: IDocument[] = [];
    this.each(doc => {
      if (predicate(doc)) {
        result.push(doc);
        found++;
        if (limit && (limit + offset) <= found) {
          return false;
        }
      }
    })
    return result;
  }

  remove(predicate: (doc: IDocument) => boolean) {
    this.each(doc => {
      if (predicate(doc)) {
        this.removeDocumentFromCurrentPartition(doc);
      }
    });
  }

  removeDocumentFromCurrentPartition(doc: IDocument) {
    const partition = this.currentPartition;
    if (!partition) return;
    delete partition.documents[doc.getStringID()];
    this.meta.partitions[partition.id].length--;
    partition.isDirty = true;
  }

  find(predicate: (doc: IDocument) => boolean): IDocument | null {
    return this.select(predicate, {
      limit: 1
    })[0] || null;
  }

  clear(): void {
    this.remove(() => true);
  }

  removeByID(...ids: number[]): void {
    // this.findPartitionForId()
    // delete this.documentData[Table.idString(id)];
    // this.documents.delete(id);
    // this.meta.length--;

    this.ids(ids, doc => {
      this.removeDocumentFromCurrentPartition(doc);
    })
  }

  getHeavyFieldFilepath(id: number | string, type: HeavyType, fieldName: string): string {
    return `${getFilesDir(this.name)}${id}_${fieldName}.${type == "Text" ? "txt" : "json"}`;
  }

  push(data: PlainObject): IDocument {
    const validationError = Document.validateData(data, this.scheme);
    if (validationError) throw new Error(`push failed, data is invalid for reason '${validationError}'`);

    const id = this.meta.index++;
    const idStr = Table.idString(id);


    this.forEachField((key, type) => {
      const value = data[key];
      if (isHeavyType(type) && !!value) {
        wfs(this.getHeavyFieldFilepath(id, type as HeavyType, key), value);
      } if (type == "date" && data[key] instanceof Date) {
        data[key] = data[key].toJSON();
      }
    });

    let lastPartition = this.openPartition();

    lastPartition.documents[idStr] = this.squarifyObject(data);
    const doc = new Document(this, idStr);
    this.meta.length++;

    lastPartition.isDirty = true;

    this.meta.partitions[lastPartition.id].end = id;
    this.meta.partitions[lastPartition.id].length++;
    return doc;
  }

  openPartition(index?: number): Partition {
    const isLastPartition = index === undefined;

    if (index === undefined) {
      index = this.meta.partitions.length - 1;
    }
    const meta: PartitionMeta | undefined = this.meta.partitions[index];
    if (!meta) {
      throw new Error("partition doesn't exists");
    }

    const fileName = getPartitionFilePath(this.name, index);
    const size = statSync(fileName).size;
    if (isLastPartition && size >= this.scheme.settings.maxPartitionSize) {
      this.meta.partitions.push({
        end: 0,
        length: 0,
      });
      wfs(fileName, {});
      return this.openPartition();
    }
    let documents: DocumentData = rfs(fileName);
    this.currentPartition = {
      documents,
      isDirty: false,
      fileName,
      size,
      meta,
      id: index,
    };

    return this.currentPartition;
  }


  expandObject(arr: any[]) {
    const result: PlainObject = {};
    let i = 0;
    this.forEachField((key, type) => {
      if (isHeavyType(type)) return;
      // if (type == "boolean") {
      //   result[key] = !!arr[i];
      //   return;
      // }
      result[key] = arr[i];
      i++;
    });
    return result;
  }

  expandDocuments(docs: Record<string, any[]>): Record<string, PlainObject> {
    const expandedData: Record<string, PlainObject> = {};
    for (const idStr in docs) {
      expandedData[idStr] = this.expandObject(docs[idStr]);
    }
    return expandedData;
  }

  squarifyObject(o: PlainObject) {
    const result: any[] = [];
    this.forEachField((key, type) => {
      // console.log(key, type);

      if (isHeavyType(type)) return;
      // if (type == "boolean") {
      //   result.push(o[key] ? 1 : 0);
      //   return;
      // }
      result.push(o[key]);
    });
    return result;
  }

  squarifyDocuments(docs: PlainObject) {
    const squareData: Record<string, any[]> = {};
    for (const idStr in docs) {
      squareData[idStr] = this.squarifyObject(docs[idStr]);
    }
    return squareData;
  }

  closePartition() {
    const p = this.currentPartition;
    if (!p) return;

    if (p.isDirty) {
      wfs(p.fileName, p.documents);
      wfs(getFilepath(this.name), this.meta);
    }
  }

  each(predicate: (doc: IDocument) => any): void {
    for (let partitionIndex = 0; partitionIndex < this.meta.partitions.length; partitionIndex++) {
      const partition = this.openPartition(partitionIndex);
      let breakSignal = false;
      for (const strId in partition.documents) {
        const doc = new Document(this, strId);

        if (breakSignal = predicate(doc) === false) break;
      }

      this.closePartition();
      if (breakSignal) break;
    }
  }

  getDocumentData(id: string): any[] {
    const data = this.currentPartition?.documents[id];
    if (!data) throw new Error("wrong IDocument id!");
    return data;
  }

  findPartitionForId(id: number): number | false {
    let start = 0;
    for (const [partitionID, pMeta] of this.meta.partitions.entries()) {
      if (start <= id && id <= pMeta.end) return partitionID;
      start = pMeta.end;
    }

    return false;
  }

  ids(ids: number[], predicate: (doc: IDocument) => any): void {
    const partitionsToOpen = new Map<number, number[]>();
    for (const id of ids) {
      let partId = this.findPartitionForId(id);
      if (partId === false) continue;
      let pIds = partitionsToOpen.get(partId) || [];
      pIds.push(id);
      partitionsToOpen.set(partId, pIds);
    }

    for (const [pID, ids] of partitionsToOpen) {
      this.openPartition(pID);
      for (const id of ids) {
        predicate(new Document(this, Table.idString(id)));
      }
      this.closePartition();
    }
  }

  at(id: number): IDocument | null {
    const docs: IDocument[] = [];
    if (this.findPartitionForId(id) === false) {
      return null;
    }
    this.ids([id], doc => docs.push(doc));
    return docs[0] || null;
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

  static getScheme(tableName: string): TableScheme | undefined {
    const dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme IDocument is invalid! 'tables' missing");
    return dbScheme.tables[tableName];
  }

  toJSON(): PlainObject[] {
    if (this.scheme.settings.manyRecords) {
      throw new Error("You probably don't want to download a whole table");
    }
    return this.select(() => true).map(doc => doc.toJSON());
  }

  getFieldsOfType(...types: FieldType[]): string[] {
    const result: string[] = [];
    this.forEachField((key, type) => {
      if (types.includes(type))
        result.push(key);
    });
    return result;
  }

  getLightKeys(): string[] {
    return this.getFieldsOfType(...LightTypes);
  }

  getHeavyKeys(): string[] {
    return this.getFieldsOfType(...HeavyTypes);
  }
}