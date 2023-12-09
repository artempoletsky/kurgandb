
import { DataBase, SchemeFile } from "./db";
import { FieldType, Document, IDocument, HeavyTypes, HeavyType, LightTypes } from "./document";
import { PlainObject, rfs, wfs, existsSync, mkdirSync, statSync, rmie, renameSync } from "./utils";


export type TableSettings = {
  largeObjects: boolean
  manyRecords: boolean
  maxPartitionSize: number
  maxPartitionLenght: number
}
export type TableScheme = {
  fields: Record<string, FieldType>
  settings: TableSettings
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
  insert(data: PlainObject): IDocument
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

export function getMetaFilepath(tableName: string): string {
  return `/data/${tableName}/meta.json`;
}

export function getPartitionFilePath(tableName: string, id: number): string {
  return `/data/${tableName}/part${id}.json`;
}

export function isHeavyType(type: FieldType): boolean {
  return HeavyTypes.includes(type as HeavyType);
}

export class Table implements ITable {
  protected meta: TableMetadata;
  public readonly fieldNameIndex: Record<string, number>;
  public readonly indexFieldName: string[];

  public readonly scheme: TableScheme;
  public readonly name: string;

  constructor(name: string) {
    const scheme = DataBase.getScheme(name);
    if (!scheme) {
      throw new Error(`table '${name}' doesn't exist`);
    }
    this.name = name;
    this.scheme = scheme;

    this.fieldNameIndex = {};
    this.indexFieldName = [];
    let i = 0;
    this.forEachField((key, type) => {
      if (isHeavyType(type)) return;
      this.indexFieldName[i] = key;
      this.fieldNameIndex[key] = i++;
    });

    this.meta = rfs(getMetaFilepath(name));

  }

  renameField(oldName: string, newName: string) {
    const { fields } = this.scheme;
    if (!fields[oldName]) throw new Error(`feild ${oldName} doesn't exist`);
    const newFields: Record<string, FieldType> = {};
    this.forEachField((name, type) => {///iterate to keep the same order in the javascript dictionary
      if (name == oldName) {
        newFields[newName] = type;
        if (isHeavyType(type)) {
          //rename the folder dedicated to the heavy field
          const workingDir = `/data/${this.name}/`;
          renameSync(workingDir + oldName, workingDir + newName);
        }
      } else {
        newFields[name] = type;
      }
    });

    this.scheme.fields = newFields;
    this.saveScheme();
  }

  getLastIndex() {
    return this.meta.index;
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
    return `/data/${this.name}/${fieldName}/${id}.${type == "Text" ? "txt" : "json"}`;
  }

  private _currentPartition: Partition | undefined;
  protected get currentPartition(): Partition {
    if (this._currentPartition) return this._currentPartition;
    const { partitions } = this.meta;
    if (!partitions.length) {
      this.createNewPartition();
    }

    if (!this._currentPartition) throw new Error("never");
    return this._currentPartition;
  }


  insert(data: PlainObject): IDocument {
    const validationError = Document.validateData(data, this.scheme);
    if (validationError) throw new Error(`insert failed, data is invalid for reason '${validationError}'`);

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
    const { partitions } = this.meta;
    const { settings } = this.scheme;


    if (!partitions.length || partitions[partitions.length - 1].length >= settings.maxPartitionLenght) {
      this.createNewPartition();
    }

    let p = this.currentPartition;

    p.documents[idStr] = this.squarifyObject(data);
    const doc = new Document(this, idStr);
    this.meta.length++;

    p.isDirty = true;
    p.meta.end = id;
    p.meta.length++;

    return doc;
  }

  createNewPartition(): void {
    this.closePartition();
    const meta: PartitionMeta = { length: 0, end: 0 };
    const id = this.meta.partitions.length;
    this.meta.partitions.push(meta);
    const fileName = getPartitionFilePath(this.name, id);
    wfs(fileName, {});
    this._currentPartition = {
      documents: {},
      isDirty: false,
      fileName,
      size: 0,
      meta,
      id,
    };
  }

  openPartition(index: number): void {
    const meta: PartitionMeta | undefined = this.meta.partitions[index];
    if (!meta) {
      throw new Error(`partition '${index}' doesn't exists`);
    }

    const fileName = getPartitionFilePath(this.name, index);
    const size = statSync(fileName).size;
    const isLastPartition = index === this.meta.partitions.length - 1;
    if (isLastPartition && size >= this.scheme.settings.maxPartitionSize) {
      this.createNewPartition();
      return;
    }

    let documents: DocumentData = rfs(fileName);
    this._currentPartition = {
      documents,
      isDirty: false,
      fileName,
      size,
      meta,
      id: index,
    };
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
    const p = this._currentPartition;
    if (!p) return;

    if (p.isDirty) {
      wfs(p.fileName, p.documents);
      wfs(getMetaFilepath(this.name), this.meta);
    }
  }

  each(predicate: (doc: IDocument) => any): void {
    for (let partitionIndex = 0; partitionIndex < this.meta.partitions.length; partitionIndex++) {
      const docs = this.currentPartition.documents;
      let breakSignal = false;
      for (const strId in docs) {
        const doc = new Document(this, strId);

        if (breakSignal = predicate(doc) === false) break;
      }

      this.closePartition();
      if (breakSignal) break;
    }
  }

  getDocumentData(id: string): any[] {
    const data = this.currentPartition?.documents[id];
    if (!data) throw new Error(`wrong document id '${id}' (${Table.idNumber(id)})`);
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

  static idString(id: number): string {
    return String.fromCharCode(id);
  }

  static idNumber(id: string): number {
    return id.charCodeAt(0)
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