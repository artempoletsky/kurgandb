
import { rimraf } from "rimraf";
import { DataBase, SchemeFile, TableSettings } from "./db";
import { FieldType, Document, HeavyTypes, HeavyType, LightTypes, TDocument } from "./document";
import { PlainObject, rfs, wfs, existsSync, mkdirSync, statSync, renameSync } from "./utils";




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



type Callback<Type extends PlainObject, R> = (doc: TDocument<Type>) => R | Promise<R>;

// export interface ITable<Type extends PlainObject> {
//   name: string,
//   length: number
//   readonly scheme: TableScheme

//   each(predicate: Callback<Type, any>): Promise<void>
//   select(predicate: Callback<Type, any>, options?: {
//     offset?: number,
//     limit?: number
//   }): Promise<Document<Type>[]>
//   select(predicate: Callback<Type, any>): Promise<TDocument<Type>[]>
//   find(predicate: Callback<Type, boolean>): Promise<TDocument<Type> | null>
//   remove(predicate: Callback<Type, boolean>): Promise<void>
//   removeByID(...ids: number[]): Promise<void>
//   insert(data: Type): Promise<TDocument<Type>>
//   clear(): Promise<void>

//   ids(ids: number[], predicate: Callback<Type, void>): Promise<void>
//   at(id: number): Promise<Document<Type> | null>

//   getDocumentData(id: string): PlainObject // gets raw data from index JSON

//   addField(name: string, type: FieldType, predicate?: Callback<Type, any>): Promise<void>
//   removeField(name: string): Promise<void>
//   forEachField(predicate: (fieldName: string, type: string) => any): void
// }

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

export class Table<Type extends PlainObject> {
  protected meta: TableMetadata;
  protected _fieldNameIndex: Record<string, number> = {};
  protected _indexFieldName: string[] = [];

  public readonly scheme: TableScheme;
  public readonly name: string;

  constructor(name: string) {
    const scheme = DataBase.getScheme(name);
    if (!scheme) {
      throw new Error(`table '${name}' doesn't exist`);
    }
    this.name = name;
    this.scheme = scheme;

    this.updateIndexes();

    this.meta = rfs(getMetaFilepath(name));
  }

  updateIndexes() {
    this._fieldNameIndex = {};
    this._indexFieldName = [];
    let i = 0;
    this.forEachField((key, type) => {
      if (isHeavyType(type)) return;
      this._indexFieldName[i] = key;
      this._fieldNameIndex[key] = i++;
    });
  }

  public get fieldNameIndex() {
    return this._fieldNameIndex;
  }

  public get indexFieldName() {
    return this._indexFieldName;
  }

  async renameField(oldName: string, newName: string) {
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
    this.updateIndexes();
  }

  async removeField(name: string) {
    const type = this.scheme.fields[name];
    const isHeavy = isHeavyType(type);

    if (isHeavy)
      rimraf.sync(`${process.cwd()}/data/${this.name}/${name}`);
    else {
      const index = this.fieldNameIndex[name];
      await this.each(doc => {
        this.getDocumentData(doc.getStringID()).splice(index, 1);
        this.markCurrentPartitionDirty();
      });
      this.closePartition();
    }

    delete this.scheme.fields[name];
    this.saveScheme();
  }

  getCurrentPartitionID(): number {
    return this.currentPartition.id;
  }

  markCurrentPartitionDirty(): void {
    if (!this._currentPartition) throw new Error("current partition is undefined");
    this.currentPartition.isDirty = true;
  }

  async addField(name: string, type: FieldType, predicate?: Callback<Type, any>) {
    if (this.scheme.fields[name]) {
      throw new Error(`field '${name}' already exists`);
    }

    const filesDir = `/data/${this.name}/${name}/`;

    if (isHeavyType(type) && !existsSync(filesDir)) {
      mkdirSync(filesDir);
    }

    await this.forEachPartition((docs, current) => {
      for (const idStr in docs) {
        const data = docs[idStr];
        data.push(getDefaultValueForType(type));
        if (predicate) {
          const doc = new Document<Type>(this, idStr);
          const newValue = predicate(doc as TDocument<Type>);
          // doc.set(name, newValue);
          data[data.length - 1] = newValue;
        }
      }
      current.isDirty = true;
    });

    this.scheme.fields[name] = type;
    this.saveScheme();
  }

  protected async forEachPartition(predicate: (docs: DocumentData, current: Partition) => any, ids?: number[]) {
    const { partitions } = this.meta;
    if (!ids) ids = Array.from(Array(partitions.length).keys())
    for (const i of ids) {
      let breakSignal: boolean;
      this.openPartition(i);
      const result = predicate(this.currentPartition.documents, this.currentPartition);;
      if (result instanceof Promise) {
        breakSignal = await result;
      } else {
        breakSignal = result;
      }
      this.closePartition();

      if (breakSignal === false) break;
    }
  }

  // async select(predicate: Callback<Type, any>): Promise<TDocument<Type>[]>
  async select(predicate: Callback<Type, boolean>, options?: { offset?: number, limit?: number }): Promise<TDocument<Type>[]> {
    if (!options) options = {};
    const offset = options.offset || 0;
    const limit = options.limit || 0;
    let found = 0;
    const result: TDocument<Type>[] = [];
    await this.each(async doc => {
      const predRes = predicate(doc as TDocument<Type>);
      const toSelect = predRes instanceof Promise ? await predRes : predRes;
      if (toSelect) {
        result.push(doc);
        found++;
        if (limit && (limit + offset) <= found) {
          return false;
        }
      }
    });
    return result;
  }

  async remove(predicate: Callback<Type, boolean>) {
    return this.each(doc => {
      if (predicate(doc)) {
        this.removeDocumentFromCurrentPartition(doc);
      }
    });
  }

  removeDocumentFromCurrentPartition(doc: Document<Type>) {
    const partition = this.currentPartition;
    if (!partition) return;
    delete partition.documents[doc.getStringID()];
    this.meta.partitions[partition.id].length--;
    partition.isDirty = true;
  }

  async find(predicate: Callback<Type, boolean>) {
    const found = await this.select(predicate, {
      limit: 1
    });
    return found[0] || null;
  }

  async clear() {
    if (!this.scheme.settings.dynamicData) throw new Error(`${this.name} is not a dynamic table`);;
    return this.remove(() => true);
  }

  async removeByID(...ids: number[]) {
    await this.ids(ids, doc => {
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
    } else {
      this.openPartition(partitions.length - 1);
    }

    if (!this._currentPartition) throw new Error("never");
    return this._currentPartition;
  }

  // insert<Type extends PlainObject>(data: Type): Document<Type>
  async insert(data: Type) {
    const validationError = Document.validateData(data, this.scheme);
    if (validationError) throw new Error(`insert failed, data is invalid for reason '${validationError}'`);

    const id = this.meta.index++;
    const idStr = Table.idString(id);

    this.forEachField((key, type) => {
      const value = data[key];
      if (isHeavyType(type) && !!value) {
        wfs(this.getHeavyFieldFilepath(id, type as HeavyType, key), value);
      } if (type == "date" && data[key] instanceof Date) {
        (data as PlainObject)[key] = data[key].toJSON();
      }
    });
    const { partitions } = this.meta;
    const { settings } = this.scheme;


    if (!partitions.length || partitions[partitions.length - 1].length >= settings.maxPartitionLenght) {
      if (settings.maxPartitionLenght)
        this.createNewPartition();
    }

    let p = this.currentPartition;

    p.documents[idStr] = this.squarifyObject(data);
    const doc: TDocument<Type> = new Document<Type>(this, idStr) as TDocument<Type>;
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

    if (this._currentPartition && this._currentPartition.id == index) return;

    this.closePartition();
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
    this._currentPartition = undefined;
  }

  async each(predicate: Callback<Type, any>) {
    let breakSignal: any;
    await this.forEachPartition(async docs => {
      for (const strId in docs) {
        const doc = new Document<Type>(this, strId);
        const result = predicate(doc as TDocument<Type>);
        if (result instanceof Promise) {
          breakSignal = await result;
        } else {
          breakSignal = result
        }
        if (breakSignal === false) break;
      }
      if (breakSignal === false) return false;
    });
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
  /**
   * run predicate for eacn document with given ids
   * @param ids 
   * @param predicate 
   */
  async ids(ids: number[], predicate: Callback<Type, any>) {
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
        predicate(new Document<Type>(this, Table.idString(id)) as TDocument<Type>);
      }
      this.closePartition();
    }
  }

  async at(id: number) {
    const docs: TDocument<Type>[] = [];
    if (this.findPartitionForId(id) === false) {
      return null;
    }
    await this.ids([id], doc => docs.push(doc));
    return docs[0] || null;
  }

  static idString(id: number): string {
    return String.fromCharCode(id);
  }

  static idNumber(id: string): number {
    return id.charCodeAt(0)
  }



  async toJSON() {
    if (this.scheme.settings.manyRecords) {
      throw new Error("You probably don't want to download a whole table");
    }
    return (await this.select(() => true)).map(doc => doc.toJSON());
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