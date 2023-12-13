
import { rimraf } from "rimraf";
import { DataBase, SchemeFile, TableSettings } from "./db";
import { FieldType, Document, HeavyTypes, HeavyType, LightTypes, TDocument } from "./document";
import { PlainObject, rfs, wfs, existsSync, mkdirSync, statSync, renameSync, perfLog, perfStart, perfEnd, unlinkSync } from "./utils";

import { setFlagsFromString } from 'v8';
import { runInNewContext } from 'vm';

// setFlagsFromString('--expose_gc');




export type TableScheme = {
  fields: Record<string, FieldType>
  settings: TableSettings
  indices: string[]
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

type Callback<Type extends PlainObject, R> = (doc: TDocument<Type>) => R;

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
  return `/data/${tableName}/_meta.json`;
}

export function getPartitionFilePath(tableName: string, id: number): string {
  return `/data/${tableName}/part${id}.json`;
}

export function isHeavyType(type: FieldType): boolean {
  return HeavyTypes.includes(type as HeavyType);
}

export class Table<Type extends PlainObject = any> {
  protected meta: TableMetadata;
  protected _fieldNameIndex: Record<string, number> = {};
  protected _indexFieldName: string[] = [];

  protected _indexType: FieldType[] = [];
  protected _indexIndexType: FieldType[] = [];

  public readonly scheme: TableScheme;
  public readonly name: string;
  public readonly index: Record<string, any[]> = {};
  protected _dirtyIndexPartitions: Map<number, boolean> = new Map();

  constructor(name: string, scheme: TableScheme, meta: TableMetadata, index: PlainObject) {
    this.index = index;
    this.name = name;
    this.scheme = scheme;
    this.meta = meta;
    this.updateFieldIndices();
  }

  static arrangeSchemeIndices(fields: PlainObject, indices: string[]): string[] {
    const newIndices: string[] = [];
    for (const key in fields) {
      if (!fields[key]) throw new Error(`Field '${key}' is missing in the fields scheme, but is marked as index`);
      if (indices.includes(key)) newIndices.push(key);
    }
    return newIndices;
  }

  updateFieldIndices() {
    this._fieldNameIndex = {};
    this._indexFieldName = [];
    let i = 0;
    const { indices } = this.scheme;

    this.forEachField((key, type) => {
      if (isHeavyType(type)) return;
      const iOf = indices.indexOf(key);
      if (iOf != -1) {
        this._indexIndexType[i] = type;
      }

      this._indexType[i] = type;
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

  public get indexType() {
    return this._indexType;
  }

  public get indexIndexType() {
    return this._indexIndexType;
  }

  renameField(oldName: string, newName: string) {
    const { fields } = this.scheme;
    // console.log(oldName, newName);

    if (!fields[oldName]) throw new Error(`Field '${oldName}' doesn't exist`);
    if (fields[newName]) throw new Error(`Field '${newName}' already exists`);

    const { indices } = this.scheme;
    const indexIndex = indices.indexOf(oldName);
    if (indexIndex != -1) {
      indices.splice(indexIndex, 1, newName);
    }

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

  ifDo(match: Callback<Type, boolean>, predicate: Callback<Type, any>) {
    this.each(doc => {
      const matchRes = match(doc);
      if (matchRes) {
        predicate(doc);
      }
    });
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
    this.updateFieldIndices();
  }

  removeField(name: string) {
    const { fields, indices } = this.scheme;
    const type = fields[name];
    if (!type) throw new Error(`Field ${name} doesn't exist`);
    const iOfIndex = indices.indexOf(name);
    const isHeavy = isHeavyType(type);

    if (iOfIndex != -1) {
      this.removeIndexColumn(iOfIndex);
      this.scheme.indices.splice(iOfIndex, 1);
      this.saveIndexPartitions();
    } else if (isHeavy)
      rimraf.sync(`${process.cwd()}/data/${this.name}/${name}`);
    else {
      this.removeDocColumn(this._fieldNameIndex[name]);
    }

    delete this.scheme.fields[name];
    this.saveScheme();
  }

  getCurrentPartitionID(): number | undefined {
    return this._currentPartition?.id;
  }

  markCurrentPartitionDirty(): void {
    if (!this._currentPartition) throw new Error("Partitions is closed");
    this._currentPartition.isDirty = true;
  }

  markIndexPartitionDirty(id: number): void {
    this._dirtyIndexPartitions.set(id, true);
  }

  addField(name: string, type: FieldType, predicate?: Callback<Type, any>) {
    if (this.scheme.fields[name]) {
      throw new Error(`field '${name}' already exists`);
    }

    const filesDir = `/data/${this.name}/${name}/`;

    if (isHeavyType(type) && !existsSync(filesDir)) {
      mkdirSync(filesDir);
    }
    if (!predicate) {
      predicate = () => getDefaultValueForType(type);
    }

    this.insertDocColumn(this._indexFieldName.length, (idStr, partId) => (predicate as Function)(new Document(this, idStr, partId) as TDocument<Type>));

    this.scheme.fields[name] = type;
    this.saveScheme();
  }

  protected forEachPartition(predicate: (docs: DocumentData, current: Partition) => any, ids?: number[]) {
    const { partitions } = this.meta;
    if (!ids) ids = Array.from(Array(partitions.length).keys())
    for (const i of ids) {
      this.openPartition(i);
      const breakSignal = predicate(this.currentPartition.documents, this.currentPartition);

      this.closePartition();

      if (breakSignal === false) break;
    }
  }

  // select(predicate: Callback<Type, any>): Promise<TDocument<Type>[]>
  select(predicate: Callback<Type, boolean>, options?: { offset?: number, limit?: number }): TDocument<Type>[] {
    if (!options) options = {};
    options = {
      ...{
        offset: 0,
        limit: 100
      },
      ...options
    }
    const { offset, limit } = options as { offset: number, limit: number };

    let found = 0;
    const result: TDocument<Type>[] = [];
    this.each(doc => {
      const toSelect = predicate(doc as TDocument<Type>)
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

  remove(predicate: Callback<Type, boolean>) {
    this.each(doc => {
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

  find(predicate: Callback<Type, boolean>) {
    const found = this.select(predicate, {
      limit: 1
    });
    return found[0] || null;
  }

  clear() {
    if (!this.scheme.settings.dynamicData) throw new Error(`${this.name} is not a dynamic table`);;
    return this.remove(() => true);
  }

  removeByID(...ids: number[]) {
    this.ids(ids, doc => {
      this.removeDocumentFromCurrentPartition(doc);
    })
  }

  getHeavyFieldFilepath(id: number | string, type: HeavyType, fieldName: string): string {
    return `/data/${this.name}/${fieldName}/${id}.${type == "Text" ? "txt" : "json"}`;
  }

  private _currentPartition: Partition | undefined;
  protected openLastPartition() {
    const { partitions } = this.meta;
    if (!partitions.length) {
      this.createNewPartition();
    } else {
      this.openPartition(partitions.length - 1);
    }
  }
  protected get currentPartition(): Partition {
    if (!this._currentPartition) throw new Error("current partition is undefined!");
    return this._currentPartition;
  }

  /**
   * 
   * @param data - data to insert
   * @param predicate - filing indices function
   * @returns last inserted id string
   */
  insertSquare(data: [any[], any[]][]): string {
    this.openLastPartition();
    let docs = this.currentPartition.documents;
    let partId = this.currentPartition.id;
    const { meta } = this;
    if (!data.length) throw new Error("data is empty!");

    let idStr: string = "";
    for (let i = 0; i < data.length; i++) {
      const id = ++meta.index;
      idStr = Table.idString(id);
      meta.partitions[partId].length++;
      meta.partitions[partId].end = id;
      docs[idStr] = data[i][0];
      this.index[idStr] = data[i][1];
      this._dirtyIndexPartitions.set(partId, true);

      meta.length++;
      if (this.partitionExceedsSize()) {
        this.currentPartition.isDirty = true;
        this.createNewPartition();
        docs = this.currentPartition.documents;
        partId = this.currentPartition.id;
      }
    }

    this.save();
    this.saveIndexPartitions();
    return idStr; // we throw an error if data is empty so it can't be undefined or empty
  }

  protected removeDocColumn(fieldIndex: number): void
  protected removeDocColumn(fieldIndex: number, returnAsIndex: boolean): PlainObject
  protected removeDocColumn(fieldIndex: number, returnAsIndex: boolean = false) {
    const index: PlainObject = {};
    this.forEachPartition(docs => {
      this.currentPartition.isDirty = true;
      for (const idStr in docs) {
        const arr = docs[idStr];
        if (returnAsIndex) {
          index[idStr] = arr[fieldIndex];
        }
        arr.splice(fieldIndex, 1);
      }
    });
    this.closePartition();
    if (returnAsIndex) return index;
  }


  protected markAllIndexParitionsDirty() {
    const { partitions } = this.meta;
    for (let i = 0; i < partitions.length; i++) {
      this._dirtyIndexPartitions.set(i, true);
    }
  }
  /**
   * doesn't save index
   * @param fieldIndex 
   * @param returnAsIndex 
   * @returns 
   */
  protected removeIndexColumn(fieldIndex: number): void
  protected removeIndexColumn(fieldIndex: number, returnAsIndex: boolean): PlainObject
  protected removeIndexColumn(fieldIndex: number, returnAsIndex: boolean = false) {
    const result: PlainObject = {};
    this.markAllIndexParitionsDirty();
    for (const idStr in this.index) {
      const arr = this.index[idStr];
      if (returnAsIndex) {
        result[idStr] = arr[fieldIndex];
      }
      arr.splice(fieldIndex, 1);
    }
    if (returnAsIndex) return result;
  }

  /**
   * doesn't save index
   * @param fieldIndex 
   * @param predicate 
   */
  protected insertIndexColumn(fieldIndex: number, predicate: (idStr: string) => any) {
    this.markAllIndexParitionsDirty();
    for (const idStr in this.index) {
      const arr = this.index[idStr];
      arr.splice(fieldIndex, 0, predicate(idStr));
    }
  }

  protected insertDocColumn(fieldIndex: number, predicate: (idStr: string, partId: number) => any) {
    this.forEachPartition((docs, part) => {
      this.currentPartition.isDirty = true;
      for (const idStr in docs) {
        const arr = docs[idStr];
        arr.splice(fieldIndex, 0, predicate(idStr, part.id));
      }
    });
    this.closePartition();
  }

  createIndex(field: string, save = true): void {
    const { fields, indices } = this.scheme;
    if (!fields[field]) throw new Error(`Field '${field}' doen't exist`);
    const iOfIndex = indices.indexOf(field);
    if (iOfIndex != -1) throw new Error(`Field '${field}' is already an index`);
    const type = fields[field];
    if (isHeavyType(type)) {
      throw new Error(`Can't make heavy field '${field}' as index`);
    }

    const index: PlainObject = this.removeDocColumn(this._fieldNameIndex[field], true);

    this.insertIndexColumn(indices.length, id => index[id]);
    this.scheme.indices.push(field);

    this.updateFieldIndices();
    if (save) {
      this.saveIndexPartitions();
      this.saveScheme();
    }
  }

  removeIndex(field: string, save = true) {
    const { fields, indices } = this.scheme;

    if (!fields[field]) throw new Error(`Field '${field}' doen't exist`);
    const iOfIndex = indices.indexOf(field);
    if (iOfIndex == -1) throw new Error(`Index '${field}' doen't exist`);

    const indexData = this.removeIndexColumn(iOfIndex, true);
    indices.splice(iOfIndex, 1);
    this.updateFieldIndices();
    this.insertDocColumn(this._fieldNameIndex[field], id => indexData[id]);

    if (save) {
      this.saveIndexPartitions();
      this.saveScheme();
    }
  }

  static loadIndex(name: string, meta: TableMetadata, scheme: TableScheme) {
    if (!global.gc) throw new Error("Garbage collector is turned off. Run with --expose-gc");
    // const gc = runInNewContext('gc'); // nocommit
    // const gc = global.gc;
    const result: PlainObject = {};
    const { partitions } = meta;
    const { indices, fields } = scheme;
    let indexSize = 0;
    const types = indices.map(name => fields[name]);
    for (let i = 0; i < partitions.length; i++) {
      // await loadPartition(i);
      const fileName = Table.getIndexFilename(name, i);
      indexSize += statSync(fileName).size;
      // const part = rfs(fileName);
      // for (const id in part) {
      //   result[id] = part[id].map((value: any, i: number) => Document.retrieveValueOfType(value, types[i]));
      //   delete part[id];
      // }
      if (i % 150 == 0) {
        console.log("gc start");
        global.gc();
        console.log("gc end");
      }

      Object.assign(result, rfs(fileName));
    }

    setTimeout(() => {
      (global as any).gc();  
    }, 10 * 1000);
    
    console.log(Math.floor(100 * indexSize / 1024 / 1024 / 1024) / 100);
    return result;
  }

  static getIndexFilename(tableName: string, part: number) {
    return `/data/${tableName}/index_${part}.json`;
  }

  getIndexFilename(part: number) {
    return Table.getIndexFilename(this.name, part);
  }

  partitionExceedsSize(): boolean {
    const { settings } = this.scheme;
    const { size, meta } = this.currentPartition;

    if (settings.maxPartitionLenght && (meta.length > settings.maxPartitionLenght)) {
      return true;
    }
    if (settings.maxPartitionSize && (size > settings.maxPartitionSize)) {
      return true;
    }
    return false;
  }

  // insert<Type extends PlainObject>(data: Type): Document<Type>
  insert(data: Type) {
    const validationError = Document.validateData(data, this.scheme);
    if (validationError) throw new Error(`insert failed, data is invalid for reason '${validationError}'`);

    const idStr = this.insertSquare([this.flattenObject(data)]);

    this.forEachField((key, type) => {
      const value = data[key];
      if (isHeavyType(type) && !!value) {
        wfs(this.getHeavyFieldFilepath(idStr, type as HeavyType, key), value);
      }
    });
    return new Document(this, idStr, this.currentPartition.id) as TDocument<Type>;
  }

  createNewPartition(): void {
    this.closePartition();
    const meta: PartitionMeta = { length: 0, end: 0 };
    const id = this.meta.partitions.length;
    this.meta.partitions.push(meta);
    const fileName = getPartitionFilePath(this.name, id);
    wfs(fileName, {});
    wfs(this.getIndexFilename(id), {});
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

  flattenObject(o: PlainObject): [any[], any[]] {
    const lightValues: any[] = [];
    const indexValues: any[] = [];

    this._indexFieldName.forEach((key, i) => {
      const type = this._indexType[i];
      lightValues.push(Document.storeValueOfType(o[key], type));
    });

    this.scheme.indices.forEach((key, i) => {
      const type = this._indexIndexType[i];
      indexValues.push(Document.storeValueOfType(o[key], type));
    })

    return [lightValues, indexValues];
  }

  closePartition() {
    const p = this._currentPartition;
    if (!p) return;

    if (p.isDirty) {
      this.save();
    }
    this._currentPartition = undefined;
  }

  protected save() {
    const p = this._currentPartition;
    if (!p) throw new Error("no current partition");
    wfs(p.fileName, p.documents);
    wfs(getMetaFilepath(this.name), this.meta);
    p.isDirty = false;
  }

  public saveIndexPartitions() {
    let partId = 0;
    let indexPart: PlainObject = {};
    let { partitions } = this.meta;
    for (let id = 0; id < this.meta.length; id++) {
      if (!this._dirtyIndexPartitions.get(partId)) { //skip the partition if it's not dirty
        id = partitions[partId].end + 1;
        partId++;
        continue;
      }

      if (this.index[id]) // if id is valid save the data
        indexPart[id] = this.index[id];

      if (id >= this.meta.partitions[partId].end) {
        wfs(this.getIndexFilename(partId), indexPart);
        partId++;
        indexPart = {};
      }
    }
    if (partId < partitions.length) {
      wfs(this.getIndexFilename(partId), indexPart);
    }

    this._dirtyIndexPartitions.clear();
  }

  each(predicate: Callback<Type, any>) {
    let breakSignal: any;
    this.forEachPartition((docs, part) => {
      for (const strId in docs) {
        const doc = new Document<Type>(this, strId, part.id);
        breakSignal = predicate(doc as TDocument<Type>);
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

  static findPartitionForId(id: number, partitions: PartitionMeta[], tableLenght: number): number | false {
    const ratio = id / tableLenght;
    let startPartitionID = Math.floor(partitions.length * ratio);
    let startIndex = 0;
    while (startPartitionID > 0) {
      // startPartitionID--;
      startIndex = partitions[startPartitionID - 1].end;
      if (startIndex <= id) {
        break;
      }
      startPartitionID--;
    }

    for (let i = startPartitionID; i < partitions.length; i++) {
      const element = partitions[i];
      if (startIndex <= id && id <= element.end) return i;
      startIndex = element.end;
    }
    return false;
  }

  findPartitionForId(id: number): number | false {
    return Table.findPartitionForId(id, this.meta.partitions, this.meta.length);
  }
  /**
   * run predicate for eacn document with given ids
   * @param ids 
   * @param predicate 
   */
  ids(ids: number[], predicate: Callback<Type, any>) {
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
        if (!this.getDocumentData(id + "")) throw new Error("asdasds");

        predicate(new Document<Type>(this, Table.idString(id), pID) as TDocument<Type>);
      }
      this.closePartition();
    }
  }

  at(id: number) {
    const docs: TDocument<Type>[] = [];
    if (this.findPartitionForId(id) === false) {
      return null;
    }
    this.ids([id], doc => docs.push(doc));
    return docs[0] || null;
  }

  static idString(id: number): string {
    return id + '';
  }

  static idNumber(id: string): number {
    return (id as unknown as number) * 1;
  }



  toJSON() {
    if (this.scheme.settings.manyRecords) {
      throw new Error("You probably don't want to download a whole table");
    }
    return (this.select(() => true)).map(doc => doc.toJSON());
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