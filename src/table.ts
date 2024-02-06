
import { DataBase, SCHEME_PATH, SchemeFile, TableSettings } from "./db";
import { FieldType, Document, LightTypes, TDocument } from "./document";
import { PlainObject, rfs, wfs, existsSync, mkdirSync, renameSync, rmie, $ } from "./utils";

import FragmentedDictionary, { FragmentedDictionarySettings, IDFilter, PartitionFilter, PartitionMeta } from "./fragmented_dictionary";
import TableQuery, { twoArgsToFilters } from "./table_query";
import SortedDictionary from "./sorted_dictionary";
import _, { flatten } from "lodash";
import { CallbackScope } from "./client";

// setFlagsFromString('--expose_gc');

export function parseFunctionArguments(args: string): string[] {
  args = args.replace(/\s/g, "");
  if (args == "") return [];

  const ArgExp = /{[^}]*}/g;
  const execRes = args.match(ArgExp);

  if (!execRes) throw new Error("can't parse arguments");
  return execRes;
}

export function packEventListener(handler: (...args: any[]) => void): string[] {
  const MainExp = /^[^(]*\(([^)]*)\)[^{]*\{([\s\S]*)\}\s*$/

  const execRes = MainExp.exec(handler.toString());

  if (!execRes) throw new Error("can't parse function");
  const body = execRes[2].trim();

  function parseArguments(str: string) {


    // console.log(str);
  }

  const args = parseFunctionArguments(execRes[1]);



  return [...args, body];
}

export type FieldTag = "primary" | "unique" | "index" | "memory" | "textarea" | "heavy" | "hidden";


export type TableScheme = {
  fields: Record<string, FieldType>
  tags: Record<string, FieldTag[]>
  settings: TableSettings
};


export type DocumentData = Record<string, any[]>;
export type Partition<Type> = {
  isDirty: boolean
  documents: DocumentData
  fileName: string
  id: number
  size: number
  meta: PartitionMeta<Type>
}


export type IndicesRecord = Record<string, FragmentedDictionary<string | number, any>>
export type MainDict<KeyType extends string | number> = FragmentedDictionary<KeyType, any[]>

export type DocCallback<KeyType extends string | number, Type, ReturnType> = (doc: TDocument<KeyType, Type>) => ReturnType;



export type IndexFlags = {
  isUnique: boolean,
};

export function getDefaultValueForType(type: FieldType) {
  switch (type) {
    case "number": return 0;
    case "string": return "";
    case "json": return null;
    case "date": return new Date();
    case "boolean": return false;
  }
}

export function getMetaFilepath(tableName: string): string {
  return `/${tableName}/_meta.json`;
}


export type DocumentChangeEvent<KeyType extends string | number, Type, MetaType> = {
  docData: Type,
  meta: MetaType,
  field: keyof Type,
  oldValue: any,
  newValue: any,
  table: Table<KeyType, Type, MetaType>,
}

export type DocumentInsertEvent<KeyType extends string | number, Type, MetaType> = {
  docData: Type,
  meta: MetaType,
  table: Table<KeyType, Type, MetaType>,
}

export type DocumentRemoveEvent<KeyType extends string | number, Type, MetaType> = {
  docData: Type,
  meta: MetaType,
  table: Table<KeyType, Type, MetaType>,
}

export type TableOpenEvent<KeyType extends string | number, Type, MetaType> = {
  meta: MetaType,
  table: Table<KeyType, Type, MetaType>,
}

export class Table<KeyType extends string | number, Type, MetaType = {}> {
  protected mainDict: MainDict<KeyType>;
  protected indices: IndicesRecord;
  protected memoryFields: Record<string, SortedDictionary<KeyType, any>>;
  protected _fieldNameIndex: Record<string, number> = {};
  protected _indexFieldName: string[] = [];
  protected customPrimaryKey: string | undefined;

  protected _indexType: FieldType[] = [];
  protected _indexIndexType: FieldType[] = [];

  public readonly scheme: TableScheme;
  public readonly name: string;
  public readonly primaryKey: string;
  protected _dirtyIndexPartitions: Map<number, boolean> = new Map();

  constructor(name: string, scheme: TableScheme) {
    this.primaryKey = "id";
    this.name = name;
    this.scheme = scheme;
    this.indices = {};

    for (const fieldName in scheme.tags) {
      // const fieldTags = tags[fieldName];
      if (this.fieldHasAnyTag(<keyof Type & string>fieldName, "index", "unique")) {
        this.indices[fieldName] = FragmentedDictionary.open(this.getIndexDictDir(fieldName));
      }
      if (this.fieldHasAnyTag(<keyof Type & string>fieldName, "primary")) {
        this.primaryKey = fieldName;
      }
    }

    this.mainDict = FragmentedDictionary.open(`/${name}/main/`);
    this.memoryFields = {};
    this.loadMemoryIndices();
    this.updateFieldIndices();
  }

  static fieldTypeToKeyType(type: FieldType): "int" | "string" {
    if (type == "json") throw new Error("wrong type");
    if (type == "string") return "string";
    return "int";
  }

  static tagsHasFieldNameWithAnyTag(tags: Record<string, FieldTag[]>, fieldName: string, ...tagsToFind: FieldTag[]) {
    if (!tags[fieldName]) return false;
    for (const tag of tagsToFind) {
      if (tags[fieldName].includes(tag)) {
        return true;
      }
    }
    return false;
  }

  static tagsHasFieldNameWithAllTags(tags: Record<string, FieldTag[]>, fieldName: string, ...tagsToFind: FieldTag[]) {
    if (!tags[fieldName]) return false;
    for (const tag of tagsToFind) {
      if (!tags[fieldName].includes(tag)) {
        return false;
      }
    }
    return true;
  }

  protected forEachIndex(predicate: (fieldName: string, dict: FragmentedDictionary<string | number, any>, flags: IndexFlags, tags: FieldTag[]) => void) {
    for (const fieldName in this.scheme.tags) {
      const tags = this.scheme.tags[fieldName];
      if (this.fieldHasAnyTag(<keyof Type & string>fieldName, "unique", "index")) {
        predicate(fieldName, this.indices[fieldName], {
          isUnique: tags.includes("unique")
        }, tags);
      }
    }
  }

  fieldHasAnyTag(fieldName: string, ...tags: FieldTag[]) {
    return Table.tagsHasFieldNameWithAnyTag(this.scheme.tags, fieldName, ...tags);
  }

  fieldHasAllTags(fieldName: string, ...tags: FieldTag[]) {
    return Table.tagsHasFieldNameWithAllTags(this.scheme.tags, fieldName, ...tags);
  }

  protected loadMemoryIndices() {
    const { tags } = this.scheme;
    for (const fieldName in tags) {
      const fieldTags = tags[fieldName];
      if (fieldTags.includes("memory")) {
        // TODO: implement
        // this.memoryIndices[fieldName] = this.indices[fieldName].loadAll();
      }
    }
  }

  protected updateFieldIndices() {
    this._fieldNameIndex = {};
    this._indexFieldName = [];
    let i = 0;

    this.forEachField((key, type, tags) => {
      if (tags.has("heavy")) return;
      if (key == this.primaryKey) return;

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

  protected createQuery() {
    return new TableQuery<KeyType, Type>(this, this.indices, this.mainDict);
  }

  whereRange(fieldName: (keyof Type | "id") & string, min: any, max: any): TableQuery<KeyType, Type> {
    return this.createQuery().whereRange(fieldName as any, min, max);
  }

  where<FieldType extends string | number>(fieldName: keyof Type | "id",
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): TableQuery<KeyType, Type>
  where<FieldType extends string | number>(fieldName: keyof Type | "id", ...values: FieldType[]): TableQuery<KeyType, Type>
  where<FieldType extends string | number>(fieldName: any, ...args: any[]) {
    return this.createQuery().where<FieldType>(fieldName, ...args);
  }

  filter(predicate: DocCallback<KeyType, Type, boolean>): TableQuery<KeyType, Type> {
    return this.createQuery().filter(predicate);
  }

  all() {
    return this.createQuery().whereRange(this.primaryKey as any, undefined, undefined);
  }

  renameField(oldName: string, newName: string) {
    const { fields, tags } = this.scheme;
    // console.log(oldName, newName);

    if (!fields[oldName]) throw new Error(`Field '${oldName}' doesn't exist`);
    if (fields[newName]) throw new Error(`Field '${newName}' already exists`);

    if (this.fieldHasAnyTag(oldName as any, "index", "unique", "memory")) {
      FragmentedDictionary.rename(this.getIndexDictDir(oldName), this.getIndexDictDir(newName));
    }

    const fieldTags = tags[oldName];
    if (fieldTags) {
      delete tags[oldName];
      tags[newName] = fieldTags;
    }

    const indexDict = this.indices[oldName];
    if (indexDict) {
      delete this.indices[oldName];
      this.indices[newName] = indexDict;
    }

    const newFields: Record<string, FieldType> = {};
    this.forEachField((name, type, tags) => {///iterate to keep the same order in the javascript dictionary
      if (name == oldName) {
        newFields[newName] = type;
        if (tags.has("heavy")) {
          //rename the folder dedicated to the heavy field
          renameSync(this.getHeavyFieldDir(oldName), this.getHeavyFieldDir(newName));
        }
      } else {
        newFields[name] = type;
      }
    });

    this.scheme.fields = newFields;
    this.saveScheme();
  }

  getLastIndex() {
    return this.mainDict.end;
  }

  forEachField(predicate: (fieldName: string & keyof Type, type: FieldType, tags: Set<FieldTag>) => any): void {
    const fields = this.scheme.fields;
    for (const key in fields) {
      predicate(key as any, fields[key], new Set(this.scheme.tags[key]));
    }
  }

  public get length(): number {
    return this.mainDict.length;
  }


  protected saveScheme() {
    const schemeFile: SchemeFile = rfs(SCHEME_PATH);
    schemeFile.tables[this.name] = this.scheme;
    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });
    this.updateFieldIndices();
  }

  printField(fieldName?: string) {
    if (!fieldName) return `'${this.name}[${this.primaryKey}]'`;
    return `field '${this.name}[${this.primaryKey}].${fieldName}'`;
  }

  static createIndexDictionary(tableName: string, fieldName: string, tags: FieldTag[], type: FieldType) {
    const directory = `/${tableName}/indices/${fieldName}/`;

    if (type == "json") throw new Error(`Can't create an index of json type field ${fieldName}`);

    let keyType = this.fieldTypeToKeyType(type);

    let settings: Record<string, any> = {
      maxPartitionLenght: 10 * 1000,
      maxPartitionSize: 0,
    };

    return FragmentedDictionary.init({
      directory,
      keyType,
      ...settings,
    });

  }
  protected throwAlreadyIndex(fieldName: string) {
    throw new Error(`${this.printField(fieldName)} is already an index!`);
  }

  createIndex(fieldName: string & keyof Type, unique: boolean) {
    if (this.indices[fieldName]) this.throwAlreadyIndex(fieldName);
    const type = this.scheme.fields[fieldName];

    const tags: FieldTag[] = unique ? ["unique"] : ["index"];
    this.indices[fieldName] = Table.createIndexDictionary(this.name, fieldName, tags, type);
    this.scheme.tags[fieldName] = tags;

    const indexData: Map<string | number, KeyType[]> = new Map();
    // const ids: KeyType[] = [];
    // const col: string[] | number[] = [];
    const fIndex = this._fieldNameIndex[fieldName];
    this.mainDict.iterateRanges({
      filter: (arr, id) => {
        try {
          this.fillIndexData(indexData, arr[fIndex], id, unique ? fieldName : undefined);
        } catch (error) {
          this.removeIndex(fieldName);
          throw error;
        }
        return false;
      }
    });

    this.insertColumnToIndex(fieldName, indexData);

    this.saveScheme();
  }

  protected fillIndexData
    <ColType extends string | number>(
      indexData: Map<ColType, KeyType[]>,
      value: ColType, id: KeyType, throwUnique?: string) {
    const toPush = indexData.get(value);

    if (throwUnique && toPush) {
      throw this.errorValueNotUnique(throwUnique, value);
    }

    if (!toPush) {
      indexData.set(value, [id]);
    } else {
      toPush.push(id);
    }
  }

  removeIndex(name: string) {
    if (!this.indices[name]) throw new Error(`${this.printField(name)} is not an index field!`);

    this.indices[name].destroy();
    delete this.indices[name];
    delete this.scheme.tags[name];

    this.saveScheme();
  }

  removeField(fieldName: string) {
    const { fields, tags } = this.scheme;
    const type = fields[fieldName];
    if (!type) throw new Error(`${this.printField(fieldName)} doesn't exist`);


    if (fieldName == this.primaryKey) throw new Error(`Can't remove the primary key ${this.printField(fieldName)}! Create a new table instead.`);

    if (this.fieldHasAnyTag(fieldName, "index", "unique")) {
      this.removeIndex(fieldName);
    }

    if (this.fieldHasAnyTag(fieldName, "heavy"))
      rmie(this.getHeavyFieldDir(fieldName));
    else {
      this.removeDocColumn(this._fieldNameIndex[fieldName]);
    }

    delete this.scheme.fields[fieldName];
    this.saveScheme();
  }


  addField(fieldName: string, type: FieldType, predicate?: DocCallback<KeyType, Type, any>) {
    if (this.scheme.fields[fieldName]) {
      throw new Error(`field '${fieldName}' already exists`);
    }

    const filesDir = this.getHeavyFieldDir(fieldName);

    if (this.fieldHasAnyTag(fieldName, "heavy") && !existsSync(filesDir)) {
      mkdirSync(filesDir);
    }

    const definedPredicate = predicate || (() => getDefaultValueForType(type));

    this.insertDocColumn(this._indexFieldName.length, (id, arr) => {
      return definedPredicate(new Document<KeyType, Type>(arr, id, this, this.indices) as TDocument<KeyType, Type>);
    });

    this.scheme.fields[fieldName] = type;
    this.saveScheme();
  }

  clear() {
    if (!this.scheme.settings.dynamicData) throw new Error(`${this.name} is not a dynamic table`);;
    return this.filter(() => true).delete();
  }

  getIndexDictDir(fieldName: string) {
    return `/${this.name}/indices/${fieldName}/`;
  }

  getMainDictDir() {
    return `/${this.name}/main/`;
  }

  getHeavyFieldDir(fieldName: string) {
    return `/${this.name}/heavy/${fieldName}/`;
  }

  getHeavyFieldFilepath(id: number | string, type: FieldType, fieldName: string): string {
    return `${this.getHeavyFieldDir(fieldName)}${id}.${type == "json" ? "json" : "txt"}`;
  }

  createDefaultObject(): Type {
    const result: PlainObject = {};
    this.forEachField((fieldName, type) => {
      result[fieldName] = getDefaultValueForType(type);
    });
    return result as Type;
  }

  // // /**
  // //  * 
  // //  * @param data - data to insert
  // //  * @param predicate - filing indices function
  // //  * @returns last inserted id string
  // //  */
  // insertSquare(data: any[][]): string[] | number[] {
  //   return this.mainDict.insertArray(data) as string[] | number[]; //TODO: take the last id from the table instead of the dict
  // }

  protected removeDocColumn(fieldIndex: number): void
  protected removeDocColumn(fieldIndex: number, returnAsDict: true): PlainObject
  protected removeDocColumn(fieldIndex: number, returnAsDict: boolean = false) {
    const result: Record<string | number, any> = {};
    this.mainDict.editRanges([[undefined, undefined]], (arr, id) => {
      const newArr = [...arr];
      if (returnAsDict) {
        result[id] = newArr[fieldIndex];
      }
      newArr.splice(fieldIndex, 1);
      return newArr;
    }, 0);

    if (returnAsDict) return result;
  }

  removeIdFromIndex(docID: KeyType, data: any[]) {
    this.forEachIndex((fieldName) => {
      const value = data[this._fieldNameIndex[fieldName]];
      this.unstoreIndexValue(fieldName, docID, value);
    });
  }

  storeIndexValue(fieldName: string, docID: KeyType, value: string | number) {
    this.changeIndexValue(fieldName, docID, undefined, value);
  }

  unstoreIndexValue(fieldName: string, docID: KeyType, value: string | number) {
    this.changeIndexValue(fieldName, docID, value, undefined);
  }

  changeIndexValue(fieldName: string, docID: KeyType, oldValue: undefined | string | number, newValue: undefined | string | number) {
    if (oldValue == newValue) return;
    const indexDict = this.indices[fieldName];
    if (!indexDict) {
      return;
    }
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");


    if (isUnique) {
      if (newValue !== undefined && indexDict.getOne(newValue)) throw new Error(`Attempting to create a duplicate in the unique ${this.printField(fieldName)}`);

      if (oldValue !== undefined) {
        indexDict.remove([oldValue]);
      }
      if (newValue !== undefined) {
        indexDict.setOne(newValue, docID);
      }
    } else {
      if (oldValue !== undefined) {
        const arr: KeyType[] = indexDict.getOne(oldValue) || [];
        // arr.push(id);
        arr.splice(arr.indexOf(docID), 1);
        if (arr.length) {
          indexDict.setOne(oldValue, arr);
        } else {
          indexDict.remove([oldValue]);
        }
      }

      if (newValue !== undefined) {
        const arr: KeyType[] = indexDict.getOne(newValue) || [];
        arr.push(docID);
        indexDict.setOne(newValue, arr);
      }
    }
  }


  canInsertUnique<ColumnType extends string | number>(fieldName: string, column: ColumnType[], throwError: boolean = false): boolean {
    const indexDict = this.indices[fieldName];
    if (!indexDict) throw new Error(`${this.printField(fieldName)} is not an index`);
    const found = indexDict.filterSelect(column.map(v => [v, v]), 1);
    if (found.length == 0) {
      return true;
    }
    if (throwError) throw this.errorValueNotUnique(fieldName, found[0]);
    return false;
  }

  protected errorValueNotUnique(fieldName: string, value: any) {
    if (fieldName == this.primaryKey) {
      return new Error(`Primary key value '${value}' on ${this.printField()} already exists`)
    }
    return new Error(`Unique value '${value}' for ${this.printField(fieldName)} already exists`);
  }

  public insertMany(data: Type[]): KeyType[] {
    const storable: PlainObject[] = [];

    // perfStart("make storable");
    for (const obj of data) {
      const validationError = Document.validateData(obj, this.scheme);
      if (validationError) throw new Error(`insert failed, data is invalid for reason '${validationError}'`);

      storable.push(this.makeObjectStorable(obj));
    }
    // perfEndLog("make storable");

    const indexColumns: Record<string, any[]> = {};
    this.forEachIndex((fieldName, indexDict, { isUnique }) => {
      const col = indexColumns[fieldName] = storable.map(o => o[fieldName]);
      if (isUnique) {
        this.canInsertUnique(fieldName, col, true);
      }
    });

    let ids: KeyType[];
    const values = storable.map(o => this.flattenObject(o));

    if (this.primaryKey == "id" && this.mainDict.settings.keyType == "int") {
      ids = this.mainDict.insertArray(values);
    } else {
      ids = storable.map(o => {
        const val = <KeyType>o[this.primaryKey];
        delete o[this.primaryKey];
        return val;
      });

      const exisiting = this.mainDict.hasAnyId(ids);
      if (exisiting !== false) {
        throw this.errorValueNotUnique(this.primaryKey, exisiting);
      }

      this.mainDict.insertMany(ids, values);
    }

    this.forEachField((key, type, tags) => {
      if (tags.has("heavy")) {
        for (let i = 0; i < storable.length; i++) {
          const value = storable[i][key];
          if (value) {
            wfs(this.getHeavyFieldFilepath(ids[i], type, key), value);
          }
        }
      }
    });

    // perfStart("indicies update");
    this.forEachIndex((fieldName) => {
      const indexData: Map<string | number, KeyType[]> = new Map();
      for (let i = 0; i < ids.length; i++) {
        this.fillIndexData(indexData, indexColumns[fieldName][i], ids[i]);
      }
      this.insertColumnToIndex(fieldName, indexData);
    });
    // perfEndLog("indicies update");
    return ids;
  }


  protected errorFieldNotIndex(fieldName: string) {
    return new Error(`${this.printField(fieldName)} is not an index!`);
  }

  insertColumnToIndex<ColType extends string | number>(fieldName: string, indexData: Map<ColType, KeyType[]>) {
    const column = Array.from(indexData.keys());
    const indexDict: FragmentedDictionary<ColType, any> = this.indices[fieldName] as any;
    if (!indexDict) throw this.errorFieldNotIndex(fieldName);
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");
    if (isUnique) {
      const ids = Array.from(indexData.values()).map(arr => arr[0]);
      indexDict.insertMany(column, ids);
    } else {
      indexDict.iterateRanges({
        ranges: Array.from(indexData.keys()).map(v => [v, v]),
        update: (arr, id) => {
          const toPush = indexData.get(id);
          indexData.delete(id);
          return arr.concat.apply(arr, toPush);
        },
      });

      indexDict.insertMany(Array.from(indexData.keys()), Array.from(indexData.values()));
    }
  }

  // insert<Type extends PlainObject>(data: Type): Document<Type>
  public insert(data: PlainObject & Type): KeyType {
    return this.insertMany([data])[0];
  }



  expandObject(arr: any[]) {
    const result: PlainObject = {};
    let i = 0;
    this.forEachField((key, type, tags) => {
      if (tags.has("heavy")) return;
      // if (type == "boolean") {
      //   result[key] = !!arr[i];
      //   return;
      // }
      result[key] = arr[i];
      i++;
    });
    return result;
  }

  makeObjectStorable(o: Type): PlainObject {
    const result: PlainObject = {};
    this.forEachField((key, type) => {
      result[key] = Document.storeValueOfType(o[key], type as any);
    });
    return result;
  }

  flattenObject(o: PlainObject): any[] {
    const lightValues: any[] = [];

    this._indexFieldName.forEach((key, i) => {
      lightValues.push(o[key]);
    });

    return lightValues;
  }

  protected insertDocColumn(fieldIndex: number, predicate: (id: KeyType, arr: any[]) => any) {
    this.mainDict.editRanges([[undefined, undefined]], (arr, id) => {
      const newArr = [...arr];
      newArr.splice(fieldIndex, 0, predicate(id, newArr));
      return newArr;
    }, 0);
  }

  at(id: KeyType): Type
  at<ReturnType = Type>(id: KeyType, predicate?: DocCallback<KeyType, Type, ReturnType>): ReturnType
  public at<ReturnType>(id: KeyType, predicate?: DocCallback<KeyType, Type, ReturnType>) {
    const res = this.where(this.primaryKey as any, id).limit(1).select(predicate);
    if (res.length == 0) throw new Error(`id '${id}' doesn't exists at ${this.printField()}`);
    return res[0];
  }

  atIndex(index: number): Type | null
  atIndex<ReturnType = Type>(index: number, predicate?: DocCallback<KeyType, Type, ReturnType>): ReturnType | null
  public atIndex<ReturnType>(index: number, predicate?: DocCallback<KeyType, Type, ReturnType>) {
    const id = this.mainDict.keyAtIndex(index);
    if (id === undefined) return null;
    return this.at(id, predicate);
  }

  toJSON() {
    if (this.scheme.settings.manyRecords) {
      throw new Error("You probably don't want to download a whole table");
    }
    return this.all().limit(0).select();
  }

  getFieldsOfType(...types: FieldType[]): string[] {
    const result: string[] = [];
    this.forEachField((key, type) => {
      if (types.includes(type))
        result.push(key);
    });
    return result;
  }

  getFieldsWithAnyTags(...tags: FieldTag[]): string[] {
    const result: string[] = [];
    this.forEachField((key) => {
      if (this.fieldHasAnyTag(key, ...tags)) {
        result.push(key);
      }
    });
    return result;
  }

  getFieldsWithoutAnyTags(...tags: FieldTag[]): string[] {
    const result: string[] = [];
    this.forEachField((key) => {
      if (!this.fieldHasAnyTag(key, ...tags)) {
        result.push(key);
      }
    });
    return result;
  }

  getLightKeys(): string[] {
    return this.getFieldsWithoutAnyTags("heavy");
  }

  getHeavyKeys(): string[] {
    return this.getFieldsWithAnyTags("heavy");
  }

  indexIds<FieldType extends string | number>(fieldName: keyof Type | "id",
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): KeyType[]
  indexIds<FieldType extends string | number>(fieldName: keyof Type | "id", ...values: FieldType[]): KeyType[]
  indexIds<FieldType extends string | number>(fieldName: any, ...args: any[]) {
    const index = this.indices[fieldName];
    if (!index) throw this.errorFieldNotIndex(fieldName);

    const [idFilter, partitionFilter] = twoArgsToFilters(args);
    const res = index.where({
      idFilter,
      partitionFilter,
      limit: 0,
      select: val => val
    })[0];

    return flatten(Object.values(res));
  }

  public get meta(): MetaType {
    return this.mainDict.meta.custom.$table;
  }

  protected events: Record<string, Record<string, Function>> = {};

  protected unpackEventListeners() {
    const listeners = this.mainDict.meta.custom.$serviceListeners || {};
    const { events } = this;
    for (const eventName in listeners) {
      events[eventName] = {};
      for (const handlerId in listeners[eventName]) {
        events[eventName][handlerId] = new Function(listeners[eventName][handlerId]);
      }
    }
  }

  registerEventListener(handlerId: string, eventName: "tableOpen", handler: (event: TableOpenEvent<KeyType, Type, MetaType>, scope: CallbackScope) => void): void
  registerEventListener(handlerId: string, eventName: "documentRemove", handler: (event: DocumentRemoveEvent<KeyType, Type, MetaType>, scope: CallbackScope) => void): void
  registerEventListener(handlerId: string, eventName: "documentInsert", handler: (event: DocumentInsertEvent<KeyType, Type, MetaType>, scope: CallbackScope) => void): void
  registerEventListener(handlerId: string, eventName: "documentChange", handler: (event: DocumentChangeEvent<KeyType, Type, MetaType>, scope: CallbackScope) => void): void
  registerEventListener(handlerId: string, eventName: string, handler: (event: any, scope: CallbackScope) => void): void {
    let listeners = this.events[eventName];
    const { $serviceListeners } = this.mainDict.meta.custom;
    if (!listeners) {
      listeners = this.events[eventName] = {};
      $serviceListeners[eventName] = {};
    }
    listeners[handlerId] = handler;
    $serviceListeners[handlerId] = packEventListener(handler);
    this.mainDict.meta.custom.$serviceListeners = $serviceListeners;
    if (eventName == "tableOpen") {
      const meta = {
        ...this.meta
      };
      const e: TableOpenEvent<KeyType, Type, MetaType> = {
        meta,
        table: this,
      };
      handler(e, {
        $: $,
        _: _,
        db: DataBase,
      });
      this.mainDict.meta.custom.$table = meta;
    }
  }

  unregisterEventListener(handlerId: string, eventName: "documentRemove" | "documentInsert" | "documentChange" | undefined) {
    const { events } = this;
    if (eventName === undefined) {
      for (const name in events) {
        this.unregisterEventListener(handlerId, name as any);
      }
      return;
    }
    delete events[eventName][handlerId];
    const { $serviceListeners } = this.mainDict.meta.custom;
    delete $serviceListeners[eventName][handlerId];
    this.mainDict.meta.custom.$serviceListeners = $serviceListeners;
  }

  triggerEvent(eventName: "tableOpen", handler: (event: TableOpenEvent<KeyType, Type, MetaType>) => void): void
  triggerEvent(eventName: "documentRemove", handler: (event: DocumentRemoveEvent<KeyType, Type, MetaType>) => void): void
  triggerEvent(eventName: "documentInsert", handler: (event: DocumentInsertEvent<KeyType, Type, MetaType>) => void): void
  triggerEvent(eventName: "documentChange", handler: (event: DocumentChangeEvent<KeyType, Type, MetaType>) => void): void
  triggerEvent(eventName: string, event: any): void {
    const listeners = this.events[eventName];
    if (!listeners) return;
    for (const handlerId in listeners) {
      listeners[handlerId](event, {
        $: $,
        _: _,
        db: DataBase,
      });
    }
  }

  toggleFieldHeavy(fieldName: string) {
    throw new Error("not implemented");
  }

  toggleTag(fieldName: string, tag: FieldTag) {
    if (tag == "heavy") {
      this.toggleFieldHeavy(fieldName);
      return;
    }

    let tags = this.scheme.tags[fieldName] || [];
    let set = new Set(tags);

    if (set.has(tag)) {
      if (tag == "unique" || tag == "index") {
        this.removeIndex(fieldName);
        return;
      } else {
        set.delete(tag);
      }
    } else {
      if (tag == "unique" || tag == "index") {
        this.createIndex(<any>fieldName, tag == "unique");
        return;
      } else {
        set.add(tag);
      }
    }
    this.scheme.tags[fieldName] = Array.from(set);
    this.saveScheme();
  }

  has(...ids: KeyType[]) {
    return this.mainDict.hasAllIds(ids);
  }


  getFreeId(): KeyType {
    if (this.mainDict.settings.keyType == "string") {
      let idCandidiate = "new_id";
      let tries = 0;
      while (this.has(<KeyType>idCandidiate)) {
        tries++;
        idCandidiate = "new_id_" + tries;
      }
      return idCandidiate as KeyType;
    } else {
      throw new Error("not implemented yet");
    }
  }

  getDocumentDraft(): Type {
    const res = {} as Type & PlainObject;
    this.forEachField((fieldName, type) => {
      res[fieldName] = fieldName == this.primaryKey ? this.getFreeId() : getDefaultValueForType(type);
    });

    return res;
  }

}