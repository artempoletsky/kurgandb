
import { DataBase, SCHEME_PATH, SchemeFile, TableSettings } from "./db";
import { Document, TDocument } from "./document";
import { rfs, wfs, existsSync, mkdirSync, renameSync, rmie, logError } from "./utils";


import FragmentedDictionary, { FragmentedDictionarySettings, IDFilter, PartitionFilter, PartitionMeta } from "./fragmented_dictionary";
import TableQuery, { twoArgsToFilters } from "./table_query";
import SortedDictionary from "./sorted_dictionary";
import _, { flatten } from "lodash";
import { CallbackScope } from "./client";
import { FieldTag, FieldType, $, PlainObject, EventName } from "./globals";
import { ResponseError } from "@artempoletsky/easyrpc";
import { ParsedFunction, parseFunction } from "./function";



export function packEventListener(handler: (...args: any[]) => void): ParsedFunction {
  let parsed: ParsedFunction;
  try {
    parsed = parseFunction(handler);
  } catch (error) {
    throw logError("Can't parse event listener", handler.toString());
  }

  return parsed;
}



export type TableScheme = {
  fields: Record<string, FieldType>
  fieldsOrder: string[]
  fieldsOrderUser: string[]
  tags: Record<string, FieldTag[]>
  settings: TableSettings
};


export type DocumentData = Record<string, any[]>;

export type IndicesRecord = Record<string, FragmentedDictionary<string | number, any>>

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


export type RecordsChangeEvent<KeyType extends string | number, Type, MetaType> = CallbackScope & {
  records: Type[];
  meta: MetaType;
  field: keyof Type;
  oldValue: any;
  newValue: any;
  table: Table<KeyType, Type, MetaType>;
}

export type RecordsInsertEvent<KeyType extends string | number, Type, MetaType> = CallbackScope & {
  records: Type[];
  meta: MetaType;
  table: Table<KeyType, Type, MetaType>;
}

export type RecordsRemoveEvent<KeyType extends string | number, Type, MetaType> = CallbackScope & {
  records: Type[];
  meta: MetaType;
  table: Table<KeyType, Type, MetaType>;
}

export type TableOpenEvent<KeyType extends string | number, Type, MetaType> = CallbackScope & {
  meta: MetaType;
  table: Table<KeyType, Type, MetaType>;
}

export class Table<IDType extends string | number = string | number, Type = any, MetaType = {}> {
  protected mainDict: FragmentedDictionary<IDType, any[]>;
  protected indices: IndicesRecord;
  protected memoryFields: Record<string, SortedDictionary<IDType, any>>;
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


  public get autoId(): boolean {
    return this.fieldHasAnyTag(this.primaryKey, "autoinc");
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

  protected forEachIndex(predicate: (fieldName: string, dict: FragmentedDictionary<string | number, any>, tags: Set<FieldTag>) => void) {
    for (const fieldName in this.scheme.tags) {
      const tags = new Set(this.scheme.tags[fieldName]);
      if (tags.has("index") || tags.has("unique")) {
        predicate(fieldName, this.indices[fieldName], tags);
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

    for (const key of this.scheme.fieldsOrder) {
      const type = this.scheme.fields[key];
      this._indexType[i] = type;
      this._indexFieldName[i] = key;
      this._fieldNameIndex[key] = i++;
    }
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
    return new TableQuery<IDType, Type>(this, this.indices, this.mainDict);
  }

  whereRange(fieldName: (keyof Type | "id") & string, min: any, max: any): TableQuery<IDType, Type> {
    return this.createQuery().whereRange(fieldName as any, min, max);
  }

  where<FieldType extends string | number>(fieldName: keyof Type | "id",
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): TableQuery<IDType, Type>
  where<FieldType extends string | number>(fieldName: keyof Type | "id", ...values: FieldType[]): TableQuery<IDType, Type>
  where<FieldType extends string | number>(fieldName: any, ...args: any[]) {
    return this.createQuery().where<FieldType>(fieldName, ...args);
  }

  filter(predicate: DocCallback<IDType, Type, boolean>): TableQuery<IDType, Type> {
    return this.createQuery().filter(predicate);
  }

  all() {
    return this.createQuery().whereRange(this.primaryKey as any, undefined, undefined);
  }

  renameField(oldName: string, newName: string) {
    const { fields, tags } = this.scheme;

    if (!fields[oldName]) throw new Error(`Field '${oldName}' doesn't exist`);
    if (fields[newName]) throw new Error(`Field '${newName}' already exists`);

    if (this.fieldHasAnyTag(oldName as any, "index", "unique", "memory")) {
      FragmentedDictionary.rename(this.getIndexDictDir(oldName), this.getIndexDictDir(newName));
    }

    if (this.fieldHasAnyTag(oldName, "heavy")) {
      renameSync(this.getHeavyFieldDir(oldName), this.getHeavyFieldDir(newName));
    }

    const fieldTags = tags[oldName] || [];
    delete tags[oldName];
    tags[newName] = fieldTags;

    const type = fields[oldName];
    delete fields[oldName];
    fields[newName] = type;

    const indexDict = this.indices[oldName];
    if (indexDict) {
      delete this.indices[oldName];
      this.indices[newName] = indexDict;
    }


    function arrReplace<Type>(arr: Type[], find: Type, replacement: Type) {
      let iOf = arr.indexOf(find);
      if (iOf == -1) return;
      arr.splice(iOf, 1, replacement);
    }

    arrReplace(this.scheme.fieldsOrder, oldName, newName);
    arrReplace(this.scheme.fieldsOrderUser, oldName, newName);

    this.saveScheme();
  }

  getLastIndex() {
    return this.mainDict.end;
  }

  forEachField(predicate: (fieldName: string, type: FieldType, tags: Set<FieldTag>) => any): void {
    const fields = this.scheme.fields;
    for (const key of this.scheme.fieldsOrderUser) {
      predicate(key, fields[key], new Set(this.scheme.tags[key]));
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

    const tags: FieldTag[] = this.scheme.tags[fieldName] || [];

    this.indices[fieldName] = Table.createIndexDictionary(this.name, fieldName, tags, type);


    const indexData: Map<string | number, IDType[]> = new Map();
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

    tags.push(unique ? "unique" : "index");
    this.scheme.tags[fieldName] = tags;
    this.saveScheme();
  }

  protected fillIndexData
    <IndexType extends string | number>(
      indexData: Map<IndexType, IDType[]>,
      value: IndexType, id: IDType, throwUnique?: string) {
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
    const tags = this.scheme.tags[name].filter(tag => tag != "index" && tag != "unique");
    this.scheme.tags[name] = tags;

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
    delete this.scheme.tags[fieldName];

    _.remove(this.scheme.fieldsOrderUser, (e) => e == fieldName);
    _.remove(this.scheme.fieldsOrder, (e) => e == fieldName);
    this.saveScheme();
  }


  addField(fieldName: string, type: FieldType, isHeavy: boolean, predicate?: DocCallback<IDType, Type, any>) {
    if (this.scheme.fields[fieldName])
      throw new ResponseError(`field '${fieldName}' already exists`);


    const filesDir = this.getHeavyFieldDir(fieldName);

    if (isHeavy) {
      if (!existsSync(filesDir)) mkdirSync(filesDir);
    }
    else {
      const definedPredicate = predicate || (() => getDefaultValueForType(type));
      this.insertDocColumn(this._indexFieldName.length, (id, arr) => {
        return definedPredicate(new Document<IDType, Type>(arr, id, this, this.indices) as TDocument<IDType, Type>);
      });
    }


    this.scheme.fields[fieldName] = type;
    this.scheme.tags[fieldName] = isHeavy ? ["heavy"] : [];
    this.scheme.fieldsOrderUser.push(fieldName);
    if (!isHeavy) {
      this.scheme.fieldsOrder.push(fieldName);
    }
    this.saveScheme();
  }

  throwFieldDoesntExist(fieldName: string) {
    return new Error(`Field name '${this.printField(fieldName)}' doesn't exist`);
  }

  changeFieldIndex(fieldName: string, newIndex: number) {
    const { fieldsOrderUser } = this.scheme;
    const iOf = fieldsOrderUser.indexOf(fieldName);
    if (iOf == -1) throw this.throwFieldDoesntExist(fieldName);
    fieldsOrderUser.splice(iOf, 1);
    fieldsOrderUser.splice(newIndex, 0, fieldName);
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

  /**
   * 
   * @param id the record ID
   * @param type the type of the field
   * @param fieldName the name of the field
   * @returns a relative path to the heavy field contents
   */
  getHeavyFieldFilepath(id: IDType, type: FieldType, fieldName: string): string {
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

  buildIndexDataForRecords(records: Type[]) {
    const result: Record<string, Map<string | number, IDType[]>> = {}

    const rrs: PlainObject[] = records as any;
    const idsSet = new Set<IDType>();
    const ids: IDType[] = [];

    for (const rec of rrs) {
      const id = rec[this.primaryKey];
      if (idsSet.has(id))
        throw this.errorValueNotUnique(this.primaryKey, id);

      idsSet.add(id);
      ids.push(id);
    }

    this.forEachIndex((fieldName, dict, tags) => {
      const map = new Map<string | number, IDType[]>();
      for (let i = 0; i < ids.length; i++) {
        const val = rrs[i][fieldName];
        const id = ids[i];
        if (map.has(val)) {
          if (tags.has("unique")) throw this.errorValueNotUnique(fieldName, val);
          (<IDType[]>map.get(val)).push(id);
        } else {
          map.set(val, [id]);
        }
      }
      result[fieldName] = map;
    });

    return result;
  }

  removeFromIndex(records: Type[]) {
    const indexData = this.buildIndexDataForRecords(records);
    for (const key in indexData) {
      this.removeColumnFromIndex(key, indexData[key]);
    }
  }

  removeHeavyFilesForEachID(ids: IDType[]) {
    this.forEachField((fieldName, type, tags) => {
      if (tags.has("heavy")) {
        for (const id of ids) {
          const path = this.getHeavyFieldFilepath(id, type, fieldName);
          rmie(path);
        }
      }
    });
  }

  storeIndexValue(fieldName: string, docID: IDType, value: string | number) {
    this.changeIndexValue(fieldName, docID, undefined, value);
  }

  unstoreIndexValue(fieldName: string, docID: IDType, value: string | number) {
    this.changeIndexValue(fieldName, docID, value, undefined);
  }

  changeIndexValue(fieldName: string, docID: IDType, oldValue: undefined | string | number, newValue: undefined | string | number) {
    if (oldValue == newValue) return;
    const indexDict = this.indices[fieldName];
    if (!indexDict) {
      return;
    }
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");


    if (isUnique) {
      if (newValue !== undefined && indexDict.getOne(newValue)) throw new ResponseError(`Attempting to create a duplicate in the unique ${this.printField(fieldName)}`);

      if (oldValue !== undefined) {
        indexDict.remove([oldValue]);
      }
      if (newValue !== undefined) {
        indexDict.setOne(newValue, docID);
      }
    } else {
      if (oldValue !== undefined) {
        const arr: IDType[] = indexDict.getOne(oldValue) || [];
        // arr.push(id);
        arr.splice(arr.indexOf(docID), 1);
        if (arr.length) {
          indexDict.setOne(oldValue, arr);
        } else {
          indexDict.remove([oldValue]);
        }
      }

      if (newValue !== undefined) {
        const arr: IDType[] = indexDict.getOne(newValue) || [];
        arr.push(docID);
        indexDict.setOne(newValue, arr);
      }
    }
  }


  canInsertUnique<ColumnType extends string | number>(fieldName: string, column: ColumnType[], throwError: boolean = false): boolean {
    const indexDict: FragmentedDictionary<ColumnType> = fieldName == this.primaryKey ? this.mainDict : this.indices[fieldName] as any;
    if (!indexDict) throw this.errorFieldNotIndex(fieldName);

    let set = new Set();

    let found: ColumnType | false = false;
    set = new Set<ColumnType>();

    for (const val of column) {
      if (set.has(val)) {
        found = val;
        break;
      }
      set.add(val);
    }

    if (found !== false) {
      if (throwError) throw this.errorValueNotUnique(fieldName, found);
      return false;
    }

    found = indexDict.hasAnyId(column);

    if (found === false) return true;
    if (throwError) throw this.errorValueNotUnique(fieldName, found);
    return false;
  }

  protected errorValueNotUnique(fieldName: string, value: any) {
    if (fieldName == this.primaryKey) {
      return new ResponseError(`Primary key value '${value}' on ${this.printField()} already exists`)
    }
    return new ResponseError(`Unique value '${value}' for ${this.printField(fieldName)} already exists`);
  }

  public insertMany(data: Type[]): IDType[] {
    const storable: PlainObject[] = [];

    // perfStart("make storable");
    for (const obj of data) {
      const validationError = Document.validateData(obj, this.scheme, this.autoId);
      if (validationError) throw new ResponseError(`insert failed, data is invalid for reason '${validationError}'`);

      storable.push(this.makeObjectStorable(obj));
    }
    // perfEndLog("make storable");

    const indexColumns: Record<string, any[]> = {};
    this.forEachIndex((fieldName, indexDict, tags) => {
      const col = indexColumns[fieldName] = storable.map(o => o[fieldName]);
      if (tags.has("unique")) {
        this.canInsertUnique(fieldName, col, true);
      }
    });

    let ids: IDType[];
    const values = storable.map(o => this.flattenObject(o));

    if (this.autoId) {
      ids = this.mainDict.insertArray(values);
    } else {
      ids = storable.map(o => <IDType>o[this.primaryKey]);

      this.canInsertUnique(this.primaryKey, ids, true);

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
      const indexData: Map<string | number, IDType[]> = new Map();
      for (let i = 0; i < ids.length; i++) {
        this.fillIndexData(indexData, indexColumns[fieldName][i], ids[i]);
      }
      this.insertColumnToIndex(fieldName, indexData);
    });

    if (this.hasEventListener("recordsInsert")) {
      const inserted: Type[] = storable.map((o, i) => ({
        ...o,
        [this.primaryKey]: ids[i]
      } as Type));
      const e: RecordsInsertEvent<IDType, Type, MetaType> = {
        records: inserted,
        table: this,
        meta: this.meta,
        $,
        _,
        db: DataBase,
        ResponseError,
      }

      this.triggerEvent("recordsInsert", e);
    }

    // perfEndLog("indicies update");
    return ids;
  }


  protected errorFieldNotIndex(fieldName: string) {
    return new ResponseError(`{...} is not an index!`, [this.printField(fieldName)]);
  }

  insertColumnToIndex<ColType extends string | number>(fieldName: string, indexData: Map<ColType, IDType[]>) {
    const column = Array.from(indexData.keys());
    const indexDict: FragmentedDictionary<ColType, any> = this.indices[fieldName] as any;
    if (!indexDict) throw this.errorFieldNotIndex(fieldName);
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");
    if (isUnique) {
      const ids = Array.from(indexData.values()).map(arr => arr[0]);
      indexDict.insertMany(column, ids);
    } else {
      indexDict.where({
        // ranges: Array.from(indexData.keys()).map(v => [v, v]),
        idFilter: id => indexData.has(id),
        update: (arr: IDType[], id) => {
          const toPush: IDType[] = indexData.get(id) as any;
          indexData.delete(id);
          return arr.concat.apply(arr, toPush).sort();
        },
      });

      indexDict.insertMany(Array.from(indexData.keys()), Array.from(indexData.values()).map(arr => arr.sort()));
    }
  }

  removeColumnFromIndex<IndexType extends string | number>(fieldName: string, indexData: Map<IndexType, IDType[]>) {
    const column = Array.from(indexData.keys());
    const indexDict: FragmentedDictionary<IndexType, any> = this.indices[fieldName] as any;
    if (!indexDict) throw this.errorFieldNotIndex(fieldName);
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");

    indexDict.where({
      idFilter: (id) => indexData.has(id),
      update: (val: IDType[], indexId) => {
        if (isUnique)
          return undefined;
        const toRemove = indexData.get(indexId);

        val = val.filter(recordId => toRemove?.includes(recordId));
        if (val.length == 0) return undefined;
        return val;
      },
    });
  }

  // insert<Type extends PlainObject>(data: Type): Document<Type>
  public insert(data: PlainObject & Type): IDType {
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
      result[key] = Document.storeValueOfType((<any>o)[key], type as any);
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

  protected insertDocColumn(fieldIndex: number, predicate: (id: IDType, arr: any[]) => any) {
    this.mainDict.editRanges([[undefined, undefined]], (arr, id) => {
      const newArr = [...arr];
      newArr.splice(fieldIndex, 0, predicate(id, newArr));
      return newArr;
    }, 0);
  }

  protected errorWrongId(id: IDType) {
    return new ResponseError(`id '${id}' doesn't exists at ${this.printField()}`);
  }

  at(id: IDType): Type
  at<ReturnType = Type>(id: IDType, predicate?: DocCallback<IDType, Type, ReturnType>): ReturnType
  public at<ReturnType>(id: IDType, predicate?: DocCallback<IDType, Type, ReturnType>) {
    const res = this.where(this.primaryKey as any, id).limit(1).select(predicate);
    if (res.length == 0) throw this.errorWrongId(id);
    return res[0];
  }

  atIndex(index: number): Type | null
  atIndex<ReturnType = Type>(index: number, predicate?: DocCallback<IDType, Type, ReturnType>): ReturnType | null
  public atIndex<ReturnType>(index: number, predicate?: DocCallback<IDType, Type, ReturnType>) {
    const id = this.mainDict.keyAtIndex(index);
    if (id === undefined) return null;
    return this.at(id, predicate);
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

  indexKeys<IndexKeytype extends string | number = string | number>(fieldName?: string & (keyof Type | "id")): IndexKeytype[] {
    if (!fieldName) fieldName = this.primaryKey as (keyof Type & string);

    let index: FragmentedDictionary<IndexKeytype, any> = (fieldName == this.primaryKey) ? this.mainDict as any : this.indices[fieldName];
    if (!index) throw this.errorFieldNotIndex(fieldName);

    return index.keys();
  }

  indexIds<FieldType extends string | number>(fieldName: keyof Type | "id",
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): IDType[]
  indexIds<FieldType extends string | number>(fieldName: keyof Type | "id", ...values: FieldType[]): IDType[]
  indexIds(fieldName: any, ...args: any[]) {
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

  registerEventListener(handlerId: string, eventName: "tableOpen", handler: (event: TableOpenEvent<IDType, Type, MetaType>) => void): void
  registerEventListener(handlerId: string, eventName: "recordsRemove", handler: (event: RecordsRemoveEvent<IDType, Type, MetaType>) => void): void
  registerEventListener(handlerId: string, eventName: "recordsInsert", handler: (event: RecordsInsertEvent<IDType, Type, MetaType>) => void): void
  registerEventListener(handlerId: string, eventName: "recordsChange", handler: (event: RecordsChangeEvent<IDType, Type, MetaType>) => void): void
  registerEventListener(handlerId: string, eventName: string, handler: (event: any) => void): void {
    let listeners = this.events[eventName];
    const $serviceListeners = this.mainDict.meta.custom.$serviceListeners || {};
    if (!listeners) {
      listeners = this.events[eventName] = {};
      $serviceListeners[eventName] = {};
    }
    listeners[handlerId] = handler;
    $serviceListeners[handlerId] = packEventListener(handler);
    this.mainDict.meta.custom.$serviceListeners = $serviceListeners;

    // trigger it immedietly after registration
    if (eventName == "tableOpen") {
      const meta = {
        ...this.meta
      };
      const e: TableOpenEvent<IDType, Type, MetaType> = {
        $,
        _,
        db: DataBase,
        ResponseError,
        meta,
        table: this,
      };
      handler(e);
      this.mainDict.meta.custom.$table = meta;
    }
  }

  unregisterEventListener(handlerId: string, eventName?: EventName) {
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

  triggerEvent(eventName: "tableOpen", event: TableOpenEvent<IDType, Type, MetaType>): void
  triggerEvent(eventName: "recordsRemove", event: RecordsRemoveEvent<IDType, Type, MetaType>): void
  triggerEvent(eventName: "recordsInsert", event: RecordsInsertEvent<IDType, Type, MetaType>): void
  triggerEvent(eventName: "recordsChange", event: RecordsChangeEvent<IDType, Type, MetaType>): void
  triggerEvent(eventName: EventName, event: any): void {
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

  hasEventListener(eventName: EventName) {
    return !!this.events[eventName];
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

  has(...ids: IDType[]) {
    return this.mainDict.hasAllIds(ids);
  }


  getFreeId(): IDType {
    if (this.mainDict.settings.keyType == "string") {
      let idCandidiate = "new_id";
      let tries = 0;
      while (this.has(<IDType>idCandidiate)) {
        tries++;
        idCandidiate = "new_id_" + tries;
      }
      return idCandidiate as IDType;
    } else {
      throw new Error("not implemented yet");
    }
  }

  getDocumentDraft(): Type {
    const res = {} as PlainObject;
    this.forEachField((fieldName, type, tags) => {
      if (tags.has("autoinc")) return;
      res[fieldName] = fieldName == this.primaryKey ? this.getFreeId() : getDefaultValueForType(type);
    });

    return res as Type;
  }

}