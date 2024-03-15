
import { DataBase, SCHEME_PATH, SchemeFile, TableSettings } from "./db";
import { TRecord, TableRecord } from "./record";
import { rfs, wfs, existsSync, mkdirSync, renameSync, rmie, logError, $ } from "./utils";


import FragmentedDictionary, { FragmentedDictionarySettings, IDFilter, PartitionFilter, PartitionMeta } from "./fragmented_dictionary";
import TableQuery, { twoArgsToFilters } from "./table_query";
import SortedDictionary from "./sorted_dictionary";
import _, { flatten } from "lodash";
import { CallbackScope } from "./client";
import { FieldTag, FieldType, PlainObject, EventName } from "./globals";
import { ResponseError } from "@artempoletsky/easyrpc";
import { ParsedFunction, constructFunction, parseFunction } from "./function";
import zod, { ZodEffects, ZodError, ZodObject, ZodRawShape } from "zod";



export function packEventListener(handler: (...args: any[]) => void): ParsedFunction {
  let parsed: ParsedFunction;
  try {
    parsed = parseFunction(handler);
  } catch (error) {
    throw logError("Can't parse event listener", handler.toString());
  }

  return parsed;
}

export type RecordValidator = (table: Table<any, any, any>, scope: CallbackScope) => ZodObject<any> | ZodEffects<ZodObject<any>>;

export type TableScheme = {
  fields: Record<string, FieldType>
  fieldsOrder: string[]
  fieldsOrderUser: string[]
  tags: Record<string, FieldTag[]>
  settings: TableSettings
};


export type IndicesRecord = Record<string, FragmentedDictionary<string | number, any>>

export type RecordCallback<T, idT extends string | number, ReturnType, LightT, VisibleT> = (record: TRecord<T, idT, LightT, VisibleT>) => ReturnType;


export type RegisteredEvents = Record<string, Record<string, ParsedFunction>>;

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


export type EventTableBase<T, idT extends string | number, MetaT, InsertT, LightT, VisibleT> = {
  meta: MetaT;
  table: Table<T, idT, MetaT, InsertT, LightT, VisibleT>;
}

export type EventRecordChangeCompact<T, idT extends string | number, LightT, VisibleT, FieldT> = {
  record: TRecord<T, idT, LightT, VisibleT>,
  fieldName: keyof T;
  oldValue: FieldT;
  newValue: FieldT;
}

export type EventRecordChange<T, idT extends string | number, MetaT, InsertT, LightT, VisibleT, FieldT> = EventRecordChangeCompact<T, idT, LightT, VisibleT, FieldT>
  & CallbackScope
  & EventTableBase<T, idT, MetaT, InsertT, LightT, VisibleT>


export type EventRecordsInsertCompact<T> = {
  records: T[];
}

export type EventRecordsInsert<T, idT extends string | number, MetaT, InsertT, LightT, VisibleT> = EventRecordsInsertCompact<T>
  & CallbackScope
  & EventTableBase<T, idT, MetaT, InsertT, LightT, VisibleT>

export type EventRecordsRemoveCompact<T> = {
  records: T[];
}
export type EventRecordsRemove<T, idT extends string | number, MetaT, InsertT, LightT, VisibleT> = EventRecordsInsertCompact<T>
  & CallbackScope
  & EventTableBase<T, idT, MetaT, InsertT, LightT, VisibleT>

export type EventRecordsRemoveLight<T, idT extends string | number, MetaT, InsertT, LightT, VisibleT> = EventRecordsInsertCompact<LightT>
  & CallbackScope
  & EventTableBase<T, idT, MetaT, InsertT, LightT, VisibleT>

export type EventTableOpen<T, idT extends string | number, MetaT, InsertT, LightT, VisibleT> = CallbackScope
  & EventTableBase<T, idT, MetaT, InsertT, LightT, VisibleT>

export class Table<T = unknown, idT extends string | number = string | number, MetaT = {}, InsertT = T, LightT = T, VisibleT = T> {
  protected mainDict: FragmentedDictionary<idT, any[]>;
  protected indices: IndicesRecord;
  protected memoryFields: Record<string, SortedDictionary<idT, any>>;
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
      if (this.fieldHasAnyTag(<keyof T & string>fieldName, "index", "unique")) {
        this.indices[fieldName] = FragmentedDictionary.open(this.getIndexDictDir(fieldName));
      }
      if (this.fieldHasAnyTag(<keyof T & string>fieldName, "primary")) {
        this.primaryKey = fieldName;
      }
    }

    this.mainDict = FragmentedDictionary.open(`/${name}/main/`);
    this.memoryFields = {};
    this.loadMemoryIndices();
    this.updateFieldIndices();
    this.unpackEventListeners();

    const meta = this.mainDict.meta.custom;
    this.triggerEvent("tableOpen");
    if (!meta.userMeta) {
      meta.userMeta = {};
    }

    this._zObject = this.validator = 0 as any;//line below will set the validator
    try {
      this.setValidator(meta.validator);  
    } catch (err:any) {
      logError(err.message, JSON.stringify(meta.validator));
      this.setValidator();
    }
    

    if (!meta.lastId) {
      meta.lastId = 0
    }
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

    return new TableQuery<T, idT, LightT, VisibleT>(this, this.indices, this.mainDict);
  }

  whereRange(fieldName: keyof T, min: any, max: any): TableQuery<T, idT, LightT, VisibleT> {
    return this.createQuery().whereRange(fieldName as any, min, max);
  }

  where<FieldType extends string | number>(fieldName: keyof T,
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): TableQuery<T, idT, LightT, VisibleT>
  where<FieldType extends string | number>(fieldName: keyof T, ...values: FieldType[]): TableQuery<T, idT, LightT, VisibleT>
  where<FieldType extends string | number>(fieldName: any, ...args: any[]) {
    return this.createQuery().where<FieldType>(fieldName, ...args);
  }

  filter(predicate: RecordCallback<T, idT, boolean, LightT, VisibleT>) {
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

  createIndex(fieldName: string & keyof T, unique: boolean) {
    if (this.indices[fieldName]) this.throwAlreadyIndex(fieldName);
    const type = this.scheme.fields[fieldName];

    const tags: FieldTag[] = this.scheme.tags[fieldName] || [];

    this.indices[fieldName] = Table.createIndexDictionary(this.name, fieldName, tags, type);


    const indexData: Map<string | number, idT[]> = new Map();
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
      indexData: Map<IndexType, idT[]>,
      value: IndexType, id: idT, throwUnique?: string) {
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
      this.removeRecordColumn(this._fieldNameIndex[fieldName]);
    }

    delete this.scheme.fields[fieldName];
    delete this.scheme.tags[fieldName];

    _.remove(this.scheme.fieldsOrderUser, (e) => e == fieldName);
    _.remove(this.scheme.fieldsOrder, (e) => e == fieldName);
    this.saveScheme();
  }


  addField<ReturnType = any>(fieldName: string, type: FieldType, isHeavy: boolean, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>) {
    if (this.scheme.fields[fieldName])
      throw new ResponseError(`field '${fieldName}' already exists`);


    const filesDir = this.getHeavyFieldDir(fieldName);

    if (isHeavy) {
      if (!existsSync(filesDir)) mkdirSync(filesDir);
    }
    else {
      const definedPredicate = predicate || (() => getDefaultValueForType(type));
      this.insertRecordColumn(this._indexFieldName.length, (id, arr) => {
        return definedPredicate(new TableRecord(arr, id, this, this.indices) as TRecord<T, idT, LightT, VisibleT>);
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
  getHeavyFieldFilepath(id: idT, type: FieldType, fieldName: string): string {
    return `${this.getHeavyFieldDir(fieldName)}${id}.${type == "json" ? "json" : "txt"}`;
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

  protected removeRecordColumn(fieldIndex: number): void
  protected removeRecordColumn(fieldIndex: number, returnAsDict: true): PlainObject
  protected removeRecordColumn(fieldIndex: number, returnAsDict: boolean = false) {
    const result: Record<string | number, any> = {};
    this.mainDict.where({
      update(arr, id) {
        const newArr = [...arr];
        if (returnAsDict) {
          result[id] = newArr[fieldIndex];
        }
        newArr.splice(fieldIndex, 1);
        return newArr;
      }
    });
    if (returnAsDict) return result;
  }

  buildIndexDataForRecords(records: (LightT | T)[]) {
    const result: Record<string, Map<string | number, idT[]>> = {}

    const rrs: PlainObject[] = records as any;
    const idsSet = new Set<idT>();
    const ids: idT[] = [];

    for (const rec of rrs) {
      const id = rec[this.primaryKey];
      if (idsSet.has(id))
        throw this.errorValueNotUnique(this.primaryKey, id);

      idsSet.add(id);
      ids.push(id);
    }

    this.forEachIndex((fieldName, dict, tags) => {
      const map = new Map<string | number, idT[]>();
      for (let i = 0; i < ids.length; i++) {
        const val = rrs[i][fieldName];
        const id = ids[i];
        if (map.has(val)) {
          if (tags.has("unique")) throw this.errorValueNotUnique(fieldName, val);
          (<idT[]>map.get(val)).push(id);
        } else {
          map.set(val, [id]);
        }
      }
      result[fieldName] = map;
    });

    return result;
  }

  removeFromIndex(records: (LightT | T)[]) {
    const indexData = this.buildIndexDataForRecords(records);
    for (const key in indexData) {
      this.removeColumnFromIndex(key, indexData[key]);
    }
  }

  removeHeavyFilesForEachID(ids: idT[]) {
    this.forEachField((fieldName, type, tags) => {
      if (tags.has("heavy")) {
        for (const id of ids) {
          const path = this.getHeavyFieldFilepath(id, type, fieldName);
          rmie(path);
        }
      }
    });
  }

  storeIndexValue(fieldName: string, recID: idT, value: string | number) {
    this.changeIndexValue(fieldName, recID, undefined, value);
  }

  unstoreIndexValue(fieldName: string, recID: idT, value: string | number) {
    this.changeIndexValue(fieldName, recID, value, undefined);
  }

  changeIndexValue(fieldName: string, recID: idT, oldValue: undefined | string | number, newValue: undefined | string | number) {
    if (oldValue == newValue) return;
    const indexDict = this.indices[fieldName];
    if (!indexDict) {
      return;
    }
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");


    if (isUnique) {
      if (newValue !== undefined && indexDict.getOne(newValue)) throw new ResponseError(`Attempting to create a duplicate in the unique ${this.printField(fieldName)}`);

      if (oldValue !== undefined) {
        indexDict.remove(oldValue);
      }
      if (newValue !== undefined) {
        indexDict.setOne(newValue, recID);
      }
    } else {
      if (oldValue !== undefined) {
        const arr: idT[] = (indexDict.getOne(oldValue) || []).slice(0);
        // arr.push(id);
        arr.splice(arr.indexOf(recID), 1);
        if (arr.length) {
          indexDict.setOne(oldValue, arr);
        } else {
          indexDict.remove(oldValue);
        }
      }

      if (newValue !== undefined) {
        const arr: idT[] = indexDict.getOne(newValue) || [];
        arr.push(recID);
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

  public insertMany(data: InsertT[]): idT[] {
    const storable: PlainObject[] = [];


    const dataFull: T[] = [];
    let lastId = this.mainDict.meta.custom.lastId;
    const ids: idT[] = [];
    for (const obj of data) {

      if (this.autoId) {
        lastId++;
        dataFull.push({
          ...obj,
          [this.primaryKey]: lastId,
        } as T);
        ids.push(lastId);
      } else {
        ids.push((obj as any)[this.primaryKey]);
        dataFull.push(obj as any);
      }
    }

    for (const obj of dataFull) {
      try {
        this.zObject.parse(obj);
      } catch (zError) {
        throw new ResponseError(zError as ZodError);
      }

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


    const values = storable.map(o => this.flattenObject(o));

    storable.map(o => <idT>o[this.primaryKey]);
    this.canInsertUnique(this.primaryKey, ids, true);
    this.mainDict.insertMany(ids, values);


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
      const indexData: Map<string | number, idT[]> = new Map();
      for (let i = 0; i < ids.length; i++) {
        this.fillIndexData(indexData, indexColumns[fieldName][i], ids[i]);
      }
      this.insertColumnToIndex(fieldName, indexData);
    });

    if (this.hasEventListener("recordsInsert")) {
      const inserted: T[] = storable.map((o, i) => ({
        ...o,
        [this.primaryKey]: ids[i]
      } as T));
      const e: EventRecordsInsert<T, idT, MetaT, InsertT, LightT, VisibleT> = {
        records: inserted,
        table: this,
        meta: this.meta,
        $,
        _,
        db: DataBase,
        z: zod,
      }

      this.triggerEvent("recordsInsert", e);
    }
    this.mainDict.meta.custom.lastId = lastId;
    // perfEndLog("indicies update");
    return ids;
  }


  protected errorFieldNotIndex(fieldName: string) {
    return new ResponseError(`{...} is not an index!`, [this.printField(fieldName)]);
  }

  insertColumnToIndex<ColType extends string | number>(fieldName: string, indexData: Map<ColType, idT[]>) {
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
        update: (arr: idT[], id) => {
          const toPush: idT[] = indexData.get(id) as any;
          indexData.delete(id);
          return arr.concat.apply(arr, toPush).sort();
        },
      });

      indexDict.insertMany(Array.from(indexData.keys()), Array.from(indexData.values()).map(arr => arr.sort()));
    }
  }

  removeColumnFromIndex<IndexType extends string | number>(fieldName: string, indexData: Map<IndexType, idT[]>) {
    const indexDict: FragmentedDictionary<IndexType, any> = this.indices[fieldName] as any;
    if (!indexDict) throw this.errorFieldNotIndex(fieldName);
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");

    indexDict.where({
      idFilter: (id) => indexData.has(id),
      update: (val: idT[], indexId) => {
        if (isUnique)
          return undefined;
        const toRemove = indexData.get(indexId) as idT[];

        val = val.filter(recordId => !toRemove.includes(recordId));
        if (val.length == 0) return undefined;
        return val;
      },
    });
  }

  public insert(data: InsertT): idT {
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

  makeObjectStorable(o: T | InsertT | LightT): PlainObject {
    const result: PlainObject = {};
    this.forEachField((key, type) => {
      result[key] = TableRecord.storeValueOfType((<any>o)[key], type as any);
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

  protected insertRecordColumn(fieldIndex: number, predicate: (id: idT, arr: any[]) => any) {
    this.mainDict.where({
      update(arr, id) {
        const newArr = [...arr];
        newArr.splice(fieldIndex, 0, predicate(id, newArr));
        return newArr;
      }
    });
  }

  protected errorWrongId(id: idT) {
    return new ResponseError(`id '${id}' doesn't exists at ${this.printField()}`);
  }

  at(id: idT): VisibleT
  at<ReturnType = T>(id: idT, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>): ReturnType
  public at<ReturnType>(id: idT, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>) {
    const res = this.where(this.primaryKey as any, id).limit(1).select(predicate);
    if (res.length == 0) throw this.errorWrongId(id);
    return res[0];
  }

  atIndex(index: number): VisibleT | null
  atIndex<ReturnType = T>(index: number, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>): ReturnType | null
  public atIndex<ReturnType>(index: number, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>) {
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

  indexKeys<IndexKeytype extends string | number = string | number>(fieldName?: string & (keyof T | "id")): IndexKeytype[] {
    if (!fieldName) fieldName = this.primaryKey as (keyof T & string);

    let index: FragmentedDictionary<IndexKeytype, any> = (fieldName == this.primaryKey) ? this.mainDict as any : this.indices[fieldName];
    if (!index) throw this.errorFieldNotIndex(fieldName);

    return index.keys();
  }

  indexIds<FieldType extends string | number>(fieldName: keyof T | "id",
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): idT[]
  indexIds<FieldType extends string | number>(fieldName: keyof T | "id", ...values: FieldType[]): idT[]
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

  public get meta(): MetaT {
    const dictProxy = this.mainDict.meta.custom;

    return new Proxy(dictProxy.userMeta, {
      deleteProperty(target, p) {
        delete target[p];
        dictProxy.userMeta = target;
        return true;
      },
      get(target, key) {
        return target[key];
      },
      set(target, key, value) {
        target[key] = value;
        dictProxy.userMeta = target;
        return true;
      }
    });
  }

  protected events: Record<string, Record<string, Function>> = {};

  unpackEventListeners() {
    let eventsPacked = this.mainDict.meta.custom.eventsPacked;
    if (!eventsPacked) {
      this.mainDict.meta.custom.eventsPacked = eventsPacked = {};
    }
    const { events } = this;
    for (const namespaceId in eventsPacked) {
      const listeners = eventsPacked[namespaceId];
      for (const eventName in listeners) {
        const fn = constructFunction(listeners[eventName]);
        const namespace = events[eventName] || {};
        namespace[namespaceId] = fn;
        events[eventName] = namespace;
      }
    }
  }

  registerEventListenerParsed(namespaceId: string, eventName: EventName, fun: ParsedFunction): void
  registerEventListenerParsed(namespaceId: string, eventName: string, fun: ParsedFunction): void
  registerEventListenerParsed(namespaceId: string, eventName: string, fun: ParsedFunction) {
    this.registerEventListener(namespaceId, eventName, constructFunction(fun) as any);
  }

  registerEventListener(namespaceId: string, eventName: "tableOpen",
    handler: (event: EventTableOpen<T, idT, MetaT, InsertT, LightT, VisibleT>) => void): void
  registerEventListener(namespaceId: string, eventName: "recordsRemove",
    handler: (event: EventRecordsRemove<T, idT, MetaT, InsertT, LightT, VisibleT>) => void): void
  registerEventListener(namespaceId: string, eventName: "recordsRemoveLight",
    handler: (event: EventRecordsRemoveLight<T, idT, MetaT, InsertT, LightT, VisibleT>) => void): void
  registerEventListener(namespaceId: string, eventName: "recordsInsert",
    handler: (event: EventRecordsInsert<T, idT, MetaT, InsertT, LightT, VisibleT>) => void): void
  registerEventListener<FieldT>(namespaceId: string, eventName: string,
    handler: (event: EventRecordChange<T, idT, MetaT, InsertT, LightT, VisibleT, FieldT>) => void): void
  registerEventListener(namespaceId: string, eventName: string,
    handler: (event: any) => void): void {
    let listeners = this.events[eventName];
    const eventsPacked = this.mainDict.meta.custom.eventsPacked;
    if (!listeners) {
      listeners = this.events[eventName] = {};
    }
    if (!eventsPacked[namespaceId]) {
      eventsPacked[namespaceId] = {};
    }
    listeners[namespaceId] = handler;
    eventsPacked[namespaceId][eventName] = packEventListener(handler);
    this.mainDict.meta.custom.eventsPacked = eventsPacked;

    // trigger it immedietly after registration
    if (eventName == "tableOpen") {
      const meta = {
        ...this.meta
      };
      const e: EventTableOpen<T, idT, MetaT, InsertT, LightT, VisibleT> = {
        $,
        _,
        db: DataBase,
        meta,
        table: this,
        z: zod,
      };
      handler(e);
      this.mainDict.meta.custom.userMeta = meta;
    }
  }

  getRegisteredEventListeners(): RegisteredEvents {
    return this.mainDict.meta.custom.eventsPacked;
  }

  unregisterEventListener(namespaceId: string, eventName?: string): void
  unregisterEventListener(namespaceId: string, eventName?: EventName): void
  unregisterEventListener(namespaceId: string, eventName?: string) {
    const { events } = this;
    if (eventName === undefined) {
      for (const name in events) {
        this.unregisterEventListener(namespaceId, name as any);
      }
      return;
    }
    delete events[eventName][namespaceId];
    const { eventsPacked } = this.mainDict.meta.custom;
    delete eventsPacked[namespaceId][eventName];
    if (Object.keys(eventsPacked[namespaceId]).length == 0) {
      delete eventsPacked[namespaceId];
    }
    this.mainDict.meta.custom.eventsPacked = eventsPacked;
  }

  triggerEvent(eventName: "tableOpen"): void
  triggerEvent(eventName: "recordsRemove", eventData: EventRecordsRemoveCompact<T>): void
  triggerEvent(eventName: "recordsRemoveLight", eventData: EventRecordsRemoveCompact<LightT>): void
  triggerEvent(eventName: "recordsInsert", eventData: EventRecordsInsertCompact<T>): void
  triggerEvent<FieldT>(eventName: string, eventData: EventRecordChangeCompact<T, idT, LightT, VisibleT, FieldT>): void
  triggerEvent(eventName: string, eventData?: any): void {
    if (eventName == "recordChange") {
      eventName += ":" + eventData.fieldName;
    }

    const listeners = this.events[eventName];
    if (!listeners) return;
    if (!eventData) eventData = {};
    for (const handlerId in listeners) {
      listeners[handlerId]({
        $: $,
        _: _,
        db: DataBase,
        table: this,
        meta: this.meta,
        ...eventData,
      });
    }
  }

  hasEventListener(eventName: EventName, namespace?: string): boolean
  hasEventListener(eventName: string, namespace?: string): boolean
  hasEventListener(eventName: string | EventName, namespace?: string) {
    const events = this.events[eventName];
    if (!events) return false;

    if (!namespace) return Object.keys(events).length != 0;

    return !!events[namespace];
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

  has(...ids: idT[]) {
    return this.mainDict.hasAllIds(ids);
  }


  getFreeId(): idT {
    if (this.mainDict.settings.keyType == "string") {
      let idCandidiate = "new_id";
      let tries = 0;
      while (this.has(<idT>idCandidiate)) {
        tries++;
        idCandidiate = "new_id_" + tries;
      }
      return idCandidiate as idT;
    }

    return this.mainDict.meta.custom.lastId + 1;
  }

  getRecordDraft(): T {
    const res = {} as PlainObject;
    this.forEachField((fieldName, type, tags) => {
      if (tags.has("autoinc")) return;
      res[fieldName] = fieldName == this.primaryKey ? this.getFreeId() : getDefaultValueForType(type);
    });

    return res as T;
  }

  protected validator: RecordValidator;

  getSavedValidator(): ParsedFunction {
    return this.mainDict.meta.custom.validator;
  }

  setValidator(fun?: RecordValidator): void
  setValidator(fun?: ParsedFunction): void
  setValidator(fun?: ParsedFunction | RecordValidator) {
    if (!fun) {
      this.setValidator((self, { z }) => {
        const shape: ZodRawShape = {}
        for (const fieldName in self.scheme.fields) {
          const type = self.scheme.fields[fieldName];
          let rule;

          switch (type) {
            case "boolean": rule = z.boolean(); break;
            case "date": rule = z.date(); break;
            case "json": rule = z.any(); break;
            case "number": rule = z.number(); break;
            case "string": rule = z.string(); break;
          }

          shape[fieldName] = rule;
        }
        return z.object(shape);
      });
      return;
    }
    if (typeof fun == "function") {
      fun = parseFunction(fun);
    }
    this.mainDict.meta.custom.validator = fun;

    this.validator = constructFunction(fun) as RecordValidator;

    this._zObject = this.validator(this, { db: DataBase, $, _, z: zod });
  }

  protected _zObject: ZodObject<any> | ZodEffects<ZodObject<any>>;

  public get zObject() {
    return this._zObject;
  }

}