
import { DataBase, SCHEME_PATH, SchemeFile, TableSettings } from "./db";
import { TRecord, TableRecord } from "./record";
import { rfs, wfs, existsSync, mkdirSync, renameSync, rmie, logError, $, absolutePath } from "./utils";

import fs from "fs";
import FragmentedDictionary, { FragmentedDictionarySettings, IDFilter, PartitionFilter, PartitionMeta } from "./fragmented_dictionary";
import TableQuery, { twoArgsToFilters } from "./table_query";
import SortedDictionary from "./sorted_dictionary";
import _, { flatten } from "lodash";
import { CallbackScope } from "./client";
import { FieldTag, FieldType, PlainObject, EventName } from "./globals";
import { ResponseError } from "@artempoletsky/easyrpc";
import { ParsedFunction, constructFunction, parseFunction } from "./function";
import zod, { ZodEffects, ZodError, ZodObject, ZodRawShape } from "zod";
import TableUtils from "./table_utilities";


export function fieldNameStringValid(fieldName: string): boolean {
  return !!fieldName.match(/^[a-zA-Z].*$/);
}

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

  protected utils: TableUtils<T, idT>;

  public readonly scheme: TableScheme;
  public readonly name: string;
  public readonly primaryKey: string;
  protected _dirtyIndexPartitions: Map<number, boolean> = new Map();

  constructor(name: string, scheme: TableScheme) {

    this.primaryKey = "id";
    this.name = name;
    this.scheme = scheme;

    this.primaryKey = TableUtils.getPrimaryKeyFromScheme(scheme);
    const { indices, mainDict } = TableUtils.getTableDicts<idT>(scheme, name);
    this.indices = indices;
    this.mainDict = mainDict;

    this.memoryFields = {};
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
    } catch (err: any) {
      logError(err.message, JSON.stringify(meta.validator));
      this.setValidator();
    }


    if (!meta.lastId) {
      meta.lastId = 0
    }

    this.utils = new TableUtils(this, this.mainDict, this.indices);
  }


  public get autoId(): boolean {
    return this.utils.fieldHasAnyTag(this.primaryKey, "autoinc");
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
    return new TableQuery<T, idT, LightT, VisibleT>(this, this.utils);
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

    if (!fieldNameStringValid(newName)) throw this.utils.errorWrongFieldNameString();

    if (!fields[oldName]) throw new Error(`Field '${oldName}' doesn't exist`);
    if (fields[newName]) throw new Error(`Field '${newName}' already exists`);

    if (this.utils.fieldHasAnyTag(oldName as any, "index", "unique", "memory")) {
      FragmentedDictionary.rename(this.utils.getIndexDictDir(oldName), this.utils.getIndexDictDir(newName));
    }

    if (this.utils.fieldHasAnyTag(oldName, "heavy")) {
      renameSync(this.utils.getHeavyFieldDir(oldName), this.utils.getHeavyFieldDir(newName));
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


  createIndex(fieldName: string & keyof T, unique: boolean) {
    if (this.indices[fieldName]) throw this.utils.errorAlreadyIndex(fieldName);
    const type = this.scheme.fields[fieldName];

    const tags: FieldTag[] = this.scheme.tags[fieldName] || [];

    this.indices[fieldName] = TableUtils.createIndexDictionary(this.name, fieldName, tags, type);


    const indexData: Map<string | number, idT[]> = new Map();
    // const ids: KeyType[] = [];
    // const col: string[] | number[] = [];
    const fIndex = this._fieldNameIndex[fieldName];
    this.mainDict.iterateRanges({
      filter: (arr, id) => {
        try {
          this.utils.fillIndexData(indexData, arr[fIndex], id, unique ? fieldName : undefined);
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


  removeIndex(name: string) {
    if (!this.indices[name]) throw this.utils.errorFieldNotIndex(name);

    this.indices[name].destroy();
    delete this.indices[name];
    const tags = this.scheme.tags[name].filter(tag => tag != "index" && tag != "unique");
    this.scheme.tags[name] = tags;

    this.saveScheme();
  }

  removeField(fieldName: string) {
    const { fields, tags } = this.scheme;
    const type = fields[fieldName];
    if (!type) throw this.utils.errorFieldDoesntExist(fieldName);


    if (fieldName == this.primaryKey) throw new ResponseError(`Can't remove the primary key ${this.utils.printField(fieldName)}! Create a new table instead.`);

    if (this.utils.fieldHasAnyTag(fieldName, "index", "unique")) {
      this.removeIndex(fieldName);
    }

    if (this.utils.fieldHasAnyTag(fieldName, "heavy"))
      rmie(this.utils.getHeavyFieldDir(fieldName));
    else {
      this.utils.removeRecordColumn(this._fieldNameIndex[fieldName]);
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

    if (!fieldNameStringValid(fieldName)) throw this.utils.errorWrongFieldNameString();

    const filesDir = this.utils.getHeavyFieldDir(fieldName);

    if (isHeavy) {
      if (!existsSync(filesDir)) mkdirSync(filesDir);
    }
    else {
      const definedPredicate = predicate || (() => getDefaultValueForType(type));
      this.utils.insertRecordColumn(this._indexFieldName.length, (id, arr) => {
        return definedPredicate(new TableRecord(arr, id, this, this.utils) as TRecord<T, idT, LightT, VisibleT>);
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



  changeFieldIndex(fieldName: string, newIndex: number) {
    const { fieldsOrderUser } = this.scheme;
    const iOf = fieldsOrderUser.indexOf(fieldName);
    if (iOf == -1) throw this.utils.errorFieldDoesntExist(fieldName);
    fieldsOrderUser.splice(iOf, 1);
    fieldsOrderUser.splice(newIndex, 0, fieldName);
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

      storable.push(this.utils.makeObjectStorable(obj));
    }
    // perfEndLog("make storable");

    const indexColumns: Record<string, any[]> = {};
    this.utils.forEachIndex((fieldName, indexDict, tags) => {
      const col = indexColumns[fieldName] = storable.map(o => o[fieldName]);
      if (tags.has("unique")) {
        this.utils.canInsertUnique(fieldName, col, true);
      }
    });


    const values = storable.map(o => this.utils.flattenObject(o));

    storable.map(o => <idT>o[this.primaryKey]);
    this.utils.canInsertUnique(this.primaryKey, ids, true);
    this.mainDict.insertMany(ids, values);


    this.utils.forEachField((key, type, tags) => {
      if (tags.has("heavy")) {
        for (let i = 0; i < storable.length; i++) {
          const value = storable[i][key];
          if (value) {
            const toStore = type == "json" ? JSON.stringify(value) : value;
            fs.writeFileSync(this.utils.getHeavyFieldFilepath(ids[i], type, key), toStore, {});
            // wfs(this.getHeavyFieldFilepath(ids[i], type, key), value);
          }
        }
      }
    });

    // perfStart("indicies update");
    this.utils.forEachIndex((fieldName) => {
      const indexData: Map<string | number, idT[]> = new Map();
      for (let i = 0; i < ids.length; i++) {
        this.utils.fillIndexData(indexData, indexColumns[fieldName][i], ids[i]);
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


  insertColumnToIndex<ColType extends string | number>(fieldName: string, indexData: Map<ColType, idT[]>) {
    const column = Array.from(indexData.keys());
    const indexDict: FragmentedDictionary<ColType, any> = this.indices[fieldName] as any;
    if (!indexDict) throw this.utils.errorFieldNotIndex(fieldName);
    const isUnique = this.utils.fieldHasAnyTag(fieldName, "unique");
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

  insert(data: InsertT): idT {
    return this.insertMany([data])[0];
  }


  at(id: idT): VisibleT
  at<ReturnType = T>(id: idT, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>): ReturnType
  public at<ReturnType>(id: idT, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>) {
    const res = this.where(this.primaryKey as any, id).limit(1).select(predicate);
    if (res.length == 0) throw this.utils.errorWrongId(id);
    return res[0];
  }

  atIndex(index: number): VisibleT | null
  atIndex<ReturnType = T>(index: number, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>): ReturnType | null
  public atIndex<ReturnType>(index: number, predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>) {
    const id = this.mainDict.keyAtIndex(index);
    if (id === undefined) return null;
    return this.at(id, predicate);
  }

  indexKeys<IndexKeytype extends string | number = string | number>(fieldName?: string & (keyof T | "id")): IndexKeytype[] {
    if (!fieldName) fieldName = this.primaryKey as (keyof T & string);

    let index: FragmentedDictionary<IndexKeytype, any> = (fieldName == this.primaryKey) ? this.mainDict as any : this.indices[fieldName];
    if (!index) throw this.utils.errorFieldNotIndex(fieldName);

    return index.keys();
  }

  indexIds<FieldType extends string | number>(fieldName: keyof T | "id",
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): idT[]
  indexIds<FieldType extends string | number>(fieldName: keyof T | "id", ...values: FieldType[]): idT[]
  indexIds(fieldName: any, ...args: any[]) {
    const index = this.indices[fieldName];
    if (!index) throw this.utils.errorFieldNotIndex(fieldName);

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

  protected unpackEventListeners() {
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
    this.utils.forEachField((fieldName, type, tags) => {
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
            case "date": rule = z.coerce.date(); break;
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