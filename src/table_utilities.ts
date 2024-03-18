import { ResponseError } from "@artempoletsky/easyrpc";
import FragmentedDictionary from "./fragmented_dictionary";
import { FieldTag, FieldType, PlainObject, TRecord, Table, TableScheme } from "./globals";
import { rmie } from "./virtual_fs";
import { DataBase } from "./db";
import { TableRecord } from "./record";


export default class TableUtils<T, idT extends string | number>{
  public table: Table<T, idT, any, any, any, any>;
  public mainDict: FragmentedDictionary<idT>;
  public indices: Record<string, FragmentedDictionary>;
  public primaryKey: string;
  public name: string;
  public scheme: TableScheme;

  constructor(table: Table<T, idT, any, any, any, any>, mainDict: FragmentedDictionary<idT>, indices: Record<string, FragmentedDictionary>) {
    this.table = table;
    this.mainDict = mainDict;
    this.indices = indices;
    this.primaryKey = table.primaryKey;
    this.scheme = table.scheme;
    this.name = table.name;
  }

  forEachIndex(predicate: (fieldName: string, dict: FragmentedDictionary<string | number, any>, tags: Set<FieldTag>) => void) {
    const { tags } = this.table.scheme;
    for (const fieldName in tags) {
      const tagsSet = new Set(tags[fieldName]);
      if (tagsSet.has("index") || tagsSet.has("unique")) {
        predicate(fieldName, this.indices[fieldName], tagsSet);
      }
    }
  }

  errorWrongId(id: idT) {
    return new ResponseError({
      message: `id '${id}' doesn't exists at ${this.printField()}`,
      statusCode: 404,
    });
  }

  printField(fieldName?: string) {
    if (!fieldName) return `'${this.table.name}[${this.table.primaryKey}]'`;
    return `field '${this.table.name}[${this.table.primaryKey}].${fieldName}'`;
  }

  errorFieldDoesntExist(fieldName: string) {
    return new ResponseError(`${this.printField(fieldName)} doesn't exist`);
  }

  errorFieldNotIndex(fieldName: string) {
    return new ResponseError(`${this.printField(fieldName)} is not an index field!`);
  }

  errorAlreadyIndex(fieldName: string) {
    return new ResponseError(`${this.printField(fieldName)} is already an index!`);
  }

  errorValueNotUnique(fieldName: string, value: any) {
    if (fieldName == this.table.primaryKey) {
      return new ResponseError(`Primary key value '${value}' on ${this.printField()} already exists`);
    }
    return new ResponseError(`Unique value '${value}' for ${this.printField(fieldName)} already exists`);
  }

  changeIdsIndices(oldIds: idT[], newIds: idT[], records: TRecord<T, idT>[]) {
    this.forEachIndex((fieldName, dict, tags) => {
      if (tags.has("unique")) {

      }
    });
  }

  buildIndexDataForRecords(records: Partial<T>[]) {
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

  removeFromIndex(records: Partial<T>[]) {
    const indexData = this.buildIndexDataForRecords(records);
    for (const key in indexData) {
      this.removeColumnFromIndex(key, indexData[key]);
    }
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

  fieldHasAnyTag(fieldName: string, ...tags: FieldTag[]) {
    return TableUtils.tagsHasFieldNameWithAnyTag(this.scheme.tags, fieldName, ...tags);
  }

  fieldHasAllTags(fieldName: string, ...tags: FieldTag[]) {
    return TableUtils.tagsHasFieldNameWithAllTags(this.scheme.tags, fieldName, ...tags);
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

  forEachField(predicate: (fieldName: string, type: FieldType, tags: Set<FieldTag>) => any): void {
    const fields = this.scheme.fields;
    for (const key of this.scheme.fieldsOrderUser) {
      predicate(key, fields[key], new Set(this.scheme.tags[key]));
    }
  }

  removeRecordColumn(fieldIndex: number): void
  removeRecordColumn(fieldIndex: number, returnAsDict: true): PlainObject
  removeRecordColumn(fieldIndex: number, returnAsDict: boolean = false) {
    const result: Record<string | number, any> = {};
    this.mainDict.where({
      update(arr, id) {
        const newArr = arr.slice(0);
        if (returnAsDict) {
          result[id] = newArr[fieldIndex];
        }
        newArr.splice(fieldIndex, 1);
        return newArr;
      }
    });
    if (returnAsDict) return result;
  }


  changeIndexValue(fieldName: string, recID: idT, oldValue: undefined | string | number, newValue: undefined | string | number) {
    if (oldValue == newValue) return;
    const indexDict = this.indices[fieldName];
    if (!indexDict) {
      return;
    }
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");


    if (isUnique) {
      if (newValue !== undefined && indexDict.getOne(newValue)) throw this.errorValueNotUnique(fieldName, newValue);

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

  storeIndexValue(fieldName: string, recID: idT, value: string | number) {
    this.changeIndexValue(fieldName, recID, undefined, value);
  }

  unstoreIndexValue(fieldName: string, recID: idT, value: string | number) {
    this.changeIndexValue(fieldName, recID, value, undefined);
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

  static fromTable<T, idT extends string | number>(table: Table<T, idT, any, any, any, any>): TableUtils<T, idT> {
    const { mainDict, indices } = this.getTableDicts<idT>(table.scheme, table.name);
    return new TableUtils<T, idT>(table, mainDict, indices);
  }

  static getTableDicts<idT extends string | number>(scheme: TableScheme, tableName: string) {
    const mainDict = FragmentedDictionary.open<idT, any>(`/${tableName}/main/`);
    const indices: Record<string, FragmentedDictionary> = {};
    for (const fieldName in scheme.tags) {
      const tags = scheme.tags[fieldName];
      if (tags.includes("index") || tags.includes("unique")) {
        indices[fieldName] = FragmentedDictionary.open(`/${tableName}/indices/${fieldName}/`);
      }
    }
    return {
      mainDict,
      indices,
    };
  }


  makeObjectStorable(o: T): PlainObject {
    const result: PlainObject = {};
    this.forEachField((key, type) => {
      result[key] = TableRecord.storeValueOfType((<any>o)[key], type as any);
    });
    return result;
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


  static createIndexDictionary(tableName: string, fieldName: string, tags: FieldTag[], type: FieldType) {
    const directory = `/${tableName}/indices/${fieldName}/`;

    if (type == "json") throw new Error(`Can't create an index of json type field ${fieldName}`);

    let keyType = fieldTypeToKeyType(type);

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

  fillIndexData
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

  flattenObject(o: PlainObject): any[] {
    const lightValues: any[] = [];
    for (const key of this.scheme.fieldsOrder) {
      lightValues.push(o[key]);
    }
    return lightValues;
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


  insertRecordColumn(fieldIndex: number, predicate: (id: idT, arr: any[]) => any) {
    this.mainDict.where({
      update(arr, id) {
        const newArr = [...arr];
        newArr.splice(fieldIndex, 0, predicate(id, newArr));
        return newArr;
      }
    });
  }

  static getPrimaryKeyFromScheme(scheme: TableScheme) {
    for (const fieldName in scheme.tags) {
      const tags = scheme.tags[fieldName];
      if (tags.includes("primary")) {
        return fieldName;
      }
    }
    throw new Error("Can't find primary key in the scheme!");
  }
}


export function fieldTypeToKeyType(type: FieldType): "int" | "string" {
  if (type == "json") throw new Error("wrong type");
  if (type == "string") return "string";
  return "int";
}
