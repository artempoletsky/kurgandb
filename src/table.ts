
import { SchemeFile, TableSettings } from "./db";
import { FieldType, Document, HeavyTypes, HeavyType, LightTypes, TDocument } from "./document";
import { PlainObject, rfs, wfs, existsSync, mkdirSync, renameSync, rmie } from "./utils";

import FragmentedDictionary, { FragmentedDictionarySettings, PartitionMeta } from "./fragmented_dictionary";
import TableQuery from "./table_query";
import SortedDictionary from "./sorted_dictionary";

// setFlagsFromString('--expose_gc');

export type FieldTag = "primary" | "unique" | "index" | "memory";


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

export const SCHEME_PATH = "/data/scheme.json";

export type IndexFlags = {
  isUnique: boolean,
};

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

export function isHeavyType(type: FieldType): boolean {
  return HeavyTypes.includes(type as HeavyType);
}

export class Table<KeyType extends string | number, Type> {
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

    this.mainDict = FragmentedDictionary.open(`/data/${name}/main/`);
    this.memoryFields = {};
    this.loadMemoryIndices();
    this.updateFieldIndices();
  }

  static fieldTypeToKeyType(type: FieldType): "int" | "string" {
    if (type == "JSON" || type == "json" || type == "Text") throw new Error("wrong type");
    if (type == "password" || type == "string") return "string";
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

  forEachIndex(predicate: (fieldName: string, dict: FragmentedDictionary<string | number, any>, flags: IndexFlags, tags: FieldTag[]) => void) {
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

  loadMemoryIndices() {
    const { tags } = this.scheme;
    for (const fieldName in tags) {
      const fieldTags = tags[fieldName];
      if (fieldTags.includes("memory")) {
        // TODO: implement
        // this.memoryIndices[fieldName] = this.indices[fieldName].loadAll();
      }
    }
  }

  updateFieldIndices() {
    this._fieldNameIndex = {};
    this._indexFieldName = [];
    let i = 0;

    this.forEachField((key, type) => {
      if (isHeavyType(type)) return;

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

  whereRange(fieldName: keyof Type & string, min: any, max: any): TableQuery<KeyType, Type> {
    return this.createQuery().whereRange(fieldName, min, max);
  }

  where(fieldName: keyof Type & string, value: any): TableQuery<KeyType, Type> {
    return this.createQuery().where(fieldName, value);
  }

  filter(predicate: DocCallback<KeyType, Type, boolean>): TableQuery<KeyType, Type> {
    return this.createQuery().filter(predicate);
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
    this.forEachField((name, type) => {///iterate to keep the same order in the javascript dictionary
      if (name == oldName) {
        newFields[newName] = type;
        if (isHeavyType(type)) {
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

  forEachField(predicate: (fieldName: string & keyof Type, type: FieldType, index: number) => any): void {
    const fields = this.scheme.fields;
    let i = 0;
    for (const key in fields) {
      predicate(key as any, fields[key], i++);
    }
  }

  public get length(): number {
    return this.mainDict.lenght;
  }


  protected saveScheme() {
    const schemeFile: SchemeFile = rfs(SCHEME_PATH);
    schemeFile.tables[this.name] = this.scheme;
    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });
    this.updateFieldIndices();
  }

  printField(fieldName: string) {
    return `field '${this.name}[${this.primaryKey}].${fieldName}'`;
  }

  static createIndexDictionary(tableName: string, fieldName: string, tags: FieldTag[], type: FieldType) {
    const directory = `/data/${tableName}/indices/${fieldName}/`;

    if (isHeavyType(type)) throw new Error(`Can't create an index of heavy type field ${fieldName}`);
    if (type == "json") throw new Error(`Can't create an index of json type field ${fieldName}`);

    let keyType = this.fieldTypeToKeyType(type);

    let settings: Record<string, any> = {
      maxPartitionLenght: 10 * 1000,
      maxPartitionSize: 0,
    };

    FragmentedDictionary.init({
      directory,
      keyType,
      ...settings,
    });

  }

  removeIndex(name: string) {
    if (!this.indices[name]) throw new Error(`${this.printField(name)} is not an index field!`);

    this.indices[name].destroy();
    delete this.indices[name];
    delete this.scheme.tags[name];

    this.saveScheme();
  }

  removeField(name: string) {
    const { fields, tags } = this.scheme;
    const type = fields[name];
    if (!type) throw new Error(`${this.printField(name)} doesn't exist`);


    if (name == this.primaryKey) throw new Error(`Can't remove the primary key ${this.printField(name)}! Create a new table instead.`);
    const isHeavy = isHeavyType(type);

    if (this.fieldHasAnyTag(name as any, "index", "unique")) {
      this.removeIndex(name);
    }

    if (isHeavy)
      rmie(this.getHeavyFieldDir(name));
    else {
      this.removeDocColumn(this._fieldNameIndex[name]);
    }

    delete this.scheme.fields[name];
    this.saveScheme();
  }


  addField(name: string, type: FieldType, predicate?: DocCallback<KeyType, Type, any>) {
    if (this.scheme.fields[name]) {
      throw new Error(`field '${name}' already exists`);
    }

    const filesDir = this.getHeavyFieldDir(name);

    if (isHeavyType(type) && !existsSync(filesDir)) {
      mkdirSync(filesDir);
    }

    const definedPredicate = predicate || (() => getDefaultValueForType(type));

    this.insertDocColumn(this._indexFieldName.length, (id, arr) => {
      return definedPredicate(new Document(arr, id, this, this.indices) as TDocument<KeyType, Type>);
    });

    this.scheme.fields[name] = type;
    this.saveScheme();
  }

  clear() {
    if (!this.scheme.settings.dynamicData) throw new Error(`${this.name} is not a dynamic table`);;
    return this.filter(() => true).delete(0);
  }

  getIndexDictDir(fieldName: string) {
    return `/data/${this.name}/indices/${fieldName}/`;
  }

  getMainDictDir() {
    return `/data/${this.name}/main/`;
  }

  getHeavyFieldDir(fieldName: string) {
    return `/data/${this.name}/heavy/${fieldName}/`;
  }

  getHeavyFieldFilepath(id: number | string, type: HeavyType, fieldName: string): string {
    return `${this.getHeavyFieldDir(fieldName)}${id}.${type == "Text" ? "txt" : "json"}`;
  }

  /**
   * 
   * @param data - data to insert
   * @param predicate - filing indices function
   * @returns last inserted id string
   */
  insertSquare(data: any[][]): string[] | number[] {
    return this.mainDict.insertArray(data) as string[] | number[]; //TODO: take the last id from the table instead of the dict
  }

  protected removeDocColumn(fieldIndex: number): void
  protected removeDocColumn(fieldIndex: number, returnAsDict: true): PlainObject
  protected removeDocColumn(fieldIndex: number, returnAsDict: boolean = false) {
    const result: Record<string | number, any> = {};
    this.mainDict.editRanges([[undefined, undefined]], (id, arr) => {
      const newArr = [...arr];
      if (returnAsDict) {
        result[id] = newArr[fieldIndex];
      }
      newArr.splice(fieldIndex, 1);
      return newArr;
    }, 0);

    if (returnAsDict) return result;
  }

  storeIndexValue(fieldName: string, value: string | number, id: KeyType) {
    const isUnique = this.fieldHasAnyTag(fieldName, "unique");

    const indexDict = this.indices[fieldName];
    if (isUnique) {
      indexDict.setOne(value, id);
    } else {
      const arr: KeyType[] = indexDict.getOne(value) || [];
      arr.push(id);
      indexDict.setOne(value, arr);
    }
  }

  // insert<Type extends PlainObject>(data: Type): Document<Type>
  insert(data: PlainObject & Type): KeyType {
    const validationError = Document.validateData(data, this.scheme);
    if (validationError) throw new Error(`insert failed, data is invalid for reason '${validationError}'`);

    const storable = this.makeObjectStorable(data);

    this.forEachIndex((fieldName, indexDict, { isUnique }) => {
      const value = storable[fieldName];
      if (!(typeof value == "string" || typeof value == "number"))
        throw new Error(`trying to set non (string | number) value as index ${this.printField(fieldName)}=${value}`);

      const exists = indexDict.getOne(value);
      if (isUnique && exists) throw new Error(`Unique value ${value} for ${this.printField(fieldName)} already exists`);
    });


    let id: KeyType;
    if (this.primaryKey == "id") {
      id = this.mainDict.insertArray([this.flattenObject(storable)])[0];
    } else {
      id = <KeyType>storable[this.primaryKey];
      delete storable[this.primaryKey];
      this.mainDict.setOne(id, this.flattenObject(storable));
    }

    this.forEachField((key, type) => {
      const value = storable[key];
      if (isHeavyType(type) && !!value) {
        wfs(this.getHeavyFieldFilepath(id, type as HeavyType, key), value);
      }
    });


    this.forEachIndex((fieldName) => {
      const value = <string | number>storable[fieldName];
      this.storeIndexValue(fieldName, value, id);
    });

    return id;
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

  makeObjectStorable(o: Type & PlainObject): PlainObject {
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

  protected insertDocColumn(fieldIndex: number, predicate: (id: string | number, arr: any[]) => any) {
    this.mainDict.editRanges([[undefined, undefined]], (id, arr) => {
      const newArr = [...arr];
      newArr.splice(fieldIndex, 0, predicate(id, newArr));
      return newArr;
    }, 0);
  }

  at(id: KeyType): Type | null
  at<ReturnType>(id: KeyType, predicate: DocCallback<KeyType, Type, ReturnType>): ReturnType | null
  at<ReturnType>(id: KeyType, predicate?: DocCallback<KeyType, Type, ReturnType>) {
    return this.where(this.primaryKey as any, id).select(1, predicate)[0] || null;
  }

  toJSON() {
    if (this.scheme.settings.manyRecords) {
      throw new Error("You probably don't want to download a whole table");
    }
    return this.filter(() => true).select(0);
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