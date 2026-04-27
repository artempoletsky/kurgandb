import fs from "fs";
import { TableScheme, Table, IndicesRecord } from "./table";
import { DataBase, SchemeFile } from "./db";
import { FieldType, PlainObject, SchemeRecord } from "./globals";
import TableUtils from "./table_utilities";
import { IndexOneNumber } from "./IndexOneNumber";
import { IndexOneString } from "./IndexOneString";

import TableUtilities from "./TableUtilities";
import _ from "lodash";
import HeapLogical from "./HeapLogical";


type RECORD_STRUCTURE_KEY = "SERVICE_FLAGS"
  | "RECORD_LENGTH"
  | "BOOLEANS"
  | "ENUMS_START"
  | "ENUMS_LEN"
  | "NUMBERS_START"
  | "NUMBERS_LEN"
  | "DATES_START"
  | "DATES_LEN"
  | "STRINGS_LEN"
  | "JSON_START"
  | "JSON_LEN"
  | "RESERVED";

const RECORD_STRUCTURE: { key: RECORD_STRUCTURE_KEY, length: number }[] = [];
RECORD_STRUCTURE.push({ key: "SERVICE_FLAGS", length: 1 });
RECORD_STRUCTURE.push({ key: "BOOLEANS", length: 2 });
RECORD_STRUCTURE.push({ key: "ENUMS_START", length: 4 });
RECORD_STRUCTURE.push({ key: "ENUMS_LEN", length: 1 });
RECORD_STRUCTURE.push({ key: "NUMBERS_START", length: 4 });
RECORD_STRUCTURE.push({ key: "NUMBERS_LEN", length: 1 });
RECORD_STRUCTURE.push({ key: "STRINGS_LEN", length: 1 });
RECORD_STRUCTURE.push({ key: "JSON_START", length: 4 });
RECORD_STRUCTURE.push({ key: "JSON_LEN", length: 1 });
RECORD_STRUCTURE.push({ key: "RESERVED", length: 50 });


const TYPE_LENGTHS: Record<FieldType, number> = {
  boolean: 1,
  enum: 1,
  number: 8,
  date: 8,
  string: 1,
  json: 1,
};

let _currentOffset = 0;
const offsets = RECORD_STRUCTURE.reduce((result, e) => {
  _currentOffset += e.length;
  result[e.key] = _currentOffset;
  return result;
}, {} as Record<RECORD_STRUCTURE_KEY, number>);



export function calculateFieldOffsets(scheme: SchemeRecord) {
  const offsetsByName: Record<string, number> = {};
  const typeBlocksLenghts: Map<FieldType, number> = new Map([
    ["boolean", 0],
    ["enum", 0],
    ["number", 0],
    ["date", 0],
    ["string", 0],
    ["json", 0],
  ]);

  for (const element of scheme.fields) {
    let v = typeBlocksLenghts.get(element.type)!;
    typeBlocksLenghts.set(element.type, v + 1);
  }


  let currentOffset = 0;
  const typeBlocksOffsets = new Map<FieldType, number>();

  for (const [type, len] of typeBlocksLenghts) {
    typeBlocksOffsets.set(type, currentOffset);
    currentOffset += len * TYPE_LENGTHS[type];
  }
  const heapStart = currentOffset;

  const indexByType: Record<FieldType, number> = {
    boolean: 0,
    enum: 0,
    number: 0,
    date: 0,
    string: 0,
    json: 0,
  };

  for (const { type, name } of scheme.fields) {
    offsetsByName[name] = indexByType[type] * TYPE_LENGTHS[type] + typeBlocksOffsets.get(type)!;
    indexByType[type]++;
  }
  return {
    heapStart,
    offsetsByName,
  };
}

type StringsMap = Map<string, {
  value: string;
  heapId: number;
}>;
export function readStrings(buffer: Buffer, keys: string[], metaOffset: number, stringsTailStart: number) {
  let result: StringsMap = new Map();
  let currentTailOffset = 0;
  for (let i = 0; i < keys.length; i++) {
    let len = buffer[metaOffset + i];
    let key = keys[i];
    let start = stringsTailStart + currentTailOffset;
    if (len < 255) {
      let end = start + len;
      result.set(key, {
        value: buffer.subarray(start, end).toString("utf8"),
        heapId: -1
      });
      currentTailOffset += len;
    } else {
      let heapId = buffer.readUint32LE(start);
      currentTailOffset += 4;
      result.set(key, {
        value: HeapLogical.getString(heapId),
        heapId,
      });
    }
  }
  return result;
}

export function writeStrings(buffer: Buffer, source: StringsMap, metaOffset: number, stringsTailStart: number) {

  let currentTailOffset = stringsTailStart;
  let currentHeadOffset = metaOffset;
  for (let [key, { value, heapId }] of source) {
    let len = Buffer.byteLength(value);
    if (len >= 255) {
      if (heapId >= 0) {
        HeapLogical.setString(heapId, value);
      } else {
        heapId = HeapLogical.createId();
        HeapLogical.setString(heapId, value);
      }
      buffer.writeUint32LE(heapId);
      currentTailOffset += 4;
      len = 255;
    } else {
      if (heapId >= 0) HeapLogical.delete(heapId);
      buffer.write(value, currentTailOffset, "utf-8")
      currentTailOffset += len;
    }

    buffer[currentHeadOffset] = len;
    currentHeadOffset++;
  }

  return currentTailOffset;
}

const DEFAULT_PAGE_SIZE = 0x2000;

export type TRecord<T, idT extends string | number, LightT = T, VisibleT = T> = ByteRecord<T, idT, LightT, VisibleT> & T;

export class ByteRecord<T, idT extends string | number, LightT, VisibleT> {
  protected _data: any[] = [];
  protected _datesObj: Record<string, Date> = {};

  protected _numbers: Float64Array;
  protected _datesNumeric?: Float64Array;
  protected _enums?: Uint8Array;
  // protected _cache: Map<number, Buffer>;
  protected _pageSize: number = DEFAULT_PAGE_SIZE;
  protected _bufferPage: Buffer;

  protected _enumsStart: number;
  protected _enumsNum: number;
  protected _numbersStart: number;
  protected _numbersNum: number;
  protected _datesStart: number;
  protected _datesNum: number;

  protected _stringsMetaStart: number;
  protected _stringsTailStart: number;
  protected _stringsNum: number;
  protected _stringsByteLengths!: Uint8Array;
  protected _stringsOffsets!: Uint16Array;
  protected _stringsCache!: string[];

  protected _id!: idT;

  protected _jsonCache!: any[];
  protected _needsStringsTailRebuilding: boolean = false;
  protected _needsJSONTailRebuilding: boolean = false;


  protected _keyType: idT extends string ? "string" : "number";
  protected _primaryKeyIndex: idT extends string ? IndexOneString : IndexOneNumber;
  protected _idIndex: number;

  public readonly _fpr: FilePatchRecord;
  public readonly _utils: TableUtilities;

  public get $id(): idT {
    return this._id;
  }


  $getJSON(index: number) {
    if (this._jsonCache[index] !== undefined) return this._jsonCache[index];

    let len = this._bufferPage.readUint8(this._jsonStart + index);
    let start = this._bufferPage.readUint16LE(this._stringsMetaStart + index + 1);
    if (start === 0xFFFF) {
      throw "implement separate file field or the heap";
    }

    this._stringsCache[index] = this._bufferPage.subarray(start, len).toString("utf-8");

    return this._stringsCache[index];
  }

  $getFlag(i: number): number {
    return (this._bufferPage[0]! & (1 << i & 7));
  }

  $setFlag(i: number, value: any) {
    const bitMask = 1 << (i & 7);

    if (value) {
      this._bufferPage[0] |= bitMask;
    } else {
      this._bufferPage[0] &= ~bitMask;
    }
  }

  $getBool(i: number): boolean {
    const byteIndex = i >> 3; // i / 8 
    const bitIndex = i & 7; // i % 8 
    // const byte = this.buffer.at(byteIndex)!;

    return (this._bufferPage[1 + byteIndex]! & (1 << bitIndex)) !== 0;
  }

  $setBool(i: number, value: boolean) {
    const byteIndex = i >> 3;
    const bitMask = 1 << (i & 7);

    if (value) {
      this._bufferPage[byteIndex] |= bitMask;
    } else {
      this._bufferPage[byteIndex] &= ~bitMask;
    }
  }

  $cast(buffer: Buffer, offset: number) {
    this._strings = readStrings(buffer, this._stringKeys, offset + this._stringsMetaOffset, offset + this._stringsTailStart);

  }

  constructor(scheme: SchemeRecord) {
    let utils = TableUtilities.fromScheme(scheme);



    this._bufferPage = Buffer.alloc(this._pageSize);
    this._fpr = new FilePatchRecord({
      pathPage: utils.getPagesFilePath(),
      pathHeap: utils.getHeapFilePath(),
      sizePage: this._pageSize,
    });

    this._utils = utils;

    // this._table = table;

    let currentReadPos = 3;
    this._enumsNum = utils.numberOfType["enum"];
    currentReadPos += 1;
    this._enumsStart = currentReadPos;
    currentReadPos += this._enumsNum * 1;

    this._numbersNum = utils.numberOfType["number"];
    currentReadPos += 1;
    this._numbersStart = currentReadPos;
    currentReadPos += this._numbersNum * 8;

    this._datesNum = utils.numberOfType["date"];
    currentReadPos += 1;
    this._datesStart = currentReadPos;
    currentReadPos += this._datesNum * 8;

    this._stringsNum = utils.numberOfType["string"];
    currentReadPos += 1;
    this._stringsMetaStart = currentReadPos;

    currentReadPos += this._stringsNum;


    // this._jsonLen = table.getNumberOfFieldsOfType("json");
    // currentReadPos += 1;
    // this._jsonStart = currentReadPos;
    // currentReadPos += this._strings * 4;

    this._numbers = new Float64Array(this._numbersNum);

    this._stringsTailStart = currentReadPos;

    this._stringsSaver = new StringsSaver({
      bufferPage: this._bufferPage,
      fpr: this._fpr,
      stringsMetaStart: this._stringsMetaStart,
      stringsTailStart: this._stringsTailStart,
      stringsNum: this._stringsNum,
    });


    let primaryKeyType: "string" | "number" = utils.scheme.fields[utils.primaryKey] as any;
    this._keyType = primaryKeyType as any;

    this._idIndex = utils.relationFieldName_Index[utils.primaryKey];
    if (primaryKeyType == "string") {
      this._primaryKeyIndex = new IndexOneString(utils, utils.primaryKey) as any;
    } else {

      this._primaryKeyIndex = new IndexOneNumber(utils as any, utils.primaryKey) as any;
    }


    let proxy = new Proxy<ByteRecord<T, idT, LightT, VisibleT>>(this, {
      set: (target: any, key: string & keyof T, value: any) => {
        if (key.startsWith("_")) {
          target[key] = value;
          return true;
        }
        this.$set(key, value);
        return true;
      },
      get: (target: any, key: string & keyof T) => {
        if (typeof target[key] == "function") {
          return target[key];
        }
        if (key == "$id") {
          return this._id;
        }
        if (key.startsWith("_")) {
          return target[key];
        }
        return this.$get(key);
      }
    });

    return proxy;
  }

  $set(fieldName: keyof T & string, value: any): void {
    const { fields } = this._table.scheme;
    const table = this._table;
    const utils = this._utils;
    const { primaryKey } = this._table;
    const tags = this._table.scheme.tags[fieldName] || [];

    // if (primaryKey == fieldName) {
    //     if (value == this._id) return;
    //     if (utils.mainDict.hasAnyId([value])) {
    //         throw utils.errorValueNotUnique(primaryKey, value);
    //     }
    //     const { keyType } = this._utils.mainDict.settings;
    //     this._id = TableRecord.storeValueOfType(value, keyType == "string" ? "string" : "number");
    //     return;
    // }

    const type = fields[fieldName];
    if (!type) {
      throw new Error(`There is no '${fieldName}' field in '${table.name}'`);
    }


    const currentValue = this.$get(fieldName);
    const indexField = table.fieldNameIndex[fieldName];

    if (type == "number") {
      this._numbers![indexField] = value;
    } else if (type == "string") {
      this._stringsSaver.setString(indexField, value);
    } else if (type == "date") {
      // this.$setString(indexField, value);
      if (typeof value == "number") {
        this._datesNumeric![indexField] = value;
      } else if (typeof value == "string") {
        throw "not implemented";
      } else if (typeof value == "object") {
        this._datesObj[indexField] = value;
        throw "not implemented"
      }
    } else if (type == "boolean") {
      this.$setBool(indexField, value);
    } else if (type == "enum") {
      throw "not implemented";
    } else if (type == "json") {
      throw "not implemented";
    }

    let newValue: any = ByteRecord.storeValueOfType(value, type);

    // if (tags.includes("heavy")) {
    //     const hasChangeListener = this._table.hasEventListener("recordChange:" + fieldName);
    //     let oldValue: any;
    //     if (hasChangeListener) {
    //         oldValue = this.$get(fieldName);
    //     }
    //     if (type == "json") {
    //         fs.writeFileSync(this.$getExternalFilename(fieldName), JSON.stringify(value));
    //     } else {
    //         fs.writeFileSync(this.$getExternalFilename(fieldName), value);
    //     }
    //     if (hasChangeListener) {
    //         this._table.triggerEvent("recordChange", {
    //             newValue,
    //             oldValue,
    //             record: this as any,
    //             fieldName,
    //         });
    //     }
    //     return;
    // }


    // const currentValue = this._data[indexField];
    // utils.changeIndexValue(fieldName, this._id, currentValue, newValue);


    // if (type == "date") {
    //     this._datesObj[fieldName] = new Date(value);
    // }

    // this._data[indexField] = newValue;


    this._table.triggerEvent("recordChange", {
      newValue,
      oldValue: currentValue,
      record: this as any,
      fieldName,
    })
  }

  protected fieldPrint(fieldName: keyof T & string, value: any) {
    return `field: ${fieldName}='${value}' in ${this.idPrint()}`;
  }

  protected idPrint() {
    return `${this._table.name}['${this._id}']`;
  }

  $get(fieldName: string): any {

    const table = this._table;
    const { fields } = this._table.scheme;
    const tags = this._table.scheme.tags[fieldName] || [];

    const type = fields[fieldName];
    if (!type) return;

    if (tags.includes("heavy")) {
      return type == "json" ? this.$getJSONContent(fieldName) : this.$getTextContent(fieldName);
    }


    // const iOfIndex = indices.indexOf(fieldName);
    // if (iOfIndex != -1) {
    //   return table.index[this._id][iOfIndex];
    // }

    const fieldIndex = table.fieldNameIndex[fieldName];

    if (type == "boolean") {
      return this.$getBool(fieldIndex);
    } else if (type == "number") {
      return this._numbers![fieldIndex];
    } else if (type == "date") {
      if (this._datesObj[fieldName]) return this._datesObj[fieldName];
      this._datesObj[fieldName] = new Date(this._datesNumeric![fieldIndex]);
      return this._datesObj[fieldName];
    } else if (type == "string") {
      return this._stringsSaver.getString(fieldIndex);
    }

    throw "not implemented";
  }

  static retrieveValueOfType(value: any, type: FieldType) {
    if (type == "boolean") {
      return !!value;
    }
    if (type == "date") {
      return new Date(value);
      // return new Date(value);
    }
    return value;
  }



  static storeValueOfType(value: Date | string | number, type: "date"): number
  static storeValueOfType(value: boolean | number, type: "boolean"): number
  static storeValueOfType(value: string, type: "string"): string
  static storeValueOfType<JSONType extends PlainObject | any[] | null>(value: JSONType, type: "json"): JSONType
  static storeValueOfType(value: any, type: FieldType): any
  static storeValueOfType(value: any, type: FieldType): string | number | PlainObject | any[] | undefined | null {
    if (type == "boolean") {
      return value ? 1 : 0;
    }
    if (type == "date") {
      if (value instanceof Date) {
        return value.getTime();
      }
      if (typeof value == "string") {
        return (new Date(value)).getTime();
      }
    }
    if (type == "number") {
      return value * 1;
    }
    return value;
  }

  public $getTextContent(key: string) {
    const filename = this.$getExternalFilename(key);
    return fs.existsSync(filename) ? fs.readFileSync(filename, "utf8") : "";
  }

  public $getJSONContent(key: string) {
    const text = this.$getTextContent(key);
    if (!text) return null;
    return JSON.parse(text);
  }

  public $create(data: T) {
    const table = this._table;
    table.scheme.fieldsOrder.forEach((key, index) => {
      const type = table.indexType[index];
      const i = table.localIndexOfType[type];
      const value = (data as any)[key];
      if (type == "boolean") {
        this.$setBool(i, value);
      } else if (type == "number") {
        this._numbers[i] = value;
      } else if (type == "string") {
        this._stringsSaver.setString(i, value);
      } else {
        throw "not implemented";
      }
    });
  }

  public $serialize(): Buffer {
    this._stringsSaver.save();
    return this._bufferPage;
  }

  public toJSON(): VisibleT {
    return this.$visible();
  }

  public $visible(): VisibleT {
    const result: PlainObject = {};
    // const { primaryKey } = this._table;
    // result[primaryKey] = this._id;

    this._utils.forEachField((key, type, tags) => {
      if (!tags.has("hidden")) {
        result[key] = this.$get(key);
      }
    });
    return result as VisibleT;
  }

  static validateData<Type>(data: Type, scheme: TableScheme, excludeId: boolean): false | string {
    const schemeFields = scheme.fields;
    // console.log(scheme);
    // console.log(data);
    for (const key in schemeFields) {
      if (excludeId && key == "id") continue;
      if ((<any>data)[key] === undefined) return `Key: '${key}' is missing`;
    }

    for (const key in data) {
      if (excludeId && key == "id") continue;

      let value: any = data[key];
      let requiredType = scheme.fields[key];
      if (!requiredType) return `Key: '${key}' is redundant`;

      if (requiredType == typeof value) continue;
      if (requiredType == "date") {
        const d = new Date(value);
        if (isNaN(d.valueOf())) return `Date key: '${key}' is invalid date`;
      }
    }

    return false;
  }

  $getExternalFilename(field: string) {
    const type = this._table.scheme.fields[field];
    return this._utils.getHeavyFieldFilepath(this._id, type, field);
  }

  $pick<KeysT extends (keyof T)[]>(...fields: KeysT): Pick<T, KeysT[number]> {
    const result: PlainObject = {};
    this._utils.forEachField((key, type) => {
      if (fields.includes(key as keyof T))
        result[key] = this.$get(key);
    })
    return result as T;
  }

  $full(): T {
    return this.$omit() as T;
  }

  $omit<KeysT extends (keyof T)[]>(...fields: KeysT): Omit<T, KeysT[number]> {
    const result: PlainObject = {};
    this._utils.forEachField((key, type) => {
      if (!fields.includes(key))
        result[key] = this.$get(key);
    })
    return result as T;
  }

  $light(): LightT {
    return this.$pick(...this._utils.getLightKeys() as any) as any;
  }

  $isValid(): boolean {
    return this._table.zObject.safeParse(this).success;
  }

};