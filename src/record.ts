import fs from "fs";
import { TableScheme, Table, IndicesRecord } from "./table";
import { DataBase } from "./db";
import { FieldType, PlainObject } from "./globals";
import TableUtils from "./table_utilities";

export type TRecord<T, idT extends string | number, LightT = T, VisibleT = T> = TableRecord<T, idT, LightT, VisibleT> & T;

export class TableRecord<T, idT extends string | number, LightT, VisibleT> {
  protected _id: idT;
  protected _utils: TableUtils<T, idT>;
  protected _data: any[] = [];
  protected _dates: Record<string, Date> = {};

  protected _table: Table<T, idT, any, any, LightT, VisibleT>;


  public get $id(): idT {
    return this._id;
  }

  constructor(data: any[], id: idT, table: Table<T, idT, any, any, LightT, VisibleT>, utils: TableUtils<T, idT>) {
    this._utils = utils;

    this._table = table;

    this._id = id;

    this._data = data;

    let proxy = new Proxy<TableRecord<T, idT, LightT, VisibleT>>(this, {
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

    if (primaryKey == fieldName) {
      if (value == this._id) return;
      if (utils.mainDict.hasAnyId([value])) {
        throw utils.errorValueNotUnique(primaryKey, value);
      }
      const { keyType } = this._utils.mainDict.settings;
      this._id = TableRecord.storeValueOfType(value, keyType == "string" ? "string" : "number");
      return;
    }

    const type = fields[fieldName];
    if (!type) {
      throw new Error(`There is no '${fieldName}' field in '${table.name}'`);
    }
    let newValue: any = TableRecord.storeValueOfType(value, type);

    if (tags.includes("heavy")) {
      const hasChangeListener = this._table.hasEventListener("recordChange:" + fieldName);
      let oldValue: any;
      if (hasChangeListener) {
        oldValue = this.$get(fieldName);
      }
      if (type == "json") {
        fs.writeFileSync(this.$getExternalFilename(fieldName), JSON.stringify(value));
      } else {
        fs.writeFileSync(this.$getExternalFilename(fieldName), value);
      }
      if (hasChangeListener) {
        this._table.triggerEvent("recordChange", {
          newValue,
          oldValue,
          record: this as any,
          fieldName,
        })
      }
      return;
    }

    const indexField = table.fieldNameIndex[fieldName];
    const currentValue = this._data[indexField];
    utils.changeIndexValue(fieldName, this._id, currentValue, newValue);


    if (type == "date") {
      this._dates[fieldName] = new Date(value);
    }

    this._data[indexField] = newValue;


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
    const { primaryKey } = this._table;

    if (primaryKey == fieldName) {
      return this._id;
    }
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



    if (type == "date") {
      if (this._dates[fieldName]) return this._dates[fieldName];
      this._dates[fieldName] = new Date(this._data[fieldIndex]);
      return this._dates[fieldName];
    }

    return TableRecord.retrieveValueOfType(this._data[fieldIndex], type);
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

  public $serialize(): any[] {
    const result: any[] = [];
    const table = this._table;
    if (!this._data) throw new Error(`No data: ${this.idPrint()}`);


    table.scheme.fieldsOrder.forEach((key, index) => {
      const type = table.indexType[index];
      if (type == "date" && this._dates[key]) {
        result[index] = this._dates[key].toJSON();
        return;
      }

      if (!this._data) throw new Error("No data");
      result[index] = this._data[index];
    });

    return result;
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

  $pick(...fields: string[]): Partial<T> {
    const result: PlainObject = {};
    this._utils.forEachField((key, type) => {
      if (fields.includes(key))
        result[key] = this.$get(key);
    })
    return result as T;
  }

  $full(): T {
    return this.$omit() as T;
  }

  $omit(...fields: string[]): Partial<T> {
    const result: PlainObject = {};
    this._utils.forEachField((key, type) => {
      if (!fields.includes(key))
        result[key] = this.$get(key);
    })
    return result as T;
  }

  $light(): LightT {
    return this.$pick(...this._utils.getLightKeys()) as any;
  }

  $isValid(): boolean {
    return this._table.zObject.safeParse(this).success;
  }

};