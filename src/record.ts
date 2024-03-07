import fs from "fs";
import { TableScheme, Table, IndicesRecord } from "./table";
import { DataBase } from "./db";
import { FieldType, PlainObject } from "./globals";

export type TRecord<idT extends string | number, T, LightT = T, VisibleT = T> = TableRecord<idT, T, LightT, VisibleT> & T;

export class TableRecord<idT extends string | number, T, LightT = T, VisibleT = T> {
  protected _id: idT;
  protected _indices: IndicesRecord;
  protected _data: any[] = [];
  protected _dates: Record<string, Date> = {};

  protected _table: Table<idT, T, any, any, LightT, VisibleT>;


  public get $id(): idT {
    return this._id;
  }

  constructor(data: any[], id: idT, table: Table<idT, T, any, any, LightT, VisibleT>, indices: IndicesRecord) {
    this._indices = indices;

    this._table = table;

    this._id = id;

    this._data = data;

    let proxy = new Proxy<TableRecord<idT, T, LightT, VisibleT>>(this, {
      set: (target: any, key: string & keyof T, value: any) => {
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
    const { primaryKey } = this._table;
    const tags = this._table.scheme.tags[fieldName] || [];

    if (primaryKey == fieldName) {
      if (value == this._id) return;
      throw new Error("Not implemented yet");
    }

    const type = fields[fieldName];
    if (!type) {
      throw new Error(`There is no '${fieldName}' field in '${table.name}'`);
    }
    let newValue: any = TableRecord.storeValueOfType(value, type as any);

    if (tags.includes("heavy")) {
      if (type == "json") {
        fs.writeFileSync(this.$getExternalFilename(fieldName), JSON.stringify(value));
      } else {
        fs.writeFileSync(this.$getExternalFilename(fieldName), value);
      }
      return;
    }

    const indexField = table.fieldNameIndex[fieldName];
    const currentValue = this._data[indexField];
    table.changeIndexValue(fieldName, this._id, currentValue, newValue);


    if (type == "date") {
      this._dates[fieldName] = new Date(value);
    }

    this._data[indexField] = newValue;
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

    this._table.forEachField((key, type, tags) => {
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
    return DataBase.workingDirectory + this._table.getHeavyFieldFilepath(this._id, type, field);
  }

  $pick(...fields: string[]): Partial<T> {
    const result: PlainObject = {};
    this._table.forEachField((key, type) => {
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
    this._table.forEachField((key, type) => {
      if (!fields.includes(key))
        result[key] = this.$get(key);
    })
    return result as T;
  }

  $light(): LightT {
    return this.$pick(...this._table.getLightKeys()) as any;
  }

};