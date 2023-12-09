import fs from "fs";
import { TableScheme, Table, getFilesDir, isHeavyType } from "./table";
import { PlainObject, rfs, existsSync, wfs } from "./utils";

export const LightTypes = ["string", "number", "date", "boolean", "json"] as const; //store their data in main json
export const HeavyTypes = ["Text", "JSON"] as const; // store their data in a separate file
export const SpecialTypes = ["password"] as const;

export const AllTypes = [...LightTypes, ...SpecialTypes, ...HeavyTypes];
export type LightType = typeof LightTypes[number];
export type HeavyType = typeof HeavyTypes[number];
export type SpecialType = typeof SpecialTypes[number];
export type FieldType = typeof AllTypes[number]

export interface IDocument extends Record<string, any> {
  toJSON(): PlainObject
  serialize(): PlainObject
  get<Type>(fieldName: string): Type
  get(fieldName: string): any
  getStringID(): string
  set(fieldName: string, value: any): void
  pick(...fields: string[]): PlainObject
  without(...fields: string[]): PlainObject
  light(): PlainObject
  id: number
}

export class Document implements IDocument {
  protected _id: number;
  protected _stringID: string;
  protected _partitionID: number;
  protected _table: Table;
  protected _data: any[];
  protected _dates: Record<string, Date> = {};

  getPartitionID(): number {
    return this._partitionID;
  }

  getStringID(): string {
    return this._stringID;
  }

  public get id(): number {
    return this._id;
  }

  constructor(table: Table, id: string) {
    this._table = table;
    this._id = Table.idNumber(id);
    this._partitionID = table.getCurrentPartitionID();
    this._stringID = id;
    const obj = table.getDocumentData(id);
    this._data = obj;

    let proxy = new Proxy(this, {
      set: (target: Document, key: string, value: any) => {
        this.set(key, value);
        return true;
      },
      get: (target: any, key: string) => {
        if (typeof target[key] == "function") {
          return target[key];
        }
        if (key == "id") {
          return this._id;
        }
        if (key.startsWith("_")) {
          return target[key];
        }
        return this.get(key);
      }
    });

    return proxy;
  }

  set(fieldName: string, value: any): void {

    const scheme = this._table.scheme;
    const type = scheme.fields[fieldName];
    if (!isHeavyType(type) && this._table.getCurrentPartitionID() != this._partitionID) {
      throw new Error("Can't modify document in closed partition");
    }
    if (!type) {
      throw new Error(`There is no '${fieldName}' field in '${this._table.name}'`);
    }

    let newValue = value;
    if (type == "boolean") {
      newValue = value ? 1 : 0;
    } else if (type == "date") {
      this._dates[fieldName] = new Date(value);
    } else if (type == "Text") {
      fs.writeFileSync(process.cwd() + this.getExternalFilename(fieldName), value);
      return;
    } else if (type == "JSON") {
      fs.writeFileSync(process.cwd() + this.getExternalFilename(fieldName), JSON.stringify(value));
      return;
    }

    const index = this._table.fieldNameIndex[fieldName];
    this._data[index] = newValue;
    this._table.markCurrentPartitionDirty();
  }

  get<Type>(fieldName: string): Type
  get(fieldName: string): any {
    const scheme = this._table.scheme;
    const type = scheme.fields[fieldName];
    const index = this._table.fieldNameIndex[fieldName];

    if (!type) return;

    if (type == "boolean") {
      return !!this._data[index];
    }
    if (type == "date") {
      if (this._dates[fieldName]) return this._dates[fieldName];
      this._dates[fieldName] = new Date(this._data[index]);
      return this._dates[fieldName];
    }

    if (type == "Text") {
      return this.getTextContent(fieldName);
    }
    if (type == "JSON") {
      return this.getJSONContent(fieldName);
    }

    return this._data[index];

    // throw new Error(`There is no '${fieldName}' field in '${this._table.name}'`);
  }



  public getTextContent(key: string) {
    const filename = this.getExternalFilename(key);
    return existsSync(filename) ? fs.readFileSync(process.cwd() + filename, { encoding: "utf8" }) : "";
  }

  public getJSONContent(key: string) {
    return JSON.parse(this.getTextContent(key))
  }

  public serialize(): any[] {
    const result: any[] = [];
    this._table.forEachField((key, type, index) => {
      if (isHeavyType(type)) return;
      if (type == "date" && this._dates[key]) {
        result[index] = this._dates[key].toJSON();
        return;
      }
      result[index] = this._data[index];
    })
    return result;
  }

  public toJSON() {
    const result: PlainObject = {
      id: this._id
    };
    this._table.forEachField((key, type) => {
      if (type != "password") {
        result[key] = this.get(key);
      }
    });
    return result;
  }

  static validateData(data: PlainObject, scheme: TableScheme): false | string {
    const schemeFields = scheme.fields;
    // console.log(scheme);
    // console.log(data);
    for (const key in schemeFields) {
      if (!data[key]) return `Key: '${key}' is missing`;
    }

    for (const key in data) {
      let value = data[key];
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

  getExternalFilename(field: string) {
    const type = this._table.scheme.fields[field];
    let extension;
    if (type == "Text") {
      extension = ".txt";
    } else if (type == "JSON") {
      extension = ".json";
    } else {
      throw new Error("never");
    }
    return getFilesDir(this._table.name) + this.id + "_" + field + extension;
  }

  pick(...fields: string[]): PlainObject {
    const result: PlainObject = {};
    this._table.forEachField((key, type) => {
      if (fields.includes(key))
        result[key] = this.get(key);
    })
    return result;
  }

  without(...fields: string[]): PlainObject {
    const result: PlainObject = {};
    this._table.forEachField((key, type) => {
      if (!fields.includes(key))
        result[key] = this.get(key);
    })
    return result;
  }

  light(): PlainObject {
    return this.pick(...this._table.getLightKeys());
  }

};