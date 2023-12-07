import { DocumentScheme, Table } from "./table";
import { PlainObject } from "./utils";

export const LightTypes = ["string", "number", "date"] as const;
export const HeavyTypes = ["Text", "File"] as const;
export type LightType = typeof LightTypes[number];
export type HeavyType = typeof HeavyTypes[number];
export type FieldType = LightType | HeavyType;

export interface IDocument extends Record<string, any> {
  toJSON(): PlainObject
  serialize(): PlainObject
  id: number
}

export class Document implements IDocument {
  protected _id: number;
  protected _table: Table;
  protected _data: PlainObject;
  public get id(): number {
    return this._id;
  }

  constructor(table: Table, id: string) {
    this._table = table;
    this._id = Table.idNumber(id);
    const obj = table.getDocumentData(id);
    this._data = obj;

    const scheme = this._table.scheme;
    let proxy = new Proxy(this, {
      set: (target: Document, key: string, value: any) => {
        const type = scheme.fields[key];
        if (type in HeavyTypes) {
          throw new Error("not implemented yet");
        }
        if (type) {
          this._data[key] = value;
          return true;
        }
        (target as PlainObject)[key] = value;
        return true;
      },
      get: (target: Document, key: string) => {
        const type = scheme.fields[key];
        if (type in HeavyTypes) {
          throw new Error("not implemented yet");
        }
        if (type)
          return this._data[key];

        return (target as PlainObject)[key];
      }
    });

    return proxy;
  }

  public serialize() {
    const result: PlainObject = {};
    const fields = this._table.scheme.fields;
    for (const key in fields) {
      const type = fields[key];
      if (type in HeavyTypes) continue;
      result[key] = this._data[key];
    }
    return result;
  }

  public toJSON() {
    const result: PlainObject = {
      id: this._id
    };

    for (const key in this) {
      if (key.startsWith("_")) continue;
      const type = this._table.scheme.fields[key];
      if (type && type in HeavyTypes) continue;

      result[key] = (this as PlainObject)[key];
    }
    for (const key in this._data) {
      result[key] = this._data[key];
    }
    return result;
  }

  static validateData(data: PlainObject, scheme: DocumentScheme): false | string {
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
    }

    return false;
  }
};