import fs from "fs";
import { DocumentScheme, Table, getFilesDir } from "./table";
import { PlainObject, rfs, existsSync, wfs } from "./utils";

export const LightTypes = ["string", "number", "date", "boolean"] as const;
export const HeavyTypes = ["Text", "JSON"] as const;
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
  protected _dates: Record<string, Date> = {};
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
        if (type == "boolean") {
          let bArray = table.booleans[key];
          const currentValue = bArray.includes(this._id);

          if (currentValue != value) {
            if (value) {
              bArray.push(this._id);
            } else {
              bArray.splice(bArray.indexOf(this._id), 1);
            }
          }
          return true;
          // table.booleans[key].push();
        }

        if (type == "date") {
          this._dates[key] = value;
          return true;
        }
        if (type == "Text") {
          fs.writeFileSync(process.cwd() + this.getExternalFilename(key), value);
          return true;
        }

        if (type == "JSON") {
          fs.writeFileSync(process.cwd() + this.getExternalFilename(key), JSON.stringify(value));
          return true;
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
        if (type == "boolean") {
          return table.booleans[key].includes(this._id);
        }
        if (type == "date") {
          if (this._dates[key]) return this._dates[key];
          this._dates[key] = new Date(this._data[key]);
          return this._dates[key];
        }

        if (type == "Text") {
          return this.getTextContent(key);
        }
        if (type == "JSON") {
          return this.getJSONContent(key);
        }

        if (type)
          return this._data[key];

        return (target as PlainObject)[key];
      }
    });

    return proxy;
  }

  public getTextContent(key: string) {
    const filename = this.getExternalFilename(key);
    return existsSync(filename) ? fs.readFileSync(process.cwd() + filename, { encoding: "utf8" }) : "";
  }

  public getJSONContent(key: string) {
    return JSON.parse(this.getTextContent(key))
  }

  public serialize() {
    const result: PlainObject = {};
    const fields = this._table.scheme.fields;
    for (const key in fields) {
      const type = fields[key];

      if (type == "date") {
        result[key] = (this as PlainObject)[key].toJSON();
        continue;
      }
      if (HeavyTypes.includes(type as HeavyType)) continue;
      result[key] = this._data[key];
    }
    return result;
  }

  public toJSON() {
    const result: PlainObject = {
      id: this._id
    };

    const fields = this._table.scheme.fields;
    for (const key in fields) {
      const type = fields[key];

      // if (type == "Text") {
      //   result[key] = this.getTextContent(key);
      // } else if (type == "JSON") {
      //   result[key] == this.getJSONContent(key);
      // } else {
      // }

      result[key] = (this as PlainObject)[key];
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


};