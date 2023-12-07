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
  public get id(): number {
    return this._id;
  }

  constructor(table: Table, id: string) {
    this._table = table;
    this._id = Table.idNumber(id);
    const obj = table.getDocumentData(id);

    const scheme = this._table.scheme;
    return new Proxy(obj, {
      set: (target: PlainObject, key: string, value: any) => {
        const type = scheme.fields[key];

        if (!type || type in LightTypes) {// if not type extend object normally
          target[key] = value;
        } else {
          throw new Error("not implemented yet");
        }
        return true;
      },
      get: (target: PlainObject, key: string, value: any) => {
        const type = scheme.fields[key];
        if (!type) {

        }
        if (type in LightTypes) {
          target[key] = value;
        } else {
          throw new Error("not implemented yet");
        }
        return true;
      }
    }) as Document;
  }

  public serialize() {
    const result: PlainObject = {};
    const scheme = this._table.scheme;
    for (const key in scheme) {
      const type = scheme.fields[key];
      if (type in HeavyTypes) continue;
      result[key] = (this as PlainObject)[key];
    }
    return result;
  }

  public toJSON() {
    const result: PlainObject = {};
    for (const key in this) {
      const type = this._table.scheme.fields[key];
      if (type && type in HeavyTypes) continue;
      result[key] = (this as PlainObject)[key];
    }
    // result.id = this.id;
    return result;
  }

  static validateData(data: PlainObject, scheme: DocumentScheme): false | string {
    return "not implemented";
  }
};