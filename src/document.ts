import fs from "fs";
import { TableScheme, Table, isHeavyType } from "./table";
import { PlainObject, rfs, existsSync, wfs } from "./utils";

export const LightTypes = ["string", "number", "date", "boolean", "json"] as const; //store their data in main json
export const HeavyTypes = ["Text", "JSON"] as const; // store their data in a separate file
export const SpecialTypes = ["password"] as const;

export const AllTypes = [...LightTypes, ...SpecialTypes, ...HeavyTypes];
export type LightType = typeof LightTypes[number];
export type HeavyType = typeof HeavyTypes[number];
export type SpecialType = typeof SpecialTypes[number];
export type FieldType = typeof AllTypes[number]

// export interface IDocument extends Record<string, any> {
//   toJSON(): PlainObject
//   serialize(): PlainObject
//   get<Type>(fieldName: string): Type
//   get(fieldName: string): any
//   getStringID(): string
//   set(fieldName: string, value: any): void
//   pick(...fields: string[]): PlainObject
//   without(...fields: string[]): PlainObject
//   light(): PlainObject
//   id: number
// }


export type TDocument<Type extends PlainObject> = Document<Type> & Type;

export class Document<Type extends PlainObject> {
  protected _id: number;
  protected _stringID: string;
  protected _partitionID: number;
  protected _table: Table<Type>;
  protected _data: any[] = [];
  protected _dates: Record<string, Date> = {};

  getStringID(): string {
    return this._stringID;
  }

  public get id(): number {
    return this._id;
  }

  partitionIsClosed(): boolean {
    return this._partitionID === undefined || this._table.getCurrentPartitionID() != this._partitionID;
  }

  constructor(table: Table<Type>, idStr: string, partitionId: number) {
    this._table = table;
    this._id = Table.idNumber(idStr);
    this._partitionID = partitionId;
    this._stringID = idStr;
    if (this._partitionID !== undefined)
      this._data = table.getDocumentData(idStr);

    let proxy = new Proxy<Document<Type>>(this, {
      set: (target: any, key: string, value: any) => {
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
    const { indices, fields } = this._table.scheme;
    const table = this._table;
    const type = fields[fieldName];
    if (!type) {
      throw new Error(`There is no '${fieldName}' field in '${table.name}'`);
    }

    const iOfIndex = indices.indexOf(fieldName);
    if (iOfIndex != -1) {
      table.index[iOfIndex] = Document.retrieveValueOfType(value, type);
      table.markIndexPartitionDirty(this._partitionID);
      return;
    }

    if (isHeavyType(type)) {
      if (type == "Text") {
        fs.writeFileSync(process.cwd() + this.getExternalFilename(fieldName), value);
      } else if (type == "JSON") {
        fs.writeFileSync(process.cwd() + this.getExternalFilename(fieldName), JSON.stringify(value));
      }
      return;
    }

    if (this.partitionIsClosed()) throw new Error(this.partitionClosedErrorMessage(fieldName));

    let newValue = value;
    if (type == "boolean") {
      newValue = value ? 1 : 0;
    } else if (type == "date") {
      this._dates[fieldName] = new Date(value);
    }

    const indexField = table.fieldNameIndex[fieldName];

    this._data[indexField] = newValue;
    table.markCurrentPartitionDirty();
  }

  protected partitionClosedErrorMessage(fieldName: string) {
    return `Partition is closed! ${this.idPrint()}.${fieldName}`;
  }

  protected idPrint() {
    return `${this._table.name}['${this._id}']`;
  }

  get(fieldName: string): any {
    const table = this._table;
    const { fields, indices } = this._table.scheme;
    const type = fields[fieldName];
    if (!type) return;

    const iOfIndex = indices.indexOf(fieldName);
    if (iOfIndex != -1) {
      return table.index[this._id][iOfIndex];
    }

    const fieldIndex = table.fieldNameIndex[fieldName];

    if (isHeavyType(type)) {
      if (type == "Text") {
        return this.getTextContent(fieldName);
      }
      if (type == "JSON") {
        return this.getJSONContent(fieldName);
      }
    }

    if (this._partitionID === undefined) throw new Error(this.partitionClosedErrorMessage(fieldName));

    if (type == "date") {
      if (this._dates[fieldName]) return this._dates[fieldName];
      this._dates[fieldName] = new Date(this._data[fieldIndex]);
      return this._dates[fieldName];
    }

    return Document.retrieveValueOfType(this._data[fieldIndex], type);
  }

  static retrieveValueOfType(value: any, type: FieldType) {
    if (type == "boolean") {
      return !!value;
    }
    if (type == "date" && typeof value == "string") {
      return Date.now();
      // return new Date(value);
    }
    return value;
  }


  static storeValueOfType(value: any, type: FieldType) {
    if (type == "boolean") {
      return value ? 1 : 0;
    }
    if (type == "date" && value instanceof Date) {
      return value.toJSON();
    }
    return value;
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
    const table = this._table;
    if (!this._data) throw new Error(`No data: ${this.idPrint()}`);


    table.indexFieldName.forEach((key, index) => {
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

  public toJSON(): Type {
    const result: PlainObject = {
      id: this._id
    };
    this._table.forEachField((key, type) => {
      if (type != "password") {
        result[key] = this.get(key);
      }
    });
    return result as Type;
  }

  static validateData(data: PlainObject, scheme: TableScheme): false | string {
    const schemeFields = scheme.fields;
    // console.log(scheme);
    // console.log(data);
    for (const key in schemeFields) {
      if (data[key] === undefined) return `Key: '${key}' is missing`;
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
    return this._table.getHeavyFieldFilepath(this._id, type as HeavyType, field);
  }

  pick(...fields: string[]): Type {
    const result: PlainObject = {};
    this._table.forEachField((key, type) => {
      if (fields.includes(key))
        result[key] = this.get(key);
    })
    return result as Type;
  }

  without(...fields: string[]): Type {
    const result: PlainObject = {};
    this._table.forEachField((key, type) => {
      if (!fields.includes(key))
        result[key] = this.get(key);
    })
    return result as Type;
  }

  light(): Type {
    return this.pick(...this._table.getLightKeys());
  }

};