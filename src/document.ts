import fs from "fs";
import { TableScheme, Table, isHeavyType, IndicesRecord, MainDict } from "./table";
import { PlainObject, rfs, existsSync, wfs } from "./utils";

export const LightTypes = ["string", "number", "date", "boolean", "json"] as const; //store their data in main json
export const HeavyTypes = ["Text", "JSON"] as const; // store their data in a separate file
export const SpecialTypes = ["password"] as const;

export const AllTypes = [...LightTypes, ...SpecialTypes, ...HeavyTypes];
export type LightType = typeof LightTypes[number];
export type HeavyType = typeof HeavyTypes[number];
export type SpecialType = typeof SpecialTypes[number];
export type FieldType = typeof AllTypes[number]


export type TDocument<KeyType extends string | number, Type> = Document<KeyType, Type> & Type;

export class Document<KeyType extends string | number, Type> {
  protected _id: string | number;
  protected _indices: IndicesRecord;
  protected _data: any[] = [];
  protected _dates: Record<string, Date> = {};

  protected _table: Table<KeyType, Type>;


  public get id(): string | number {
    return this._id;
  }

  constructor(data: any[], id: string | number, table: Table<KeyType, Type>, indices: IndicesRecord) {
    this._indices = indices;

    this._table = table;

    this._id = id;

    this._data = data;

    let proxy = new Proxy<Document<KeyType, Type>>(this, {
      set: (target: any, key: string & keyof Type, value: any) => {
        this.set(key, value);
        return true;
      },
      get: (target: any, key: string & keyof Type) => {
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

  set(fieldName: keyof Type & string, value: any): void {
    const { fields, tags } = this._table.scheme;
    const table = this._table;
    const type = fields[fieldName];
    if (!type) {
      throw new Error(`There is no '${fieldName}' field in '${table.name}'`);
    }

    let newValue: any = Document.storeValueOfType(value, type as any);
    if (type == "date") {
      this._dates[fieldName] = new Date(value);
    }

    const index = this._indices[fieldName];
    const docID = this._id;
    if (index) {
      const currentValue = this.get(fieldName);
      if (currentValue === newValue) {
        return;
      }

      if (table.fieldHasAnyTag(fieldName, "unique")) {
        if (index.getOne(newValue)) throw new Error(`attempting to create a duplicate on the unique ${this.fieldPrint(fieldName, newValue)}`);
        index.remove(currentValue);
        index.insertOne(newValue, docID);
      } else {
        const currentArr: any[] | undefined = index.getOne(currentValue);
        const newArr: any[] = index.getOne(newValue) || [];
        if (currentArr) {
          currentArr.splice(currentArr.indexOf(docID), 1);
          if (!currentArr.length) {
            index.remove(currentValue);
          }
        }
        newArr.push(docID);
        index.insertOne(newValue, newArr);
      }
    }


    if (isHeavyType(type)) {
      if (type == "Text") {
        fs.writeFileSync(process.cwd() + this.getExternalFilename(fieldName), value);
      } else if (type == "JSON") {
        fs.writeFileSync(process.cwd() + this.getExternalFilename(fieldName), JSON.stringify(value));
      }
      return;
    }



    const indexField = table.fieldNameIndex[fieldName];

    this._data[indexField] = newValue;
  }

  protected partitionClosedErrorMessage(fieldName: string) {
    return `Partition is closed! ${this.idPrint()}.${fieldName}`;
  }

  protected fieldPrint(fieldName: keyof Type & string, value: any) {
    return `field: ${fieldName}='${value}' in ${this.idPrint()}`;
  }

  protected idPrint() {
    return `${this._table.name}['${this._id}']`;
  }

  get(fieldName: string & keyof Type): any {
    const table = this._table;
    const { fields } = this._table.scheme;
    const type = fields[fieldName];
    if (!type) return;

    // const iOfIndex = indices.indexOf(fieldName);
    // if (iOfIndex != -1) {
    //   return table.index[this._id][iOfIndex];
    // }

    const fieldIndex = table.fieldNameIndex[fieldName];

    if (isHeavyType(type)) {
      if (type == "Text") {
        return this.getTextContent(fieldName);
      }
      if (type == "JSON") {
        return this.getJSONContent(fieldName);
      }
    }


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
      return new Date(value);
      // return new Date(value);
    }
    return value;
  }



  static storeValueOfType(value: Date | string | number, type: "date"): number
  static storeValueOfType(value: boolean | number, type: "boolean"): number
  static storeValueOfType(value: string, type: "string"): string
  static storeValueOfType(value: string, type: "Text"): string
  static storeValueOfType(value: string, type: "password"): string
  static storeValueOfType<JSONType extends PlainObject | any[] | null>(value: JSONType, type: "json"): JSONType
  static storeValueOfType<JSONType extends PlainObject | any[] | null>(value: JSONType, type: "JSON"): JSONType
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

  public getTextContent(key: string) {
    const filename = this.getExternalFilename(key);
    return existsSync(filename) ? fs.readFileSync(process.cwd() + filename, { encoding: "utf8" }) : "";
  }

  public getJSONContent(key: string) {
    const text = this.getTextContent(key);
    if (!text) return null;
    return JSON.parse(text);
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

  static validateData<Type>(data: Type & PlainObject, scheme: TableScheme): false | string {
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