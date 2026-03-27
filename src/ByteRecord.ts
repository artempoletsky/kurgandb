import fs from "fs";
import { TableScheme, Table, IndicesRecord } from "./table";
import { DataBase } from "./db";
import { FieldType, PlainObject } from "./globals";
import TableUtils from "./table_utilities";


type RECORD_STRUCTURE_KEY = "SERVICE_FLAGS"
    | "RECORD_LENGTH"
    | "BOOLEANS"
    | "ENUMS_START"
    | "ENUMS_LEN"
    | "NUMBERS_START"
    | "NUMBERS_LEN"
    | "DATES_START"
    | "DATES_LEN"
    | "STRINGS_START"
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
RECORD_STRUCTURE.push({ key: "STRINGS_START", length: 4 });
RECORD_STRUCTURE.push({ key: "STRINGS_LEN", length: 1 });
RECORD_STRUCTURE.push({ key: "JSON_START", length: 4 });
RECORD_STRUCTURE.push({ key: "JSON_LEN", length: 1 });
RECORD_STRUCTURE.push({ key: "RESERVED", length: 50 });


let _currentOffset = 0;
const offsets = RECORD_STRUCTURE.reduce((result, e) => {
    _currentOffset += e.length;
    result[e.key] = _currentOffset;
    return result;
}, {} as Record<RECORD_STRUCTURE_KEY, number>);


// User data
const BOOLEANS_BLOCK = 20; //2 bytes
const NUMBERS_BLOCK = 22; //varied


export class ByteRecord<T, idT extends string | number, LightT, VisibleT> {
    protected _id: idT;
    protected _utils: TableUtils<T, idT>;
    protected _data: any[] = [];
    protected _dates: Record<string, Date> = {};

    protected _table: Table<T, idT, any, any, LightT, VisibleT>;

    protected _numbers: Float32Array;
    protected _dates: Float32Array;
    protected _enums: Uint8Array;


    public get $id(): idT {
        return this._id;
    }

    protected bufferFixed: Buffer;

    $readFromFile(offset: number, len: number): Buffer {

    }

    $readAll() {
        let start = this.bufferFixed.readUInt32LE(offsets.NUMBERS_START);
        let len = this.bufferFixed.readUInt32LE(offsets.NUMBERS_LEN);
        let buf = this.$readFromFile(start, len * 4);
        this._numbers = new Float32Array(buf.buffer, buf.byteOffset, len);


        start = this.bufferFixed.readUInt32LE(offsets.ENUMS_START);
        len = this.bufferFixed.readUInt32LE(offsets.ENUMS_LEN);
        buf = this.$readFromFile(start, len);
        this._enums = new Uint8Array(buf.buffer, buf.byteOffset, len);

    }


    $readNumberFile(index: number) {
        const offset = this.bufferFixed.readUInt32LE(offsets.NUMBERS_START) + index * 4;
        return this.$readFromFile(offset, 4).readFloatLE(0);
    }

    $getFlag(i: number): number {
        return (this.bufferFixed[0]! & (1 << i & 7));
    }

    $setFlag(i: number, value: any) {
        const bitMask = 1 << (i & 7);

        if (value) {
            this.bufferFixed[0] |= bitMask;
        } else {
            this.bufferFixed[0] &= ~bitMask;
        }
    }

    $getBool(i: number): boolean {
        const byteIndex = i >> 3; // i / 8 
        const bitIndex = i & 7; // i % 8 
        // const byte = this.buffer.at(byteIndex)!;

        return (this.bufferFixed[1 + byteIndex]! & (1 << bitIndex)) !== 0;
    }

    $setBool(i: number, value: boolean) {
        const byteIndex = i >> 3;
        const bitMask = 1 << (i & 7);

        if (value) {
            this.bufferFixed[byteIndex] |= bitMask;
        } else {
            this.bufferFixed[byteIndex] &= ~bitMask;
        }
    }


    constructor(data: any[], id: idT, table: Table<T, idT, any, any, LightT, VisibleT>, utils: TableUtils<T, idT>) {
        this._utils = utils;

        this._table = table;

        this._id = id;

        this._data = data;

        this.bufferFixed = Buffer.alloc(2);

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
                });
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