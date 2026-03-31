import fs from "fs";
import { TableScheme, Table, IndicesRecord } from "./table";
import { DataBase } from "./db";
import { FieldType, PlainObject } from "./globals";
import TableUtils from "./table_utilities";
import { IndexOneNumber } from "./IndexOneNumber";
import { IndexOneString } from "./IndexOneString";
import FilePatchRecord from "./FilePatchRecord";


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


let _currentOffset = 0;
const offsets = RECORD_STRUCTURE.reduce((result, e) => {
    _currentOffset += e.length;
    result[e.key] = _currentOffset;
    return result;
}, {} as Record<RECORD_STRUCTURE_KEY, number>);


const DEFAULT_PAGE_SIZE = 0x2000;

export type TRecord<T, idT extends string | number, LightT = T, VisibleT = T> = ByteRecord<T, idT, LightT, VisibleT> & T;

export class ByteRecord<T, idT extends string | number, LightT, VisibleT> {
    protected _utils: TableUtils<T, idT>;
    protected _data: any[] = [];
    protected _datesObj: Record<string, Date> = {};

    protected _table: Table<T, idT, any, any, LightT, VisibleT>;

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

    protected _jsonStart: number;
    protected _jsonLen: number;
    protected _id!: idT;

    protected _jsonCache!: any[];
    protected _needsStringsTailRebuilding: boolean = false;
    protected _needsJSONTailRebuilding: boolean = false;


    protected _keyType: idT extends string ? "string" : "number";
    protected _primaryKeyIndex: idT extends string ? IndexOneString : IndexOneNumber;
    protected _idIndex: number;

    public readonly wal: FilePatchRecord;

    public get $id(): idT {
        return this._id;
    }

    $buildStringsTail() {
        let tail = "";

        const stringsOffsets = [];
        let writePosition = 0;
        for (let i = 0; i < this._stringsNum; i++) {
            const str = this.$getString(i);
            const strByteLen = Buffer.byteLength(str, "utf-8");
            if (strByteLen > 0xFF) {
                throw "not implemented";
            }
            this._bufferPage[this._stringsMetaStart + i * 3] = strByteLen;
            stringsOffsets.push(writePosition);
            tail += str;
            writePosition += strByteLen;
        }

        const JSONOffsets = [];

        for (let i = 0; i < this._jsonLen; i++) {
            const str = this.$getJSON(i);
            const strByteLen = Buffer.byteLength(str, "utf-8");
            if (strByteLen > 0xFFFF) {
                throw "not implemented";
            }
            this._bufferPage[this._stringsMetaStart + i * 3] = strByteLen;
            stringsOffsets.push(writePosition);
            tail += str;
            writePosition += strByteLen;
        }

        let tailStart = this._pageSize - writePosition;
        for (let i = 0; i < this._stringNum; i++) {
            this._bufferPage.writeInt16LE(tailStart + stringsOffsets[i], this._stringsMetaStart + i * 2 + 1);
        }


        return tail;
    }

    $readPage(page: number) {
        let buf: Buffer;
        // if (this._cache.has(page)) {
        //     buf = this._cache.get(page)!;
        // } else {
        
        //     this._cache.set(page, buf);
        // }
        this._bufferPage = this.wal.readPage(page);



        this._stringsCache = [];
        this._jsonCache = [];

        this._stringsByteLengths = new Uint8Array(this._bufferPage.buffer, this._stringsMetaStart, this._stringsNum);
        this._stringsOffsets = new Uint16Array(this._stringsNum);
        let currentOffset = this._stringsTailStart;
        for (let i = 0; i < this._stringsNum; i++) {
            this._stringsOffsets[i] = currentOffset;
            currentOffset += this._stringsByteLengths[i];
        }

        this._numbers = new Float64Array(this._bufferPage.buffer, this._numbersStart, this._numbersNum);
        this._datesNumeric = new Float64Array(this._bufferPage.buffer, this._datesStart, this._datesNum);
        this._enums = new Uint8Array(this._bufferPage.buffer, this._enumsStart, this._enumsNum);

        if (this._keyType == "number") {
            this._id = this._numbers[this._idIndex] as any;
        } else {
            this._id = this.$getString(this._idIndex) as any;
        }


        return buf;
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

    $getString(index: number) {
        if (this._stringsCache[index] !== undefined) return this._stringsCache[index];

        let len = this._stringsByteLengths[index];
        let start = this._stringsOffsets[index];
        if (len === 0xFF) {
            throw "implement separate file field or the heap";
        }

        this._stringsCache[index] = this._bufferPage.subarray(start, len).toString("utf-8");

        return this._stringsCache[index];
    }

    $setString(index: number, string: string) {
        let prev = this._stringsCache[index];
        if (prev == string) return;
        let byteLength = Buffer.byteLength(string, "utf-8");
        if (byteLength > 0xFF) {
            throw "implement separate file field or the heap";
        }
        this._needsStringsTailRebuilding = true;

        this._stringsCache[index] = string;
        this._stringsByteLengths[index] = byteLength;
    }

    $writeCacheStringsToBuffer() {
        const tail = this.$buildStringsTail();
        this._bufferPage.write(tail, this._stringsTailStart, "utf-8");
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


    constructor(table: Table<T, idT, any, any, LightT, VisibleT>, utils: TableUtils<T, idT>) {
        this.wal = new FilePatchRecord({
            pathPage: utils.getPagesFilePath(),
            pathHeap: utils.getHeapFilePath(),
            sizePage: this._pageSize,
        });

        this._utils = utils;

        this._table = table;

        let currentReadPos = 3;
        this._enumsNum = table.numberOfType["enum"];
        currentReadPos += 1;
        this._enumsStart = currentReadPos;
        currentReadPos += this._enumsNum * 1;

        this._numbersNum = table.numberOfType["number"];
        currentReadPos += 1;
        this._numbersStart = currentReadPos;
        currentReadPos += this._numbersNum * 8;

        this._datesNum = table.numberOfType["date"];
        currentReadPos += 1;
        this._datesStart = currentReadPos;
        currentReadPos += this._datesNum * 8;

        this._stringsNum = table.numberOfType["string"];
        currentReadPos += 1;
        this._stringsMetaStart = currentReadPos;

        currentReadPos += this._stringsNum;


        // this._jsonLen = table.getNumberOfFieldsOfType("json");
        // currentReadPos += 1;
        // this._jsonStart = currentReadPos;
        // currentReadPos += this._strings * 4;

        this._numbers = new Float64Array(this._numbersNum);

        this._stringsTailStart = currentReadPos;


        let primaryKeyType: "string" | "number" = table.scheme.fields[table.primaryKey] as any;
        this._keyType = primaryKeyType as any;
        if (primaryKeyType == "string") {
            this._primaryKeyIndex = new IndexOneString(table.utils, table.primaryKey) as any;
        } else {

            this._primaryKeyIndex = new IndexOneNumber(table.utils as any, table.primaryKey) as any;
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
            this.$setString(indexField, value);
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
            return this.$getString(fieldIndex);
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
        this._bufferPage = Buffer.allocUnsafe(this._pageSize);
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
                this.$setString(i, value);
            } else {
                throw "not implemented";
            }
        });
    }

    public $serialize(): Buffer {
        this.$writeCacheStringsToBuffer();
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