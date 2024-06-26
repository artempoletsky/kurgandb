
import md5 from "md5";
import { TableScheme } from "./table";
import { AllTablesDict } from "./db";
import { GlobalScope } from "./client";
export type { LogEntry } from "./db";
export type {
  Table,
  TableScheme,
  RegisteredEvents,
  EventTableOpen,
  EventRecordChange,
  EventRecordsInsert,
  EventRecordsRemove,
  EventRecordsRemoveLight,
  RecordValidator,
} from "./table";
export type { TRecord } from "./record";


export const FieldTypes = ["string", "number", "date", "boolean", "json"] as const;

export type FieldType = typeof FieldTypes[number];

export const FieldTags = ["primary", "unique", "index", "memory", "textarea", "heavy", "hidden", "autoinc"] as const;

export type FieldTag = typeof FieldTags[number];

export type PlainObject = Record<string, any>;

export const EventNames = ["tableOpen", "recordsRemove", "recordsRemoveLight", "recordsInsert", "recordChange"] as const;

export type EventName = typeof EventNames[number];



export function randomIndex(length: number, exclude: Set<number> = new Set()): number {
  if (length <= 0) throw new Error("length must be greater than 0");

  let range = length - exclude.size;
  if (range <= 0 || exclude.size == 0) {
    return Math.floor(Math.random() * length);
  }
  let res = Math.floor(Math.random() * range);
  while (exclude.has(res)) {
    res += 1;
  }
  return res;
}

export function randomIndices(length: number, count: number) {
  if (length <= 0) throw new Error("length must be greater than 0");
  // const result: number[] = [];
  const exclude: Set<number> = new Set();
  for (let i = 0; i < count; i++) {
    const rand = randomIndex(length, exclude);
    exclude.add(rand);
  }
  return Array.from(exclude.keys());
}


export function encodePassword(password: string, method: "md5" = "md5") {
  return md5(password);
}

export function aggregateDictionary(target: Record<string, number>, summand: Record<string, number>, subtract: boolean = false) {
  const keys = new Set<string>();
  const multiplier = subtract ? -1 : 1;
  for (const key in target) {
    keys.add(key);
    target[key] += multiplier * (summand[key] || 0);
  }
  for (const key in summand) {
    if (keys.has(key)) continue;
    target[key] = (target[key] || 0) + multiplier * summand[key];
  }
}

export function dictFromKeys<Type>(keys: string[], predicate: (key: string) => Type): Record<string, Type> {
  const res: Record<string, Type> = {};
  for (const key of keys) {
    res[key] = predicate(key);
  }
  return res;
}

export function reduceDictionary<Type = any, ReturnType = any>
  (array: Type[], predicate: (result: Record<string, ReturnType>, object: Type, index: number) => void): Record<string, ReturnType> {
  return array.reduce((res, obj, i) => {
    predicate(res, obj, i);
    return res;
  }, {} as Record<string, ReturnType>);
}


export function formToDocument(form: HTMLFormElement, scheme: TableScheme): PlainObject {
  const result: PlainObject = {};
  const formData = new FormData(form);

  for (const fieldName in scheme.fields) {
    const type = scheme.fields[fieldName];
    const value = formData.get(fieldName)?.toString() || "";
    if (type == "number") {
      result[fieldName] = parseFloat(value);
    } if (type == "json") {
      result[fieldName] = JSON.parse(value);
    } else {
      result[fieldName] = value;
    }
  }
  return result;
}

export type PluginFactory = (scope: GlobalScope) => Promise<PlainObject>;