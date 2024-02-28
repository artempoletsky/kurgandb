import type { TDocument } from "./document";
import md5_fun from "md5";
import { TableScheme } from "./table";


export const FieldTypes = ["string", "number", "date", "boolean", "json"] as const;

export type FieldType = typeof FieldTypes[number];

export const FieldTags = ["primary", "unique", "index", "memory", "textarea", "heavy", "hidden", "autoinc"] as const;

export type FieldTag = typeof FieldTags[number];

export type PlainObject = Record<string, any>;


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

function pick<Type>(...fields: (keyof Type)[]) {
  return (doc: TDocument<any, Type>) => {
    return doc.pick(...fields as string[]);
  }
}

function omit<Type>(...fields: (keyof Type)[]) {
  return (doc: TDocument<any, Type>) => {
    return doc.omit(...fields as string[]);
  }
}

function primary<KeyType extends string | number, Type>(doc: TDocument<KeyType, Type>) {
  return doc.id;
}

function field<Type>(fieldName: keyof Type): any {
  return (doc: TDocument<any, Type>) => {
    return doc.get(fieldName as string);
  }
}

function full<KeyType extends string | number, Type>(doc: TDocument<KeyType, Type>) {
  return doc.omit();
}

export const md5 = md5_fun;

export function encodePassword(password: string, method: "md5" = "md5") {
  return md5(password);
}

export const $ = {
  randomIndex,
  randomIndices,
  pick,
  omit,
  primary,
  field,
  full,
  md5,
  encodePassword,
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