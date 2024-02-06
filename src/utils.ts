import fs from "fs";
import { rimraf } from "rimraf";
import { DataBase } from "./db";
import { TDocument } from "./document";

export type PlainObject = Record<string, any>;

type WFSOptions = {
  pretty: boolean
} | undefined;

function abs(path: string) {
  return `${DataBase.workingDirectory}/${path}`;
}
export function wfs(filename: string, data: any, options?: WFSOptions) {
  const { pretty = false } = options || {};

  if (pretty) {
    return fs.writeFileSync(abs(filename), JSON.stringify(data, null, 2).replace(/\n/g, "\r\n"));
  } else {
    return fs.writeFileSync(abs(filename), JSON.stringify(data));
  }
}

export function rfs(filename: string, asText = false) {
  const text = fs.readFileSync(abs(filename), { encoding: "utf8" });
  if (asText) return text;
  return JSON.parse(text);
}

export function existsSync(filename: string) {
  return fs.existsSync(abs(filename));
}

export function statSync(filename: string) {
  return fs.statSync(abs(filename));
}


export function unlinkSync(filename: string) {
  fs.unlinkSync(abs(filename));
}

export function mkdirSync(dirname: string) {
  fs.mkdirSync(abs(dirname), { recursive: true });
}

/**
 * remove directory/file if exists
 * @param dirname 
 */
export function rmie(name: string) {
  if (existsSync(name)) {

    // unlinkSync(name);
    if (name.endsWith("/")) {
      // console.log(name);?

      rimraf.sync(abs(name));
      // fs.rmdirSync();
    } else {
      unlinkSync(abs(name));
    }
  }
}


/**
 * make directory if not exists
 * @param dirname 
 */
export function mdne(dirname: string) {
  if (!fs.existsSync(abs(dirname))) {
    fs.mkdirSync(abs(dirname));
  }
}

export function renameSync(oldName: string, newName: string) {
  return fs.renameSync(abs(oldName), abs(newName));
}


let PerfMeasuerements: PlainObject = {};
export function perfStart(name: string) {
  PerfMeasuerements[name] = performance.now();
  // performance.mark(name + "_start");
}
export function perfEnd(name: string) {
  PerfMeasuerements[name] -= performance.now();
  // performance.mark(name + "_end");
}

export function perfLog(name?: string) {
  // const measure = performance.measure(name, name + "_start", name + "_end");
  if (!name) {
    console.log(PerfMeasuerements);
    PerfMeasuerements = {};
  } else {
    console.log(name + ": ", perfDur(name), "ms");
  }
}

export function perfEndLog(name: string) {
  perfEnd(name);
  perfLog(name);
}

export function perfDur(name: string): number {
  return Math.floor(-PerfMeasuerements[name] * 100) / 100;
}


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

export function pick<Type>(...fields: (keyof Type)[]) {
  return (doc: TDocument<any, Type>) => {
    return doc.pick(...fields as string[]);
  }
}

export function omit<Type>(...fields: (keyof Type)[]) {
  return (doc: TDocument<any, Type>) => {
    return doc.omit(...fields as string[]);
  }
}

export function primary<KeyType extends string | number, Type>(doc: TDocument<KeyType, Type>) {
  return doc.id;
}

export function field<Type>(fieldName: keyof Type): any {
  return (doc: TDocument<any, Type>) => {
    return doc.get(fieldName as string);
  }
}

export function full<KeyType extends string | number, Type>(doc: TDocument<KeyType, Type>) {
  return doc.omit();
}

export const $ = {
  randomIndex,
  randomIndices,
  pick,
  omit,
  primary,
  field,
  full,
}