import fs from "fs";
import { rimraf } from "rimraf";
import { DataBase } from "./db";
import {
  PlainObject,
  randomIndex,
  randomIndices,
  encodePassword,
  dictFromKeys,
  aggregateDictionary,
  reduceDictionary
} from "./globals";
import type { TRecord } from "./record";
import md5 from "md5";
import { ResponseError } from "@artempoletsky/easyrpc";

function pick<Type>(...fields: (keyof Type)[]) {
  return (rec: TRecord<Type, any>) => {
    return rec.$pick(...fields as string[]);
  }
}

function omit<Type>(...fields: (keyof Type)[]) {
  return (rec: TRecord<Type, any>) => {
    return rec.$omit(...fields as string[]);
  }
}

function primary<KeyType extends string | number, Type>(rec: TRecord<Type, KeyType>) {
  return rec.$id;
}

function field<Type>(fieldName: keyof Type): any {
  return (rec: TRecord<Type, any>) => {
    return rec.$get(fieldName as string);
  }
}

function full<Type>(rec: TRecord<Type, any, any, any>): Type {
  return rec.$full();
}

function light<LightT>(rec: TRecord<any, any, LightT, any>): LightT {
  return rec.$light();
}

function valid(rec: TRecord<any, any, any, any>): boolean {
  return rec.$isValid();
}

function invalid(rec: TRecord<any, any, any, any>): boolean {
  return !rec.$isValid();
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
  dictFromKeys,
  aggregateDictionary,
  reduceDictionary,
  ResponseError,
  log: writeIntoLogFile,
  light,
  valid,
  invalid,
}


type WFSOptions = {
  pretty: boolean
} | undefined;

export function absolutePath(path: string) {
  return `${DataBase.workingDirectory}/${path}`;
}
export function wfs(filename: string, data: any, options?: WFSOptions) {
  const { pretty = false } = options || {};

  if (pretty) {
    return fs.writeFileSync(absolutePath(filename), JSON.stringify(data, null, 2).replace(/\n/g, "\r\n"));
  } else {
    return fs.writeFileSync(absolutePath(filename), JSON.stringify(data));
  }
}

export function rfs(filename: string, asText = false) {
  const text = fs.readFileSync(absolutePath(filename), { encoding: "utf8" });
  if (asText) return text;
  return JSON.parse(text);
}

export function existsSync(filename: string) {
  return fs.existsSync(absolutePath(filename));
}

export function statSync(filename: string) {
  return fs.statSync(absolutePath(filename));
}


export function unlinkSync(filename: string) {
  fs.unlinkSync(absolutePath(filename));
}

export function mkdirSync(dirname: string) {
  fs.mkdirSync(absolutePath(dirname), { recursive: true });
}

/**
 * remove directory/file if exists
 * @param dirname 
 */
export function rmie(name: string) {
  rimraf.sync(absolutePath(name));

  // if (existsSync(name)) {

  //   // unlinkSync(name);
  //   if (name.endsWith("/")) {
  //     // console.log(name);?

  //     rimraf.sync(abs(name));
  //     // fs.rmdirSync();
  //   } else {
  //     unlinkSync(abs(name));
  //   }
  // }
}


/**
 * make directory if not exists
 * @param dirname 
 */
export function mdne(dirname: string) {
  if (!fs.existsSync(absolutePath(dirname))) {
    fs.mkdirSync(absolutePath(dirname));
  }
}

export function renameSync(oldName: string, newName: string) {
  return fs.renameSync(absolutePath(oldName), absolutePath(newName));
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


function formatNumber(num: number) {
  return ('0' + num).slice(-2);
}

export function getDateString(date: Date) {
  return `${formatNumber(date.getUTCDate())}.${formatNumber(date.getUTCMonth())}.${date.getUTCFullYear()}`;
}

export function getTimeString(date: Date) {
  return `${formatNumber(date.getHours())}:${formatNumber(date.getMinutes())}:${formatNumber(date.getSeconds())}` +
    ` (${formatNumber(date.getUTCHours())}:${formatNumber(date.getUTCMinutes())}:${formatNumber(date.getUTCSeconds())})`;
}

export const LOG_DELIMITER = "\r\n====================================================================================\r\n";
export const LOGS_DIRECTORY = process.cwd() + "/kurgandb_log";

export function writeIntoLogFile(message: string, details: string, logLevel: "error" | "info" | "warning" = "error") {
  const dir = LOGS_DIRECTORY;
  if (!fs.existsSync(dir)) fs.mkdirSync(dir);
  const date = new Date();
  const dateString = getDateString(date);
  const timeString = getTimeString(date);
  const filename = `${dir}/${dateString}.txt`;


  let fileContents = fs.existsSync(filename) ? fs.readFileSync(filename, "utf8") : "";

  fileContents += LOG_DELIMITER;
  fileContents += `${timeString}: ${logLevel}\r\n\r\n`;
  fileContents += `${message} \r\n\r\n`;
  fileContents += `${details} \r\n`;

  fs.writeFileSync(filename, fileContents);
}

export function logError(message: string, details?: string): Error
export function logError(err: Error): Error
export function logError(arg1: string | Error, arg2?: string): Error {
  if (typeof arg1 == "string") {
    writeIntoLogFile(arg1, arg2 || "", "error");
    return new Error(arg1);
  }

  writeIntoLogFile(arg1.message, arg1.stack || "");
  return arg1;
}

