import { rimraf } from "rimraf";

import { Table, TableScheme } from "./table";
import { $, LOGS_DIRECTORY, LOG_DELIMITER, mkdirSync, rfs, wfs } from "./utils";
import FragmentedDictionary from "./fragmented_dictionary";
import fs, { existsSync } from "fs";
import vfs, { setRootDirectory } from "./virtual_fs";
import { FieldTag, FieldType, PlainObject, PluginFactory } from "./globals";
import lodash from "lodash";
import zod from "zod";

import pkg from "../package.json";
import { ResponseError } from "@artempoletsky/easyrpc";

export const SCHEME_PATH = "scheme.json";

import { exec } from "child_process";
import TableUtils, { fieldTypeToKeyType } from "./table_utilities";
import { ParsedFunction, constructFunction, parseFunction } from "./function";

export type TCreateTable<Type> = {
  name: string
  settings?: Partial<TableSettings>
  tags?: Record<string, FieldTag[]>
  fields: Record<keyof Type, FieldType>
};

export type SchemeFile = {
  tables: Record<string, TableScheme>
};

export const DefaultTableSettings = {
  // manyRecords: true,
  // dynamicData: false,

  // maxPartitionSize: 1024 * 1024 * 1024,
  // maxPartitionLenght: 10 * 1000,
  // maxIndexPartitionLenght: 100 * 1000,
}

export type TableSettings = typeof DefaultTableSettings;

export type AllTablesDict = Record<string, Table<any, any, any>>;
// const EmptyTable: TableMetadata = {
//   index: 0,
//   length: 0,
//   partitions: []
// };

const Tables: AllTablesDict = {}

let workingDirectory: string;
let initialized = false;

export type LogEntry = {
  level: "error" | "warning" | "info";
  message: string;
  details: string;
  time: string;
  timeUTC: string;
}
export class DataBase {


  public static get versionString(): string {
    return `KurganDB v${pkg.version}`;
  }


  public static get version(): string {
    return pkg.version;
  }


  public static get workingDirectory(): string {
    if (!workingDirectory) {
      const envDir = process.env.KURGANDB_DATA_DIR;
      if (envDir) {
        workingDirectory = envDir;
      } else {
        workingDirectory = process.cwd() + "/kurgandb_data";
      }
      setRootDirectory(workingDirectory);
    }
    return workingDirectory;
  }

  static init(newWorkingDirectory?: string) {
    if (newWorkingDirectory) {
      if (newWorkingDirectory.endsWith("/")) {
        newWorkingDirectory = newWorkingDirectory.slice(0, -1);
      }
      if (newWorkingDirectory == workingDirectory) return;
      workingDirectory = newWorkingDirectory;
      initialized = false;
      for (const key in Tables) {
        delete Tables[key];
      }
      vfs.setRootDirectory(workingDirectory);
    }
    if (initialized) {
      return;
    }
    const dir = this.workingDirectory;
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    if (!fs.existsSync(`${dir}/${SCHEME_PATH}`)) {
      wfs(SCHEME_PATH, {
        tables: {}
      }, {
        pretty: true
      });
    }

    this.loadAllTables();
    initialized = true;

    let dict: FragmentedDictionary<string, ParsedFunction>;
    const dictDir = "/_plugins/";
    if (existsSync(this.workingDirectory + dictDir + "settings.json")) {
      dict = FragmentedDictionary.open(dictDir);
    } else {
      debugger;
      dict = FragmentedDictionary.init({
        directory: dictDir,
        keyType: "string",
      });
    }

    for (const [fn, name] of dict.loadAll()) {
      Plugins[name] = constructFunction(fn);
    }
  }

  static loadAllTables() {
    let dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme is invalid! The 'tables' key is missing");
    for (const tableName in dbScheme.tables) {
      this.getTable(tableName);
    }
  }

  static getTables() {
    return Tables;
  }

  static getTable<T, idT extends string | number, MetaT = {}>(name: string): Table<T, idT, MetaT> {
    if (!Tables[name]) {
      // const meta = rfs(getMetaFilepath(name));

      let scheme = this.getScheme(name);
      if (!scheme) throw new ResponseError({
        message: "Table {...} doesn't exist",
        args: [name],
        statusCode: 404,
      });

      Tables[name] = new Table<T, idT, MetaT>(name, scheme);
    }

    return Tables[name] as any;
  }

  static getScheme(tableName: string): TableScheme | undefined {
    const dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme is invalid! 'tables' missing");
    return dbScheme.tables[tableName];
  }

  static doesTableExist(tableName: string): boolean {
    return !!this.getScheme(tableName);
  }

  static createTable<T, idT extends string | number, MetaT = {}>
    ({ name, fields, settings: rawSettings, tags }: TCreateTable<T>): Table<T, idT, MetaT, any, any, any> {
    if (this.doesTableExist(name)) {
      throw new Error(`Table '${name}' already exists`);
    }
    if (!rawSettings) rawSettings = {};
    if (!tags) tags = {} as Record<string, FieldTag[]>;

    const settings: TableSettings = Object.assign(DefaultTableSettings, rawSettings);

    let autoId = true;
    for (const fieldName in tags) {
      if (tags[fieldName].includes("primary")) {
        autoId = false;
        break;
      }
    }

    if (autoId) {
      tags.id = ["primary", "autoinc"];
      fields = {
        id: "number",
        ...fields,
      };
    }

    mkdirSync(`/${name}/`);
    mkdirSync(`/${name}/indices/`);
    const mainDictDir = `/${name}/main/`;
    let keyType: "int" | "string" = "int";
    for (const fieldName in tags) {
      const fieldTags = tags[fieldName];
      const tagsSet = new Set(fieldTags);
      const type = fields[<keyof T>fieldName];


      if (tagsSet.has("index") || tagsSet.has("unique")) {
        TableUtils.createIndexDictionary(name, fieldName, fieldTags, type);
      }

      if (tagsSet.has("primary")) {
        if (type !== "string" && type != "number")
          throw new Error(`can't set up field '${fieldName}' of type '${type}' as primary key for '${name}'`);
        keyType = fieldTypeToKeyType(type);
      }
    }
    // mkdirSync(mainDictDir);
    const mainDict = FragmentedDictionary.init({
      keyType,
      maxPartitionLength: 5000,
      directory: mainDictDir,
    });

    if (keyType == "int") {
      mainDict.meta.custom.lastId = 0;
    }

    mkdirSync(`/${name}/heavy/`);



    const fieldsOrderUser = Object.keys(fields);
    const fieldsOrder = new Set(fieldsOrderUser);
    for (const fieldName in fields) {
      const fieldTags = tags[fieldName] || [];
      if (fieldTags.includes("heavy")) {
        mkdirSync(`/${name}/heavy/${fieldName}/`);
        fieldsOrder.delete(fieldName);
      }
      if (fieldTags.includes("primary")) {
        fieldsOrder.delete(fieldName);
      }
      tags[fieldName] = fieldTags;
    }

    // wfs(getMetaFilepath(name), EmptyTable);

    const schemeFile: SchemeFile = rfs(SCHEME_PATH);


    schemeFile.tables[name] = {
      fields,
      fieldsOrder: Array.from(fieldsOrder),
      fieldsOrderUser,
      tags,
      settings,
    };

    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });

    return this.getTable<T, idT, MetaT>(name);
  }

  static removeTable(name: string) {
    if (!this.doesTableExist(name)) {
      throw new Error(`Table '${name}' doesn't exist`);
    }
    const schemeFile: SchemeFile = rfs(SCHEME_PATH);

    delete schemeFile.tables[name];
    delete Tables[name];
    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });

    rimraf.sync(`${this.workingDirectory}/${name}`);
  }

  static getLogsList(): string[] {
    if (!fs.existsSync(LOGS_DIRECTORY)) return [];
    let files = fs.readdirSync(LOGS_DIRECTORY);
    files = files.filter(f => f.endsWith(".txt")).map(f => f.slice(0, f.length - 4));
    return files;
  }

  static getLog(fileName: string): LogEntry[] {
    const fullPath = `${LOGS_DIRECTORY}/${fileName}.txt`;
    if (!fs.existsSync(fullPath)) throw new ResponseError("fileName", "{...} doesn't exist", [fileName]);
    const fileContents = fs.readFileSync(fullPath, "utf8");
    const recordsRaw = fileContents.split(LOG_DELIMITER);
    const result: LogEntry[] = [];

    for (const rec of recordsRaw) {
      const lines = rec.split("\n").map(l => l.trim());
      const first = lines.shift();
      if (!first) continue;
      // 10:44:33 (05:44:33): error
      const firstMatch = /(\d\d:\d\d:\d\d) \((\d\d:\d\d:\d\d)\): (\S+)/.exec(first);
      if (!firstMatch) continue;
      const time = firstMatch[1];
      const timeUTC = firstMatch[2];
      const level = firstMatch[3];

      lines.shift();
      const second = lines.shift();
      if (!second) continue;

      const message = second.trim();

      const details = lines.join("\r\n").trim();

      // console.log(second);
      // const match = /^([^\n]+)\n([^\n]+)\n([\s\S]+)$/.exec(rec)
      // console.log(match);

      result.push({
        message,
        level: level as any,
        details,
        time,
        timeUTC,
      });

    }
    return result;
  }

  static registerPlugin(name: string, factory: PluginFactory): void
  static registerPlugin(name: string, factory: ParsedFunction): void
  static registerPlugin(name: string, factory: PluginFactory | ParsedFunction) {
    let fn: PluginFactory, parsed: ParsedFunction;

    if (typeof factory == "function") {
      fn = factory;
      parsed = parseFunction(factory);
    } else {
      try {
        fn = constructFunction(factory) as PluginFactory;
      } catch (error: any) {
        throw new ResponseError("Plugin factory construnction has failed: {...}", [error.message]);
      }
      parsed = factory;
    }

    Plugins[name] = fn({
      db: DataBase,
      $: $,
      _: lodash,
      z: zod,
    });
    const dict = FragmentedDictionary.open("/_plugins/");
    dict.setOne(name, parsed);
  }

  static unregisterPlugin(name: string) {
    const dict = FragmentedDictionary.open("/_plugins/");
    dict.remove(name);
    delete Plugins[name];
  }

}

export const Plugins: PlainObject = {};
// DataBase.loadAllTables();