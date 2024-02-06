import { rimraf } from "rimraf";
import { FieldType } from "./document";
import { FieldTag, Table, TableScheme } from "./table";
import { PlainObject, existsSync, mkdirSync, rfs, wfs } from "./utils";
import FragmentedDictionary from "./fragmented_dictionary";
import fs from "fs";
import vfs, { setRootDirectory } from "./virtual_fs";

export const SCHEME_PATH = "scheme.json";

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
  largeObjects: false,
  manyRecords: true,
  // maxPartitionSize: 1024 * 1024 * 1024,
  // maxPartitionLenght: 10 * 1000,
  // maxIndexPartitionLenght: 100 * 1000,
  dynamicData: false,
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
export class DataBase {

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
  }

  static loadAllTables() {
    let dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme IDocument is invalid! The 'tables' key is missing");
    for (const tableName in dbScheme.tables) {
      this.getTable(tableName);
    }
  }

  static getTables() {
    return Tables;
  }

  static getTable<KeyType extends string | number, Type, MetaObject = {}>(name: string): Table<KeyType, Type, MetaObject> {
    if (!Tables[name]) {
      // const meta = rfs(getMetaFilepath(name));
      let dbScheme: SchemeFile = rfs(SCHEME_PATH);
      Tables[name] = new Table<KeyType, Type, MetaObject>(name, dbScheme.tables[name]);
    }

    return Tables[name] as any;
  }

  static getScheme(tableName: string): TableScheme | undefined {
    const dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme IDocument is invalid! 'tables' missing");
    return dbScheme.tables[tableName];
  }

  static doesTableExist(tableName: string): boolean {
    return !!this.getScheme(tableName);
  }

  static createTable<KeyType extends string | number, Type, MetaObject = {}>
  ({ name, fields, settings: rawSettings, tags }: TCreateTable<Type>): Table<KeyType, Type, MetaObject> {
    if (this.doesTableExist(name)) {
      throw new Error(`Table '${name}' already exists`);
    }
    if (!rawSettings) rawSettings = {};
    if (!tags) tags = {} as Record<string, FieldTag[]>;

    const settings: TableSettings = Object.assign(DefaultTableSettings, rawSettings);



    mkdirSync(`/${name}/`);
    mkdirSync(`/${name}/indices/`);
    const mainDictDir = `/${name}/main/`;
    let keyType: "int" | "string" = "int";
    for (const fieldName in tags) {
      const fieldTags = tags[fieldName];
      const type = fields[<keyof Type>fieldName];


      if (Table.tagsHasFieldNameWithAnyTag(tags, fieldName, "index", "unique")) {
        Table.createIndexDictionary(name, fieldName, fieldTags, type);
      }
      if (Table.tagsHasFieldNameWithAnyTag(tags, fieldName, "primary")) {
        try {
          keyType = Table.fieldTypeToKeyType(type);
        } catch (error) {
          throw new Error(`can't set up field '${fieldName}' of type '${type}' as primary key for '${name}'`);
        }
      }
    }
    // mkdirSync(mainDictDir);
    FragmentedDictionary.init({
      keyType,
      maxPartitionLenght: 5000,
      directory: mainDictDir,
    });

    mkdirSync(`/${name}/heavy/`);




    for (const fieldName in fields) {
      const type = fields[fieldName];
      const fieldTags = tags[fieldName] || [];
      if (fieldTags.includes("heavy")) {
        mkdirSync(`/${name}/heavy/${fieldName}/`);
      }
    }

    // wfs(getMetaFilepath(name), EmptyTable);

    const schemeFile: SchemeFile = rfs(SCHEME_PATH);
    schemeFile.tables[name] = {
      fields,
      tags,
      settings
    };

    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });

    return this.getTable<KeyType, Type, MetaObject>(name);
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
}

// DataBase.loadAllTables();