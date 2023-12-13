import { rimraf } from "rimraf";
import { FieldType } from "./document";
import { SCHEME_PATH, Table, TableMetadata, TableScheme, getMetaFilepath, isHeavyType } from "./table";
import { PlainObject, mkdirSync, rfs, wfs } from "./utils";


export type TCreateTable<Type> = {
  name: string
  settings?: Partial<TableSettings>
  indices: (keyof Type)[]
  fields: Record<keyof Type, FieldType>
};

export type SchemeFile = {
  tables: Record<string, TableScheme>
};

export const DefaultTableSettings = {
  largeObjects: false,
  manyRecords: true,
  maxPartitionSize: 1024 * 1024 * 1024,
  maxPartitionLenght: 10 * 1000,
  maxIndexPartitionLenght: 100 * 1000,
  dynamicData: false,
}

export type TableSettings = typeof DefaultTableSettings;

const EmptyTable: TableMetadata = {
  index: 0,
  length: 0,
  partitions: []
};

const Tables: Record<string, Table<any>> = {}


export class DataBase {

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

  static getTable(name: string) {
    if (!Tables[name]) {
      const meta = rfs(getMetaFilepath(name));
      let dbScheme: SchemeFile = rfs(SCHEME_PATH);
      Tables[name] = new Table(name, dbScheme.tables[name], meta, Table.loadIndex(name, meta, dbScheme.tables[name]));
    }

    return Tables[name];
  }

  static getScheme(tableName: string): TableScheme | undefined {
    const dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme IDocument is invalid! 'tables' missing");
    return dbScheme.tables[tableName];
  }

  static isTableExist(tableName: string): boolean {
    return !!this.getScheme(tableName);
  }

  static createTable<Type extends PlainObject>({ name, fields, settings, indices: unsortedIndices }: TCreateTable<Type>): Table<Type> {
    if (this.isTableExist(name)) {
      throw new Error(`Table '${name}' already exists`);
    }
    if (!settings) settings = {};

    settings = {
      ...DefaultTableSettings,
      ...settings
    };

    const indices: string[] = Table.arrangeSchemeIndices(fields, unsortedIndices as string[]);

    mkdirSync(`/data/${name}/`);

    wfs(Table.getIndexFilename(name, 0), {});

    for (const fieldName in fields) {
      const type = fields[fieldName];
      if (isHeavyType(type)) {
        mkdirSync(`/data/${name}/${fieldName}/`);
      }
    }

    wfs(getMetaFilepath(name), EmptyTable);

    const schemeFile: SchemeFile = rfs(SCHEME_PATH);
    schemeFile.tables[name] = {
      fields,
      indices,
      settings: settings as TableSettings,
    };

    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });

    return this.getTable(name);
  }

  static removeTable(name: string) {
    if (!this.isTableExist(name)) {
      throw new Error(`Table '${name}' doesn't exist`);
    }
    const schemeFile: SchemeFile = rfs(SCHEME_PATH);

    delete schemeFile.tables[name];
    delete Tables[name];
    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });

    rimraf.sync(`${process.cwd()}/data/${name}/`);
  }
}

// DataBase.loadAllTables();