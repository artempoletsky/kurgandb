import { rimraf } from "rimraf";
import { FieldType } from "./document";
import { SCHEME_PATH, TableMetadata, TableScheme, TableSettings, isHeavyType } from "./table";
import { mkdirSync, rfs, wfs } from "./utils";


export type TCreateTable = {
  name: string
  settings?: Partial<TableSettings>
  fields: Record<string, FieldType>
};

export type SchemeFile = {
  tables: Record<string, TableScheme>
};

export const DefaultTableSettings: TableSettings = {
  largeObjects: false,
  manyRecords: true,
  maxPartitionSize: 1024 * 1024 * 1024,
  maxPartitionLenght: 10,
}


const EmptyTable: TableMetadata = {
  index: 0,
  length: 0,
  partitions: []
};

export class DataBase {
  static getScheme(tableName: string): TableScheme | undefined {
    const dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme IDocument is invalid! 'tables' missing");
    return dbScheme.tables[tableName];
  }

  static isTableExist(tableName: string): boolean {
    return !!this.getScheme(tableName);
  }

  static createTable({ name, fields, settings }: TCreateTable) {
    if (this.isTableExist(name)) {
      throw new Error(`Table '${name}' already exists`);
    }
    if (!settings) settings = {};

    settings = {
      ...DefaultTableSettings,
      ...settings
    } as TableSettings;

    mkdirSync(`/data/${name}/`);
    for (const fieldName in fields) {
      const type = fields[fieldName];
      if (isHeavyType(type)) {
        mkdirSync(`/data/${name}/${fieldName}/`);
      }
    }

    wfs(`/data/${name}/meta.json`, EmptyTable);

    const schemeFile: SchemeFile = rfs(SCHEME_PATH);
    schemeFile.tables[name] = {
      fields,
      settings: settings as TableSettings,
    };

    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });
  }

  static removeTable(name: string) {
    if (!this.isTableExist(name)) {
      throw new Error(`Table '${name}' doesn't exist`);
    }
    const schemeFile: SchemeFile = rfs(SCHEME_PATH);

    delete schemeFile.tables[name];
    wfs(SCHEME_PATH, schemeFile, {
      pretty: true
    });

    rimraf.sync(`${process.cwd()}/data/${name}/`);
  }

}