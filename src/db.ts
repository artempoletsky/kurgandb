import { FieldType } from "./document";
import { SCHEME_PATH, TableScheme, getFilepath, getFilesDir } from "./table";
import { rfs, rmie, wfs } from "./utils";

export type TCreateTable = {
  name: string
  fields: Record<string, FieldType>
};

export type SchemeFile = {
  tables: Record<string, TableScheme>
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

  static createTable({ name, fields }: TCreateTable) {
    if (this.isTableExist(name)) {
      throw new Error(`Table '${name}' already exists`);
    }
    const schemeFile: SchemeFile = rfs(SCHEME_PATH);
    schemeFile.tables[name] = {
      fields,
      settings: {
        largeObjects: false,
        manyRecords: true,
        maxPartitionSize: 1024 * 1024
      }
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

    rmie(getFilepath(name));
    rmie(getFilesDir(name));
  }

}