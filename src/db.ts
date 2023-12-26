import { rimraf } from "rimraf";
import { FieldType } from "./document";
import { FieldTag, SCHEME_PATH, Table, TableScheme, getMetaFilepath, isHeavyType } from "./table";
import { PlainObject, mkdirSync, rfs, wfs } from "./utils";
import FragmentedDictionary from "./fragmented_dictionary";


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

export type AllTablesDict = Record<string, Table<any, any>>;
// const EmptyTable: TableMetadata = {
//   index: 0,
//   length: 0,
//   partitions: []
// };

const Tables: AllTablesDict = {}


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

  static getTable<KeyType extends string | number, Type>(name: string): Table<KeyType, Type> {
    if (!Tables[name]) {
      // const meta = rfs(getMetaFilepath(name));
      let dbScheme: SchemeFile = rfs(SCHEME_PATH);
      Tables[name] = new Table<KeyType, Type>(name, dbScheme.tables[name]);
    }

    return Tables[name] as any;
  }

  static getScheme(tableName: string): TableScheme | undefined {
    const dbScheme: SchemeFile = rfs(SCHEME_PATH);
    if (!dbScheme?.tables) throw new Error("Scheme IDocument is invalid! 'tables' missing");
    return dbScheme.tables[tableName];
  }

  static isTableExist(tableName: string): boolean {
    return !!this.getScheme(tableName);
  }

  static createTable<KeyType extends string | number, Type>({ name, fields, settings: rawSettings, tags }: TCreateTable<Type>): Table<KeyType, Type> {
    if (this.isTableExist(name)) {
      throw new Error(`Table '${name}' already exists`);
    }
    if (!rawSettings) rawSettings = {};
    if (!tags) tags = {} as Record<string, FieldTag[]>;

    const settings: TableSettings = Object.assign(DefaultTableSettings, rawSettings);



    mkdirSync(`/data/${name}/`);
    mkdirSync(`/data/${name}/indices/`);
    const mainDictDir = `/data/${name}/main/`;
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

    mkdirSync(`/data/${name}/heavy/`);




    for (const fieldName in fields) {
      const type = fields[fieldName];
      if (isHeavyType(type)) {
        mkdirSync(`/data/${name}/heavy/${fieldName}/`);
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

    return this.getTable<KeyType, Type>(name);
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