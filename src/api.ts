
import { Predicate, predicateToQuery } from "./client";
import { AllTablesDict, DataBase, TCreateTable } from "./db";
import { HeavyTypes, LightTypes } from "./document";
import validate, { APIObject, APIRequest, APIValidationObject, ValidationRule, Validator, validateUnionFabric } from "./lib/rpc";
import { Table } from "./table";

import { PlainObject, rfs, wfs, existsSync, unlinkSync, rmie } from "./utils";
import { allIsSaved } from "./virtual_fs";

const Rules: APIValidationObject = {};
const API: APIObject = {};

export type TQuery = {
  payload: PlainObject
  tables: string[]
  predicateBody: string
};


type QueryImplementation = (tables: AllTablesDict, scope: PlainObject) => any;
function constructQuery(args: TQuery): QueryImplementation {
  return new Function(`{ ${args.tables.join(', ')} }`, `{ payload, db, $ }`, args.predicateBody) as QueryImplementation;
}

const PrediateConstructor: Validator = async ({ payload, args }) => {
  try {
    payload.queryImplementation = constructQuery(args);
  } catch (error) {
    return `query construction has failed with error: ${error}`;
  }
  return true;
}

const AreTablesExist: Validator = async ({ payload, args }) => {
  let dict: AllTablesDict = {};
  for (const name of args.tables) {
    if (!DataBase.isTableExist(name)) {
      return `table ${name} doesn't exist`
    }
    dict[name] = DataBase.getTable(name);
  }
  payload.tablesDict = dict;
  return true;
}

const RunQuery: Validator = async ({ payload, args }) => {
  const { tablesDict, queryImplementation } = payload as TQueryPayload;
  try {
    payload.result = queryImplementation(tablesDict, {
      payload: args.payload
    });
  } catch (error) {
    return `query has failed with error: ${error}`;
  }

  await allIsSaved();
  return true;
}

Rules.query = [{
  payload: {},
  tables: ["string[]", AreTablesExist],
  predicateBody: "string",
}, PrediateConstructor, RunQuery] as ValidationRule<TQuery>;

type TQueryPayload = {
  queryImplementation: Function
  tablesDict: AllTablesDict
  result: any
}

async function query({ }: TQuery, { result }: TQueryPayload) {
  return result;
}

export async function queryUnsafe(args: TQuery) {
  const queryImplementation: QueryImplementation = constructQuery(args);
  const tables = args.tables.reduce((res, tableName) => {
    res[tableName] = DataBase.getTable(tableName);
    return res;
  }, {} as AllTablesDict);
  const queryResult = queryImplementation(tables, { payload: args.payload, db: DataBase });
  return queryResult;
}

export async function clientQueryUnsafe(p: Predicate, payload: PlainObject = {}) {
  return queryUnsafe(predicateToQuery(p, payload));
}

API.query = query;
////////////////////////////////////////////////////////////////////




const validateRecordFactory = function (possibleValues: any[]): Validator {
  return async ({ value }) => {
    if (typeof value != "object") return "object expected"
    for (const key in value) {
      let v = value[key];
      if (!possibleValues.includes(v)) {
        return `type of '${key}' is invalid; got: '${v}' expected ${possibleValues.join(', ')}`;
      }
    }
    return true;
  };
}

const checkTableNotExists: Validator = async ({ value }) => {
  if (DataBase.isTableExist(value)) return `table '${value}' already exists`;
  return true;
};

Rules.createTable = {
  name: ["string", checkTableNotExists],
  fields: validateRecordFactory([...LightTypes, ...HeavyTypes]),
  tags: "any",
} as ValidationRule<TCreateTable<PlainObject>>;

export async function createTable({ name, fields, tags }: TCreateTable<PlainObject>) {
  DataBase.createTable({
    name,
    fields,
    tags,
  });
  return {
    message: "OK"
  };
}

API.createTable = createTable;
/////////////////////////////////////////////////////////////////////////


type TRemoveTable = {
  name: string
};

const checkTableExists: Validator = async ({ value }) => {
  if (!DataBase.isTableExist(value)) return `table '${value}' doesn't exist`;
  return true;
};

Rules.removeTable = {
  name: ["string", checkTableExists],
} as ValidationRule<TRemoveTable>;

async function removeTable({ name }: TRemoveTable) {
  DataBase.removeTable(name);
  return {
    message: "OK"
  };
}

API.removeTable = removeTable;
/////////////////////////////////////////////////////////////////////////

export async function POST(req: PlainObject): Promise<[any, number]> {
  if (!req.args || !req.method) {
    return ["method or args are missing", 400];
  }

  const [response, status] = await validate(req as APIRequest, Rules, API);
  return [response, status.status];
}