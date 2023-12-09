
import { FieldType, HeavyType, HeavyTypes, LightTypes } from "./document";
import validate, { APIObject, APIRequest, APIValidationObject, ValidationRule, Validator, validateUnionFabric } from "./lib/rpc";
import { SCHEME_PATH, SchemeFile, Table, getBooleansFilepath, getFilepath, getFilesDir } from "./table";

import { PlainObject, rfs, wfs, existsSync, unlinkSync, mdne, rmie } from "./utils";

const Rules: APIValidationObject = {};
const API: APIObject = {};

type TQuery = {
  payload: PlainObject
  tables: string[]
  predicateBody: string
};

const PrediateConstructor: Validator = async ({ payload, args }) => {
  try {
    payload.queryImplementation = new Function(`{ ${args.tables.join(', ')} }`, `{ payload, _, $ }`, args.predicateBody);
  } catch (error) {
    return `query construction has failed with error: ${error}`;
  }
  return true;
}

const AreTablesExist: Validator = async ({ payload, args }) => {
  let dict: Record<string, Table> = {};
  for (const name of args.tables) {
    if (!Table.isTableExist(name)) {
      return `table ${name} doesn't exist`
    }
    dict[name] = new Table(name);
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

  for (const key in tablesDict) {
    const table = tablesDict[key];
    table.closePartition();
  }
  return true;
}

Rules.query = [{
  payload: {},
  tables: ["string[]", AreTablesExist],
  predicateBody: "string",
}, PrediateConstructor, RunQuery] as ValidationRule;

type TQueryPayload = {
  queryImplementation: Function
  tablesDict: Record<string, Table>
  result: any
}

async function query({ }: TQuery, { result }: TQueryPayload) {
  return result;
}

API.query = query;
////////////////////////////////////////////////////////////////////



type TCreateTable = {
  name: string
  fields: Record<string, FieldType>
};

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
  if (Table.isTableExist(value)) return `table '${value}' already exists`;
  return true;
};

Rules.createTable = {
  name: ["string", checkTableNotExists],
  fields: validateRecordFactory([...LightTypes, ...HeavyTypes])
} as ValidationRule;

async function createTable({ name, fields }: TCreateTable) {
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
  if (!Table.isTableExist(value)) return `table '${value}' doesn't exist`;
  return true;
};

Rules.removeTable = {
  name: ["string", checkTableExists],
} as ValidationRule;

async function removeTable({ name }: TRemoveTable) {
  const schemeFile: SchemeFile = rfs(SCHEME_PATH);

  delete schemeFile.tables[name];
  wfs(SCHEME_PATH, schemeFile, {
    pretty: true
  });

  rmie(getFilepath(name));
  rmie(getFilesDir(name));
  rmie(getBooleansFilepath(name));

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