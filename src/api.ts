import validate, { APIObject, APIRequest, APIValidationObject, ValidationRule, Validator, validateUnionFabric } from "./lib/rpc";
import { Table } from "./table";

import { PlainObject } from "./utils";

const Rules: APIValidationObject = {};
const API: APIObject = {};

export const QueryTypes = ["read", "write"] as const;
export type QueryType = typeof QueryTypes[number];
type TQuery = {
  type: QueryType,
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
  return true;
}

Rules.query = [{
  type: validateUnionFabric(QueryTypes),
  payload: "object",
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





export async function POST(req: PlainObject): Promise<[any, number]> {
  if (!req.args || !req.method) {
    return ["method or args are missing", 400];
  }

  const [response, status] = await validate(req as APIRequest, Rules, API);
  return [response, status.status];
}