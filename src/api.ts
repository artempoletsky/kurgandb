
import { CallbackScope, Predicate, Promisify, QUERY_REGISTER_REQUIRED_MESSAGE, predicateToQuery, remoteQuery } from "./client";
import { AllTablesDict, DataBase, TCreateTable } from "./db";
import validate, { APIObject, APIRequest, APIValidationObject, ResponseError } from "@artempoletsky/easyrpc";
import { Table } from "./table";

import { allIsSaved } from "./virtual_fs";
import _ from "lodash";
import { $, PlainObject, md5 } from "./globals";
import z from "zod";
import { logError } from "./utils";




const zStringNonEmpty = z.string().min(1, "Required");

const ZQuery = z.object({
  queryId: zStringNonEmpty,
  payload: z.any(),
});

export type AQuery = z.infer<typeof ZQuery>;

const AsyncFunction: FunctionConstructor = async function () { }.constructor as FunctionConstructor;

type QueryImplementation = (tables: AllTablesDict, payload: PlainObject, scope: CallbackScope) => any;



export function getCurrentRequestDetails(): string {
  const source = QueriesSourceCache[currentQueryHash];;
  const async = source.isAsync ? "async " : "";
  return `${async} (${source.predicateArgs.join(", ")}) {
  ${source.predicateBody}
}`
}

let currentQueryHash: string = "";

export async function query(args: AQuery) {

  currentQueryHash = args.queryId;
  let queryImplementation = QueriesCache[currentQueryHash];
  if (!queryImplementation) {
    // console.log(QUERY_REGISTER_REQUIRED_MESSAGE, args.queryId);

    throw new ResponseError(QUERY_REGISTER_REQUIRED_MESSAGE);
  }

  let result: any
  try {
    result = queryImplementation(DataBase.getTables(), args.payload, {
      ResponseError,
      db: DataBase,
      _,
      $
    });
  } catch (err: any) {
    if (err.response && err.message && err.statusCode) {
      throw err;
    } else {
      logError(err.message, getCurrentRequestDetails());
      throw new ResponseError(`Query has failed with error: ${err}`);
    }
  }

  return result;
}

export type FQuery = (arg: AQuery) => Promise<any>;



const ZRegisterQuery = z.object({
  isAsync: z.boolean(),
  predicateArgs: z.array(zStringNonEmpty),
  predicateBody: zStringNonEmpty,
});

export type ARegisterQuery = z.infer<typeof ZRegisterQuery>;

const QueriesCache: Record<string, QueryImplementation> = {};
const QueriesSourceCache: Record<string, ARegisterQuery> = {};


function generateQueryHash({ isAsync, predicateArgs, predicateBody }: ARegisterQuery) {
  return md5(isAsync + "|" + predicateArgs.join(",") + "|" + predicateBody);
}

export async function registerQuery(args: ARegisterQuery) {

  const hash = generateQueryHash(args);
  const constructorArgs = [...args.predicateArgs, args.predicateBody];
  const Construnctor = args.isAsync ? AsyncFunction : Function;
  try {
    QueriesCache[hash] = new Construnctor(...constructorArgs) as QueryImplementation;

  } catch (err: any) {
    throw new ResponseError("Query construction has failed: {...}", [err.message]);
  }

  QueriesSourceCache[hash] = args;
  return hash;
}

export type FRegisterQuery = typeof registerQuery;



export async function POST(req: APIRequest): Promise<[any, number]> {
  if (!req.args || !req.method) {
    return ["method or args are missing", 400];
  }

  const [response, status] = await validate(req, {
    query: ZQuery,
    registerQuery: ZRegisterQuery,
  }, {
    query,
    registerQuery
  });
  return [response, status.status];
}