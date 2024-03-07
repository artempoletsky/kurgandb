
import { CallbackScope, Predicate, Promisify, QUERY_REGISTER_REQUIRED_MESSAGE, predicateToQuery, remoteQuery } from "./client";
import { AllTablesDict, DataBase, TCreateTable } from "./db";
import validate, { APIObject, APIRequest, APIValidationObject, ResponseError } from "@artempoletsky/easyrpc";
import { Table } from "./table";

import { allIsSaved } from "./virtual_fs";
import _ from "lodash";
import { PlainObject } from "./globals";
import z from "zod";
import { $, logError } from "./utils";
import { constructFunction } from "./function";




const zStringNonEmpty = z.string().min(1, "Required");

const ZQuery = z.object({
  queryId: zStringNonEmpty,
  payload: z.any(),
});

export type AQuery = z.infer<typeof ZQuery>;


type QueryImplementation = (tables: AllTablesDict, payload: PlainObject, scope: CallbackScope) => any;


export function queryToString(source: ARegisterQuery): string {
  if (!source) return "undefined";
  const async = source.isAsync ? "async " : "";
  return `${async} (${source.predicateArgs.join(", ")}) {
  ${source.predicateBody}
}`
}

export function getCurrentRequestDetails(): string {
  return queryToString(QueriesSourceCache[currentQueryHash]);
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
      db: DataBase,
      _,
      $,
    });
  } catch (err: any) {
    if (err.response && err.message && err.statusCode) {
      throw err;
    } else {
      logError(err.message,
        getCurrentRequestDetails() + "\r\n"
        + JSON.stringify(args.payload) + "\r\n"
        + err.stack + "\r\n"
      );
      throw new ResponseError({
        message: `Query has failed with error: ${err}`,
        statusCode: 500,
      });
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
  return $.md5(isAsync + "|" + predicateArgs.join(",") + "|" + predicateBody);
}

export async function registerQuery(args: ARegisterQuery) {

  const hash = generateQueryHash(args);
  try {
    QueriesCache[hash] = constructFunction({
      isAsync: args.isAsync,
      body: args.predicateBody,
      args: args.predicateArgs,
    }) as QueryImplementation;
  } catch (err: any) {
    logError("Query construction failed", err.message + "\r\n\r\n" + queryToString(args));
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