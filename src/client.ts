
import { AQuery, FnQuery, queryUnsafe, POST } from "./api";
import { DataBase } from "./db";
import { $, PlainObject } from "./globals";
import { Table } from "./table";

import { getAPIMethod } from "@artempoletsky/easyrpc/client";
import lodash from "lodash";

// { } , { }
const ArgumentsExp = /^\{([^}]*)\}\s*\,\s*\{([^}]*)\}$/
function parseArguments(argStr: string): string[][] {
  let res = argStr.match(ArgumentsExp);
  if (!res) throw new Error("can't parse argumens for predicate");

  function prep(str: string): string[] {
    const trimmed = str.trim();
    if (!trimmed) return [];
    return trimmed.split(",").map(e => e.trim());
  }
  let result = [prep(res[1]), prep(res[2])];

  return result;
}
// (*) => {*}
const PredicateExp = /^\(([^)]*)\)\s*=>\s*\{([\s\S]*)\}\s*$/
function parsePredicate(predicate: string): {
  body: string,
  args: string[][]
} {
  const execRes = PredicateExp.exec(predicate);

  if (!execRes) throw new Error("can't parse predicate");

  const args = parseArguments(execRes[1].trim());
  const body = execRes[2].trim();

  return {
    args,
    body
  }
}

export function get(predicate: Function) {
  predicate.toString();
}

const BracesExp = /([^\s\{\},]+)/g
function parseBraceArgument(argStr: string): string[] | null {
  return argStr.match(BracesExp);
}

export type CallbackScope = {
  db: typeof DataBase
  $: typeof $
  _: typeof lodash
}

export type Predicate<Tables, Payload> = (tables: Tables, scope: CallbackScope & {
  payload: Payload
}) => any;


export function predicateToQuery<Tables, Payload>(predicate: Predicate<Tables, Payload>, payload: PlainObject): AQuery {
  const parsed = parsePredicate(predicate.toString());

  return {
    predicateBody: parsed.body,
    tables: parsed.args[0],
    payload,
  }
}


export async function remoteQuery
  <Tables extends Record<string, Table<any, any>>, Payload extends PlainObject>
  (predicate: Predicate<Tables, Payload>, payload: PlainObject = {}) {
  const address = process.env.KURGANDB_REMOTE_ADDRESS;
  if (!address) {
    throw new Error("There is no remote address specified to connect to!");
  }
  const remoteQuery: FnQuery = getAPIMethod(address, "query", {
    cache: "no-store"
  });

  return remoteQuery(predicateToQuery<Tables, Payload>(predicate, payload));
}

export const standAloneQuery: typeof remoteQuery = async (predicate, payload = {}) => {
  DataBase.init();
  const [response, status] = await POST({
    method: "query",
    args: predicateToQuery<any, any>(predicate, payload)
  });
  if (status == 200) return response;
  return Promise.reject(response);
}


export const queryUniversal: typeof remoteQuery = async (predicate, payload = {}) => {
  const address = process.env.KURGANDB_REMOTE_ADDRESS;
  if (address) {
    return remoteQuery(predicate, payload);
  }
  return standAloneQuery(predicate, payload);
}

