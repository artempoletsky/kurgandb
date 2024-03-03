
import { ResponseError } from "@artempoletsky/easyrpc";
import { AQuery, FQuery, ARegisterQuery, FRegisterQuery, registerQuery, query } from "./api";
import { DataBase } from "./db";
import { $, PlainObject, md5 } from "./globals";
import { Table } from "./table";

import { getAPIMethod } from "@artempoletsky/easyrpc/client";
import lodash from "lodash";
import { logError } from "./utils";

// { } , { }
const ArgumentsExp = /\{([^\}]*)\}/g
function parseHead(head: string): {
  args: string[];
  isAsync: boolean;
} {
  // const expRes = head.match(ArgumentsExp);
  // if (!expRes) throw new Error("can't parse arguments for predicate");

  // const args = expRes.map(s => s.slice(1, s.length - 1).trim());
  // console.log(args);

  let argsStarted = false;
  let bracesStarted = false;
  const args: string[] = [];
  let currentArg = "";
  head = head.replace(/\s+/g, " ");
  for (let i = 0; i < head.length; i++) {
    const c = head[i];
    if (!argsStarted) {
      if (c == "(") argsStarted = true;
      continue;
    }

    if ((c == "," && !bracesStarted) || c == ")") {
      args.push(currentArg.trim());
      currentArg = "";
      if (c == ")") {
        break;
      }
      continue;
    }

    if (c == "{") bracesStarted = true;

    if (c == "}") bracesStarted = false;

    currentArg += c;
  }

  return {
    args,
    isAsync: head.startsWith("async"),
  };
}
// (*) => {*}
const PredicateExp = /^\(([^)]*)\)\s*=>\s*\{([\s\S]*)\}\s*$/;



function errorCantParse(predicate: string) {
  return logError("can't parse predicate", predicate);
}

const HeadSplitExp = /^[^\)]+\)/; //finds head of the function
const BodyExtractExp = /^[^\{]*\{([\s\S]*)\}\s*$/;
function parsePredicate(predicate: string): {
  isAsync: boolean;
  body: string;
  args: string[];
} {
  const splitRes = HeadSplitExp.exec(predicate);
  if (!splitRes) throw errorCantParse(predicate);

  const head = splitRes[0];

  const bodyUnprepared = predicate.slice(head.length, predicate.length).trim();
  const bodyExtractRes = BodyExtractExp.exec(bodyUnprepared);
  let body: string;
  if (!bodyExtractRes) {
    if (bodyUnprepared.startsWith("=>")) {
      body = "return "+ bodyUnprepared.slice(2, bodyUnprepared.length).trim();
    } else {
      throw errorCantParse(predicate)
    }
  } else {
    body = bodyExtractRes[1].trim();
  }




  const { args, isAsync } = parseHead(head);

  return {
    isAsync,
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
  ResponseError: typeof ResponseError
}

export type Predicate<Tables, Payload, ReturnType> = (tables: Tables, payload: Payload, scope: CallbackScope) => ReturnType;


export function predicateToQuery<Tables, Payload, ReturnType>(predicate: Predicate<Tables, Payload, ReturnType>): ARegisterQuery {
  const parsed = parsePredicate(predicate.toString());

  return {
    isAsync: parsed.isAsync,
    predicateBody: parsed.body,
    predicateArgs: parsed.args,
  }
}

export type Promisify<T> = Promise<T extends Promise<any> ? Awaited<T> : T>;


const QueriesHashes: Map<Predicate<any, any, any>, string> = new Map();

export const QUERY_REGISTER_REQUIRED_MESSAGE = `Query register required`;

async function registerOrCall(predicate: Predicate<any, any, any>, payload: any, registerQuery: FRegisterQuery, remoteQueryAPI: FQuery) {
  let queryId = QueriesHashes.get(predicate);
  if (!queryId) {
    queryId = await registerQuery(predicateToQuery(predicate));
    QueriesHashes.set(predicate, queryId);
  }

  let result;

  try {
    result = await remoteQueryAPI({
      queryId,
      payload,
    });
  } catch (err: any) {
    if (err.message == QUERY_REGISTER_REQUIRED_MESSAGE) {
      QueriesHashes.delete(predicate);
      return registerOrCall(predicate, payload, registerQuery, remoteQueryAPI);
    }
    throw err;
  }

  return result;
}

export async function remoteQuery
  <Tables extends Record<string, Table<any, any, any>>, Payload, ReturnType>
  (predicate: Predicate<Tables, Payload, ReturnType>, payload?: Payload)
  : Promisify<ReturnType> {
  if (!payload) payload = {} as Payload;

  const address = process.env.KURGANDB_REMOTE_ADDRESS;
  if (!address)
    throw logError("There is no remote address specified to connect to!", "");


  const remoteQueryAPI = getAPIMethod<FQuery>(address, "query", {
    // cache: "no-store"
  });

  const registerQuery = getAPIMethod<FRegisterQuery>(address, "registerQuery", {
    cache: "no-store"
  });

  return registerOrCall(predicate, payload, registerQuery, remoteQueryAPI);
}

export const standAloneQuery: typeof remoteQuery = async (predicate, payload) => {
  if (!payload) payload = {} as any;
  DataBase.init();
  return registerOrCall(predicate, payload, registerQuery, query);
}


export const queryUniversal: typeof remoteQuery = async (predicate, payload) => {
  const address = process.env.KURGANDB_REMOTE_ADDRESS;
  if (address) {
    return remoteQuery(predicate, payload);
  }
  return standAloneQuery(predicate, payload);
}

