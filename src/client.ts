
import {  FQuery, ARegisterQuery, FRegisterQuery, registerQuery, query } from "./api";
import { DataBase } from "./db";
import { $ } from "./utils";


import { getAPIMethod } from "@artempoletsky/easyrpc/client";
import lodash from "lodash";
import { logError } from "./utils";
import { parseFunction } from "./function";
import { Table } from "./globals";



function errorCantParse(predicate: string) {
  return logError("can't parse predicate", predicate);
}


export type CallbackScope = {
  db: typeof DataBase;
  $: typeof $;
  _: typeof lodash;
}

export type Predicate<Tables, Payload, ReturnType> = (tables: Tables, payload: Payload, scope: CallbackScope) => ReturnType;


export function predicateToQuery<Tables extends Record<string, Table>, Payload, ReturnType>(predicate: Predicate<Tables, Payload, ReturnType>): ARegisterQuery {
  const parsed = parseFunction(predicate);

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
  <Payload, ReturnType, Tables = any>
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

