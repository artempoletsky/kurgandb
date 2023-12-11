
import { TQuery, queryUnsafe } from "./api";
import { DataBase } from "./db";
import { Table } from "./table";
import { PlainObject } from "./utils";


export function dbConnect() {

}
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


export type Predicate = (tables: Record<string, Table<any>>, scope: {
  payload: PlainObject
  db: typeof DataBase
  $: Record<string, Function>
}) => any;


export function predicateToQuery(predicate: Predicate, payload: PlainObject): TQuery {
  const parsed = parsePredicate(predicate.toString());

  return {
    predicateBody: parsed.body,
    tables: parsed.args[0],
    payload,
  }
}


