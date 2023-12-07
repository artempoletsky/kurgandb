
import { Table } from "./table";
import { PlainObject } from "./utils";


export function dbConnect() {

}
const ArgumentsExp = /\s*(\{[^}]+\})|([^\{\},]+)\s*,?/g
function parseArguments(argStr: string): string[] | null {
  return argStr.match(ArgumentsExp);
}

const PredicateExp = /^([^\)]+)[^\{]*\{([\s\S]+)\}[^}]*$/
function parsePredicate(predicate: string): string[] | null {
  const execRes = PredicateExp.exec(predicate);
  if (!execRes) return null;
  const result = parseArguments(execRes[1].replace(/^[^(]*\(/, "").trim());
  if (!result) return null;
  result.push(execRes[2].trim());
  return result;
}

export function get(predicate: Function) {
  predicate.toString();
}

const BracesExp = /([^\s\{\},]+)/g
function parseBraceArgument(argStr: string): string[] | null {
  return argStr.match(BracesExp);
}


type Predicate = (tables: Record<string, Table>, scope: {
  payload: PlainObject
  userMethods: Record<string, Function>
  _: Record<string, Function>
}) => any;


export async function clientQuery(predicate: Predicate, payload: PlainObject) {
  const parsedPredicate = parsePredicate(predicate.toString());
  if (!parsedPredicate) throw new Error("Can't parse the predicate function");

  const tables = parseBraceArgument(parsedPredicate[0]);

  if (!tables) throw new Error("Can't parse the first argument of the predicate function");

  console.log(tables);

  // predicate.toString(), payload
  // let result = await query();
  // console.log(result);

  // parsePredicate(predicate.toString());
}