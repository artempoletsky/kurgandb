

export type ParsedFunction = {
  isAsync: boolean,
  args: string[];
  body: string
}

const HeadSplitExp = /^[^)=]+[)=]/; //finds head of the function
const BodyExtractExp = /^[^\{]*\{([\s\S]*)\}\s*$/;

export function parseFunction(fun: string): ParsedFunction
export function parseFunction(fun: Function): ParsedFunction
export function parseFunction(predicate: Function | string): ParsedFunction {
  if (typeof predicate == "function") predicate = predicate.toString();

  const cantParseMessage = "Can't parse function";

  const splitRes = HeadSplitExp.exec(predicate);
  if (!splitRes) throw new Error(cantParseMessage);

  let head = splitRes[0];
  if (head.endsWith("=")) {
    head = head.slice(0, head.length - 1);
  }

  const bodyUnprepared = predicate.slice(head.length, predicate.length).trim();
  const bodyExtractRes = BodyExtractExp.exec(bodyUnprepared);
  let body: string;
  if (!bodyExtractRes) {
    if (bodyUnprepared.startsWith("=>")) {
      body = "return " + bodyUnprepared.slice(2, bodyUnprepared.length).trim();
    } else {
      throw new Error(cantParseMessage);
    }
  } else {
    body = bodyExtractRes[1].trim();
  }

  const { args, isAsync } = parseFunctionHead(head.trim());

  return {
    isAsync,
    args,
    body,
  }
}


function parseFunctionHead(head: string): {
  args: string[];
  isAsync: boolean;
} {
  // const expRes = head.match(ArgumentsExp);
  // if (!expRes) throw new Error("can't parse arguments for predicate");

  // const args = expRes.map(s => s.slice(1, s.length - 1).trim());

  if (head.match(/^\w+$/)) return {
    isAsync: false,
    args: [head],
  };
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
      currentArg = currentArg.trim()
      if (currentArg)
        args.push(currentArg);
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

const AsyncFunction: FunctionConstructor = (async function AsyncFunction() { }).constructor as FunctionConstructor;

export function constructFunction(parsed: ParsedFunction): Function {

  const ctr = parsed.isAsync ? AsyncFunction : Function;
  const constructorArgs = [...parsed.args, parsed.body];
  let result: Function;

  try {
    result = ctr(...constructorArgs);
  } catch (err) {
    throw err;
  }

  return result;
}