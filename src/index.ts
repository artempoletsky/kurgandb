
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

async function query(scope: Record<string, any>, predicate: string) {
  const parsedPredicate = parsePredicate(predicate);
  if (!parsedPredicate) return;
  // console.log(parsedPredicate);
  let queryImplementation;
  try {
    queryImplementation = new Function(...parsedPredicate);
  } catch (error) {
    return `query construction has failed with error: ${error}`;
  }

  const tables = {
    users: [],
    posts: []
  }
  let result = "";
  try {
    result = queryImplementation(scope, tables);
  } catch (error) {
    return `query has failed with error: ${error}`;
  }
  return result;
}

const fun = (a: string) => { a + 1 };

type PlainObject = Record<string, any>;
// interface Model extends PlainObject;
interface Table extends Array<PlainObject> {

}

// console.log(fun.toString());
type Predicate = (scope: {
  payload: PlainObject
  userMethods: Record<string, Function>
  _: Record<string, Function>
}, tables: Record<string, Table>) => any;

async function clientQuery(scope: PlainObject, predicate: Predicate) {
  // console.log(predicate.toString());
  let result = await query(scope, predicate.toString());
  console.log(result);

  // parsePredicate(predicate.toString());
}

clientQuery({
  name: "John"
}, ({ payload, userMethods, _ }, { users, posts }) => {
  const user: any = users.find(user => userMethods.md5(user.name) == payload.name);
  if (!user) return user;
  user.posts = posts.filter(post => post.user == user.id).map(_.exclude("body"));
  return user;
});

// clientQuery({}, (a: any, b: any) => { a + b });
// clientQuery({}, function ({ foo: far, bar }: any, b: any) { });

