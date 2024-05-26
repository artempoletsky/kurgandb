KurganDB is a server database written in Typescript for Typescript.

## Key features

1. Queries here are just javascript functions that execute in a sandbox. You have full power of javascript in your queries. Furthemore, you have full power of NPN since you can install an NPM package to the database, write a plugin and use these plugin and package in your queries. 
2. Rich typescript support with code autocomplition. Your database records are typescript objects right of the box. No ORM needed everything is object here.
3. Can work both as a database server with HTTP API and as a library without network connection directly accessing file system. And you can switch in no time.
4. Javascript-friendly syntax. If you know how to filter a javascript array you already know how to make queries. 
5. Uses [zod](https://github.com/colinhacks/zod) for data validation.
6. You probably like [lodash](https://lodash.com/). We have it.
7. It has events. You can subscribe to creating, deleting and updating records.
8. Goes with [a highly customizeble admin panel](https://github.com/artempoletsky/install-kurgandb-admin).

## Queries

Get a record by id
```typescript
const user = await query(({ users }, { id }) => {
  // users is your table
  // id is a property from your payload object
  return users.at(id);
}, { id: 123 });// this is your payload object where you pass required data
// type of user is User
```

Select multiple records
```typescript
const comments = await query(({ comments }, { username }) => {
  return comments.filter(comment => comment.username == username).select(); // get all comments of this user
}, { username: "johndoe" });
// type of comments is Comment[]
```

You can modify example above using the `where` function.
```typescript
const comments = await query(({ comments }, { username }) => {
  return comments.where("username", username).select();
}, { username: "johndoe" });
// type of comments is Comment[]
```

Get all table records
```typescript
const comments = await query(({ comments }) => {
  return comments.all().select(); // by default 100 limit is set so you'll get first 100 records
});
// type of comments is Comment[]
```

Get all table records without limit
```typescript
const comments = await query(({ comments }) => {
  return comments.all().limit(0).select(); 
});
// type of comments is Comment[]
```

You can chain the `where` and the `filter` methods in any order. 
```typescript
const comments = await query(({ comments }) => {
  const maxDate = new Date("1995-12-17T03:24:00");
  return comments
    .where("username", "johndoe")
    .where("date", date => date < maxDate) // you can pass a predicate to where as well
    .filter(comment => {
      if (comment.meetsSomeCondition) {
        return true;
      }
      return false;
    }).select();
});
// type of comments is Comment[]
```

The `select` method works as the `Array.prototype.map` function. 
```typescript
const commentIds = await query(({ comments }) => {
  return comments.all().select(comment => comment.id); // select only ids 
});
// type of commentIds is number[]
```

The database has built in utility functions. You can modify example above with this:
```typescript
const commentIds = await query(({ comments }, {}, { $ }) => {
  // first argument of the query is the tables object that contains all your tables
  // second arguments is the payload object where you pass all needed data for the request
  // third argument is a global scope where 
  // we have KurganDB utility functions in $ object, lodash as _, zod as z, DataBase as db
  // and you can add your custom objects as plugins
  return comments.all().select($.primary);
});
// type of commentIds is number[]
```

Just like in SQL you can pick required fields. 
```typescript
const comments = await query(({ comments }, {}, { $ }) => {
  return comments.all().select($.pick("id", "text"));
});
// type of comments is { id: number; text: string; }[]
```

The `update` method works like `Array.prototype.forEach` method. 
```typescript
const updatedUser = await query(({ users }) => {
  users.where("username", "johndoe").update(user=> {
    user.fullName = "John Doe";
  });
  return users.where("username", "johndoe").select()[0];
});
// type of updatedUser is User
```

There is no joins and no need for multiple queries. 
```typescript
const data = await query(({ users, comments }, { username }, { $ }) => {
  return {
    user: users.where("username", username).select()[0],
    comments: comments.where("username", username).select(),
  } // whatever you return is a result of the awaited promise
}, { username: "johndoe" });
// type of data is { user: User; comments: Comment[]; }
```

Deleting records
```typescript
const removedIds = await query(({ comments }, { username }) => {
 return comments.where("username", username).delete();
}, { username: "johndoe" });
// type of removedIds is number[]
```

Inserting new records
```typescript
const newCommentId = await query(({ comments }, comment) => {
 return comments.insert(comment); // returns the primary key. 
 // We are using autoincrement option in this example so the primary key will be generated automatically.
}, { 
    username: "johndoe",
    text: "Hello world!",
    date: new Date(),
  }); // payload object is a comment
// type of newCommentId is number
```

## Sandbox 

Although quieries look like a javascript function they are in fact just text. They execute in a sandbox and have their own scope.

```typescript
let myCoolVariable = 1;
await query(() => {
 myCoolVariable // error myCoolVariable is not defined
 // we convert this function to text and send it over HTTP to the database and execute there
}); 
```

To fix this example you have to use the payload object:
```typescript
let myCoolVariable = 1;
await query(({}, { myCoolVariable }) => {
 myCoolVariable // is reachable now
}, { myCoolVariable }); 
```
If we use KurganDB as a library without using HTTP the behavior doesn't change.

## query function

KurganDB has 3 query functions. 
- `standAloneQuery` - the DB works as a library with your filesystem. And serves as both a client and a server.
- `remoteQuery` - the DB works as a client to the remote KurganDB server.
- `queryUniversal` - if it finds `KURGANDB_REMOTE_ADDRESS` in your env varables it works as `remoteQuery`, otherwise as `standAloneQuery`.

None of them knows anything about your database structure. You have to wrap it with your function and provide all needed types. 

This is done semi-automatically when using [KurganDB admin panel](https://github.com/artempoletsky/install-kurgandb-admin) code generation. 
Design your DB in the visual editor, then press the "Generate globals" on the scripts tab. 

```typescript
// db.ts

import { Predicate, queryUniversal } from "@artempoletsky/kurgandb";
import type { PlainObject, Table } from "@artempoletsky/kurgandb/globals";

// Empty for now. Will be explained further
type Plugins = {

}

// the type of your User record. 
// this goes to the globals.ts file and is kept here for demonstration purposes
export type User = {
  username: string;
  id: number;
  fullName: string;
}

// list all your tables here
export type Tables = {
  users: Table<
    User, // type of the record in your table
    number, // type of the primary key. In our case it's `id: number;`
  >;
}

// ReturnType is what you return in your query
// Payload is an argument you pass to your query
// Typescript will infer ReturnType and Payload automatically

// We are passing Table and Plugins types here for Typescript

// this method is generated with KurganDB admin
export async function query<Payload extends PlainObject, ReturnType>(predicate: Predicate<Tables, Payload, ReturnType, Plugins>, payload?: Payload) {
  return queryUniversal<Payload, ReturnType, Tables, Plugins>(predicate, payload);
}

// Predicate is (tables: Tables, payload: Payload, scope: GlobalScope & Plugins) => ReturnType
```
Compare these two:
```typescript

await queryUniversal(({ users }) => {
  // has no idea what users is. users is `any`
}); 

await query(({ users }) => {
  // users is an instance of Table<User, number>
}); 
```


## Method factory

Often we only need to pass data from the client to the database and vice versa. In this case we can use methodFactory function avoid writing exessive code.

```typescript
// API method without methodFactory
export async function getUserData(payload: { id: number }){
  return await query(({ users }, { id }) => {
    return users.at(id);
  }, payload);
}
// type of getUserData is (payload: { id: number }) => Promise<User>
```

It can be replaced with this:
```typescript
// the same API method with methodFactory
export const getUserData = methodFactory(({ users }, { id }: { id: number }) => {
  return users.at(id);
});
// type of getUserData is (payload: { id: number }) => Promise<User>
```

How do you get this methodFactory?
[KurganDB admin panel](https://github.com/artempoletsky/install-kurgandb-admin) will generate this method for you. Or you can copy this code. 
```typescript
// db.ts

// this method is generated with KurganDB admin
export function methodFactory<Payload extends PlainObject, PredicateReturnType, ReturnType = PredicateReturnType>(predicate: Predicate<Tables, Payload, PredicateReturnType, Plugins>, then?: (dbResult: PredicateReturnType) => ReturnType) {
  return async function (payload: Payload) {
    const dbResult = await query(predicate, payload);
    if (!then) return dbResult as ReturnType;
    return then(dbResult) as ReturnType;
  }
}
```

You might noticed that methodFactory takes an optional `then` argument. This is where you can modify the result from the DB before sending it to the front end.

```typescript
export const getUserData = methodFactory(({ users }, { id }: { id: number }) => {
  return users.at(id); // this is PredicateReturnType
}, (user) => { // your PredicateReturnType is User
  const someData = getSomeData();
  return {
    user,
    someData,
  }// this is your ReturnType
});
// type of getUserData is (payload: { id: number }) => Promise<{ user: User, someData: any }> 
```

## Errors handling

KurganDB is based on top of [EasyRPC](https://github.com/artempoletsky/easyrpc) library. 

- In EasyRPC API methods are just asynchronous javascript functions that take 1 Payload argument. 
- Errors are handled by throwing an exception. If the exception is an instance of ResponseError it's a 400 error, otherwise it's a 500 error. 400 errors go to the client, 500 errors stays at the back end.
- Response errors(400) are passed from the DB to the backend and to the client. 

Let's return to our previous example:
```typescript 
export const getUserData = methodFactory(({ users }, { id }: { id: number }) => {
  return users.at(id); // throws default 404 error if `id` is not found and sends it to the client
});
// type of getUserData is (payload: { id: number }) => Promise<User>
// getUserData is an EasyRPC method
```
You can customize your error messages and anything you're sending to the client as an error response by various ways. 
```typescript 
export const getUserData = methodFactory(({ users }, { id }: { id: number }, { $ }) => {
  if (!users.has(id)) throw $.notFound("User not found!"); // HTTP status is 404, Message is "User not found!"
  return users.at(id); // always found
});
```
```typescript 
// the same as above
export const getUserData = methodFactory(({ users }, { id }: { id: number }, { $ }) => {
  if (!users.has(id)) throw $.ResponseError.notFound("User not found!");
  return users.at(id);
});
```
```typescript 
export const premiumMethod = methodFactory(({ users }, { id }: { id: number }, { $ }) => {
  const user = users.at(id);
  if (!user.premium) throw new $.ResponseError({
    statusCode: 402,
    message: "Payment required",
  });
});
```
```typescript 
// the same as above
export const premiumMethod = methodFactory(({ users }, { id }: { id: number }, { $ }) => {
  const user = users.at(id);
  if (!user.premium) throw $.err({
    statusCode: 402,
    message: "Payment required",
  });
});
```

## Plugins

In order to avoid code repetition we usually want to wrap repeating logic into a function.
```typescript 
function myCoolFunction(){
  // do something
}
export const method1 = methodFactory(() => {
  myCoolFunction(); // error myCoolFunction is not defined
  // do something
});
export const method2 = methodFactory(() => {
  myCoolFunction(); // error myCoolFunction is not defined
  // do something else
});
```
Due to a sandbox nature of the queries, example above is erroneous. myCoolFunction is not defined in both methods.

This is where you need to create a plugin. Let's see an example from [KurganDB admin panel](https://github.com/artempoletsky/install-kurgandb-admin)
```typescript 
//plugins.ts

import { GlobalScope } from "@artempoletsky/kurgandb";

// myPlugin is the name of your plugin
export const myPlugin = {
  npm: ["is-odd"], // KurganDB admin will install npm packages listed here before activating the plugin
  install: function (scope: GlobalScope) { // install is a plugin factory function
    const isOdd = scope.$.require("is-odd"); // we can't use regular `require` here due to Next.js linting/trace errors. 
    return {// this object will be visible in your queries as myPlugin
      isOdd(number: number): boolean {
        return isOdd(number);
      },
      myMethod() {
        return scope.db.versionString;
      }
    }
  }
}

// this will be imported to your db.ts and used in your query function
export type Plugins = {
  myPlugin: ReturnType<typeof myPlugin.install>;
}
```

Now after activating `myPlugin` in the admin panel we can use `isOdd` method in our queries. 
```typescript 
//methods.ts

export const method1 = methodFactory(({}, {}, { myPlugin }) => {
  return myPlugin.isOdd(1);
});

export const method2 = methodFactory(({}, {}, { myPlugin }) => {
  return myPlugin.isOdd(2);
});
```
Important note. If you want to edit you plugin you must turn it off and on to apply changes. 

Just like every magic in KurganDB a plugin it's just a javascript function turned into text. And it's executed on the database initialization.

## Install as a library

```sh
npm install @artempoletsky/kurgandb
```

## Install as a server

```sh
git clone https://github.com/artempoletsky/kurgandb
cd kurgandb
npm install
npm run start
```

Or you can use Docker:
```sh
git clone https://github.com/artempoletsky/kurgandb
cd kurgandb
docker compose -f "compose.yaml" up -d --build
```


## env file settings
You can override global variables by creating an `.env` file in Node's current working directory.

```env
KURGANDB_SERVER_PORT = 8080
```
Specifies port on which the server will be listening. `8080` is default.

```env
KURGANDB_DATA_DIR = "D:/path/to/your/directory"
```
Specifies where the DB will store it's data. If not set it will be `process.cwd() + "/kurgandb_data"`.

```env
KURGANDB_REMOTE_ADDRESS = "http://127.0.0.1:8080"
```
Specifies address and port for the `remoteQuery` method. If not specified the method will throw an error.