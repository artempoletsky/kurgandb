#! /usr/bin/env node

import { rules, methods } from "./api";
// import { clientQuery } from "./client";

import http from "http";
import { DataBase } from "./db";
import { httpListener} from "@artempoletsky/easyrpc";

DataBase.init();

const PORT = process.env.KURGANDB_SERVER_PORT || 8080


http.createServer(httpListener({
  httpGetFallback: DataBase.versionString,
  rules,
  api: methods,
})).listen(PORT);

// const version = require('../package.json').version;

console.info(`${DataBase.versionString} is listening on '${PORT}'`);
console.info(`Working directory: ${DataBase.workingDirectory}`);
