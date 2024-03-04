#! /usr/bin/env node

import { POST } from "./api";
// import { clientQuery } from "./client";

import http from "http";
import { DataBase } from "./db";

DataBase.init();

const PORT = process.env.KURGANDB_SERVER_PORT || 8080


async function request(requestStr: string, method: string): Promise<[any, number]> {
  let requestObject: any;

  if (method == "POST") {
    try {
      requestObject = JSON.parse(requestStr);
    } catch (error) {
      return [{
        message: "Unable to parse request body",
      }, 400];
    }

    return await POST(requestObject);
  } else {
    return [DataBase.versionString, 200];
  }
}

http.createServer(function (req, res) {
  if (req.method == "OPTIONS") {
    res.writeHead(204, {
      // 'Date': (new Date()).toString(),
      'Server': "Mydb 0.0.1",
      'Access-Control-Allow-Origin': '*',
      "Access-Control-Allow-Credentials": "true",
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Request-Headers': 'X-PINGOTHER, Content-Type',
      'Access-Control-Allow-Headers': 'Access-Control-Allow-Headers, Origin, Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers',
      'Access-Control-Max-Age': '86400',
      'Vary': 'Accept-Encoding, Origin',
      'Keep-Alive': 'timeout=2, max=100',
      'Connection': 'Keep-Alive',
    });
    res.end();
    return;
  }

  let body: any[] = [];
  let bodystr: string;
  req
    .on('data', chunk => {
      body.push(chunk);
    })
    .on('end', async () => {
      bodystr = Buffer.concat(body).toString();

      const [postResult, status] = await request(bodystr, req.method || "GET");
      res.statusCode = status;
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.write(JSON.stringify(postResult));

      res.end();
    });



}).listen(PORT);

// const version = require('../package.json').version;

console.info(`${DataBase.versionString} is listening on '${PORT}'`);
console.info(`Working directory: ${DataBase.workingDirectory}`);
