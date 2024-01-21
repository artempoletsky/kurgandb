import { POST } from "./api";
// import { clientQuery } from "./client";


import http from "http";
import { DataBase } from "./db";

DataBase.init();

const PORT = process.env.KURGANDB_SERVER_PORT || 8080

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

      let requestObject = JSON.parse(bodystr);

      if (req.method == "POST") {
        let [postResult, status] = await POST(requestObject);
        res.statusCode = status;

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Access-Control-Allow-Origin', '*');
        // res.writeHead(200, {
        //   'Content-Type': 'application/json',
        //   'Access-Control-Allow-Origin': '*'
        // });
        res.write(JSON.stringify(postResult));
      }
      res.end();
      // res.on('error', err => {
      //   console.error(err);
      // });
    });



}).listen(PORT);

const version = require('../package.json').version;

console.info(`KurganDB v${version} are listening on '${PORT}'`);
console.info(`Working directory: ${DataBase.workingDirectory}`);
