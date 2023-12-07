import fs from "fs";
import { rimraf } from "rimraf";

export type PlainObject = Record<string, any>;

const CWD = process.cwd();

type WFSOptions = {
  pretty: boolean
} | undefined;

export function wfs(filename: string, data: any, options?: WFSOptions) {
  const { pretty = false } = options || {};

  if (pretty) {
    return fs.writeFileSync(CWD + filename, JSON.stringify(data, null, 2).replace(/\n/g, "\r\n"));
  } else {
    return fs.writeFileSync(CWD + filename, JSON.stringify(data));
  }
}

export function rfs(filename: string) {
  return JSON.parse(fs.readFileSync(CWD + filename, { encoding: "utf8" }));
}

export function existsSync(filename: string) {
  return fs.existsSync(CWD + filename);
}

export function unlinkSync(filename: string) {
  fs.unlinkSync(CWD + filename);
}

export function mkdirSync(dirname: string) {
  fs.mkdirSync(CWD + dirname)
}

/**
 * remove directory/file if exists
 * @param dirname 
 */
export function rmie(name: string) {
  if (existsSync(name)) {

    // unlinkSync(name);
    if (name.endsWith("/")) {
      // console.log(name);?

      rimraf.sync(name.slice(0, -1));
      fs.rmdirSync(CWD + name);
    } else {
      unlinkSync(name);
    }
  }
}


/**
 * make directory if not exists
 * @param dirname 
 */
export function mdne(dirname: string) {
  if (!existsSync(dirname)) {
    mkdirSync(dirname);
  }
}