import _, { set } from "lodash";
import { $ } from "./utils";
import VariableBuffer from "./VariableBuffer";

type BufferLike = Buffer | VariableBuffer;

export function createOffsetsConst<T extends string>(structure: Map<T, number>): Record<T, number> {
  let _currentOffset = 0;
  return Array.from(structure).reduce((result, e) => {
    result[e[0]] = _currentOffset;
    _currentOffset += e[1];
    return result;
  }, {} as Record<T, number>);
}

export function calculateLength(structure: Map<string, number>): number {
  return Array.from(structure).reduce((sum, e) => sum + e[1], 0);
}


export function lenToMethods(len: number, isFloat: boolean = false): { setMethod: string, getMethod: string } {
  let setMethod: string = "";
  let getMethod: string = "";

  if (isFloat) {
    throw "not implemented";
  }

  let b: Buffer;
  switch (len) {
    case 1:
      setMethod = "writeUint8";
      getMethod = "readUint8";
      break;
    case 2:
      setMethod = "writeUint16LE";
      getMethod = "readUint16LE";
      break;
    case 4:
      setMethod = "writeUint32LE";
      getMethod = "readUint32LE";
      break
    case 8:
      setMethod = "writeDoubleLE";
      getMethod = "readDoubleLE";
      break;
    case 16:
      setMethod = "set16";
      getMethod = "get16";
      break;
  }

  if (!setMethod || !getMethod) {
    throw new Error(`NamedByteBuffer; lenToMethods; Unsupported length ${len}`);
  }
  return { setMethod, getMethod };
}

export function generateFunctionSuperblock(type: "set" | "get", method: string, offset: number, field: string): Function {
  let mainbody: string;

  if (method == "get16") {
    return new Function("b", "v", `b.copy(v, 0, ${offset}, ${offset + 16})`);
  } else if (method == "set16") {
    return new Function("b", "v", `v.copy(b, ${offset}, 0, 16);`);
  }

  if (type === "set") {
    mainbody = `b.${method}(v, ${offset});`;
  } else {
    mainbody = `return b.${method}(${offset});`;
  }


  if (process.env.NODE_ENV === "production") {
    if (type === "set")
      return new Function("b", "v", mainbody);
    else
      return new Function("b", mainbody);
  } else {

    if (type === "set") {
      return new Function("b", "v", `
try { 
  ${mainbody} 
} catch (e) { 
 console.error('Error setting "${field}" at offset ${offset} with value', v, e); 
 throw e;   
}`);
    } else {
      return new Function("b", `
try { 
  ${mainbody} 
} catch (e) { 
 console.error('Error getting "${field}" at offset ${offset}', e); 
 throw e;   
}`);
    }
  }
}

export type TSuperblock<T extends string> = Record<T, number> & {
  $copyToEnd: (page: Buffer) => void;
  $copyToStart: (page: Buffer) => void;
  $setBuffer: (buffer: Buffer, readingPos?: number) => void;
  $getBuffer: () => Buffer;
  $size: number;
};

export default class Superblock {
  static fromPageTail<T extends string>(map: Map<T, number>, page: Buffer) {
    let result = this.create(map);
    let b = result.$getBuffer();
    page.copy(b, 0, page.byteLength - result.$size, page.byteLength);
    return result;
  }

  static create<T extends string>(map: Map<T, number>): TSuperblock<T> {
    let methods: Record<string, Function> = {};
    let offsets = createOffsetsConst(map);
    let length = calculateLength(map);

    for (const [key, len] of map) {
      let { setMethod, getMethod } = lenToMethods(len);
      methods[key + "Set"] = generateFunctionSuperblock("set", setMethod, offsets[key], key);
      methods[key + "Get"] = generateFunctionSuperblock("get", getMethod, offsets[key], key);
    }
    let b: Buffer = Buffer.alloc(length);

    const $ = {
      $setBuffer(buffer: Buffer) {
        b = buffer;
      },
      $getBuffer() {
        return b;
      },
      $size: length,
    } as const;

    return new Proxy({} as Record<T, number> & typeof $, {
      get(target, prop: T) {
        if (prop in $) {
          return $[prop as keyof typeof $];
        }
        return methods[prop + "Get"](b);
      },
      set(target, prop: T, value) {
        methods[prop + "Set"](b, value);
        return true;
      }
    });
  }
}
