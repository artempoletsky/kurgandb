import _, { set } from "lodash";
import { $ } from "./utils";



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
  }

  if (!setMethod || !getMethod) {
    throw new Error(`Unsupported length ${len}`);
  }
  return { setMethod, getMethod };
}

export function generateFunction(type: "set" | "get", entryLength: number | undefined, method: string, offset: number, field: string): Function {
  let mainbody: string;

  if (entryLength !== undefined) {
    if (type === "set") {
      mainbody = `b.${method}(v, i * ${entryLength} + ${offset});`;
    } else {
      mainbody = `return b.${method}(i * ${entryLength} + ${offset});`;
    }
  } else {
    if (type === "set") {
      mainbody = `b.${method}(v, ${offset});`;
    } else {
      mainbody = `return b.${method}(${offset});`;
    }
  }


  if (process.env.NODE_ENV === "production") {
    if (entryLength !== undefined) {
      if (type === "set")
        return new Function("b", "i", "v", mainbody);
      else
        return new Function("b", "i", mainbody);

    } else {
      if (type === "set")
        return new Function("b", "v", mainbody);
      else
        return new Function("b", mainbody);
    }

  } else {
    if (entryLength !== undefined) {
      if (type === "set") {
        return new Function("b", "i", "v", `
try { 
  ${mainbody} 
} catch (e) { 
 console.error('Error setting "${field}" at offset ${offset} with value, index', v, i, e); 
 throw e;   
}`);
      } else {
        return new Function("b", "i", `
try { 
  ${mainbody} 
} catch (e) { 
 console.error('Error getting "${field}" at offset ${offset} with index', i, e); 
 throw e;   
}`);
      }
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
}

export type TPage<T extends string> = Record<T,
  {
    set: (index: number, value: number) => void;
    get: (index: number) => number
  }> & {
    $setBuffer: (buffer: Buffer) => void;
    $getBuffer: () => Buffer;
    $capacityArray: number;
    $sizeEntry: number;
    $sizePage: number;
    $canShiftRight: (capacity: number, num?: number) => boolean;
    $shiftRight: (capacity: number, fromIndex: number, num?: number) => void;
    $shiftLeft: (capacity: number, fromIndex: number, num?: number) => void;
  };

export type THeader<T extends string> = Record<T, number> & {
  $setBuffer: (buffer: Buffer) => void;
  $getBuffer: () => Buffer;
};

export default class NamedByteBuffer {
  static createHeader<T extends string>(map: Map<T, number>) {
    let methods: Record<string, Function> = {};
    let offsets = createOffsetsConst(map);
    let length = calculateLength(map);

    for (const [key, len] of map) {
      let { setMethod, getMethod } = lenToMethods(len);
      methods[key + "Set"] = generateFunction("set", undefined, setMethod, offsets[key], key);
      methods[key + "Get"] = generateFunction("get", undefined, getMethod, offsets[key], key);
    }
    let b: Buffer = Buffer.alloc(length);

    const $ = {
      $setBuffer(buffer: Buffer) {
        b = buffer;
      },
      $getBuffer() {
        return b;
      },
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

  static createArrayOrPage<T extends string>(map: Map<T, number>, arrayLength?: number, pageSize?: number): TPage<T> {
    if (arrayLength && pageSize) {
      throw "NamedByteBuffer: Only one of arrayLength or pageSize can be specified";
    }
    if (!arrayLength && !pageSize) {
      throw "NamedByteBuffer: Either arrayLength or pageSize must be specified";
    }

    let offsets = createOffsetsConst(map);
    let entryLength = calculateLength(map);
    if (!arrayLength) {
      arrayLength = Math.floor(pageSize! / entryLength);
    }
    if (!pageSize) {
      pageSize = arrayLength * entryLength;
    }
    let length = entryLength * arrayLength;

    type R = Record<T, {
      set: (index: number, value: number) => void;
      get: (index: number) => number;
    }>;

    let b: Buffer = Buffer.alloc(pageSize);

    const $ = {
      $setBuffer(buffer: Buffer) {
        b = buffer;
      },
      $getBuffer() {
        return b;
      },
      $capacityArray: arrayLength,
      $sizeEntry: entryLength,
      $sizePage: pageSize,
      $canShiftRight(capacity: number, num: number = 1) {
        return capacity + num <= arrayLength;
      },
      $shiftRight(capacity: number, fromIndex: number, num: number = 1) {
        if (!this.$canShiftRight(capacity, num)) {
          throw "NamedByteBuffer: Index out of bounds";
        }
        let byteFrom = fromIndex * entryLength;
        let byteTo = (fromIndex + num) * entryLength;
        let toShift = (capacity - fromIndex) * entryLength;
        b.copy(b, byteTo, byteFrom, toShift + byteFrom);
      },
      $shiftLeft(capacity: number, fromIndex: number, num: number = 1) {
        let byteFrom = (fromIndex + num) * entryLength;
        let byteTo = fromIndex * entryLength;
        let toShift = (capacity - fromIndex) * entryLength;
        b.copy(b, byteTo, byteFrom, toShift + byteFrom);
      },
    } as const;

    let result = {} as R;
    for (const [key, len] of map) {
      let { setMethod, getMethod } = lenToMethods(len);
      let field = {} as any;
      let setter = generateFunction("set", entryLength, setMethod, offsets[key], key);
      let getter = generateFunction("get", entryLength, getMethod, offsets[key], key);
      field.set = ((setter) => (i: number, v: number) => setter(b, i, v))(setter);
      field.get = ((getter) => (i: number) => getter(b, i))(getter);
      result[key] = field;
    }

    return { ...result, ...$ } as any
  }

  static createArray<T extends string>(map: Map<T, number>, arrayLength: number): TPage<T> {
    return this.createArrayOrPage(map, arrayLength, undefined);
  }

  static createPage<T extends string>(map: Map<T, number>, pageSize: number = 0x2000): TPage<T> {
    return this.createArrayOrPage(map, undefined, pageSize);
  }
}