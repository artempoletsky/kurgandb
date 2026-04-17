import _ from "lodash";
import { calculateLength, createOffsetsConst, lenToMethods } from "./Superblock";



export function generateFunction(type: "set" | "get", entryLength: number, method: string, offset: number, field: string): Function {
  let mainbody: string;

  if (method == "get16") {
    return new Function("b", "i", "v", `
  let s = i * ${entryLength} + ${offset};
  b.copy(v, 0, s, s + 16);`);
  } else if (method == "set16") {
    return new Function("b", "i", "v", `v.copy(b, i * ${entryLength} + ${offset}, 0, 16);`);
  }

  if (type === "set") {
    mainbody = `b.${method}(v, i * ${entryLength} + ${offset});`;
  } else {
    mainbody = `return b.${method}(i * ${entryLength} + ${offset});`;
  }


  if (process.env.NODE_ENV === "production") {
    if (type === "set")
      return new Function("b", "i", "v", mainbody);
    else
      return new Function("b", "i", mainbody);

  } else {

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
  }
}

export type TPageView<T extends string> = Record<T,
  {
    set: (index: number, value: number) => void;
    get: (index: number) => number
    set16: (index: number, value: Buffer) => void;
    get16: (index: number, value: Buffer) => void;
    binarySearchValue: (value: number, length: number) => { found: boolean, pos: number };
    binarySearchSortKey: (value: Buffer, length: number) => { found: boolean, pos: number };
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


export default class BytePageView {
  static create<T extends string>(map: Map<T, number>, tailReserved: number = 0, pageSize: number = 0x2000): TPageView<T> {

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
        if (b.byteLength != buffer.byteLength) {
          throw new Error("NamedByteBuffer: not implemented");
        }
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
      if (setMethod == "set16") {
        field.set16 = ((setter) => (i: number, v: number) => setter(b, i, v))(setter);
      } else {
        field.set = ((setter) => (i: number, v: number) => setter(b, i, v))(setter);
      }

      if (getMethod == "get16") {
        field.get16 = ((getter) => (i: number, v: Buffer) => getter(b, i, v))(getter);
      } else {
        field.get = ((getter) => (i: number) => getter(b, i))(getter);
      }

      field.binarySearchSortKey = ((field) => (value: Buffer, length: number) => binarySearchSortKey2(field, value, length))(field);
      field.binarySearchValue = ((field) => (value: number, length: number) => binarySearchValue2(field, value, length))(field);
      result[key] = field;

    }

    return { ...result, ...$ } as any
  }

  static createArray<T extends string>(map: Map<T, number>, arrayLength: number): TPageView<T> {
    return this.createArrayOrPage(map, arrayLength, undefined);
  }

  static createPage<T extends string>(map: Map<T, number>, pageSize: number = 0x2000): TPageView<T> {
    return this.createArrayOrPage(map, undefined, pageSize);
  }
}



export function binarySearchValue<T extends string>(
  page: TPageView<T>,
  length: number,
  value: number,
  key: T | "value" = "value") {
  if (!(key in page))
    throw new Error(`binarySearchSortKey: There is no ${key} property in the page`);

  return binarySearchValue2(page[key as T], value, length);
}

function binarySearchValue2(
  field: { get: (n: number) => number },
  value: number,
  length: number) {
  let lo = 0;
  let hi = length - 1;
  let mid = 0;


  while (lo <= hi) {
    mid = Math.floor((lo + hi) / 2);
    let v = field.get(mid);
    if (v === value) {
      return {
        found: true,
        pos: mid,
      };
    }
    if (v < value) {
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }

  return {
    found: false,
    pos: mid,
  };
}

export function binarySearchSortKey<T extends string>(
  page: TPageView<T>,
  value: Buffer,
  length: number,
  key: T | "sortKey" = "sortKey") {

  if (!(key in page))
    throw new Error(`binarySearchSortKey: There is no ${key} property in the page`);

  return binarySearchSortKey2(page[key as T], value, length);
}


function binarySearchSortKey2(
  field: { get16: (n: number, b: Buffer) => void },
  value: Buffer,
  length: number) {

  let lo = 0;
  let hi = length - 1;
  let mid = 0;

  let v = Buffer.allocUnsafe(16);
  while (lo <= hi) {
    mid = Math.floor((lo + hi) / 2);
    field.get16(mid, v);

    let cmp = v.compare(value);
    if (cmp === 0) {
      return {
        found: true,
        pos: mid,
      };
    }
    if (cmp < 0) {
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }

  return {
    found: false,
    pos: mid,
  };
}