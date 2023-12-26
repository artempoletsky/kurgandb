
import sortedIndex from "lodash.sortedindex";
import uniq from "lodash.uniq";

function sortPredicate<KeyType extends string | number>(a: KeyType, b: KeyType) { return a > b ? 1 : -1 };

export default class SortedDictionary<KeyType extends string | number, Type> {
  protected dict: Record<string | number, Type> = {};
  protected _keys: KeyType[];
  protected keyType: "string" | "int";

  constructor(dict: Record<string | number, Type> = {}, keyType: "string" | "int" = "string", sorted = true, keys?: KeyType[]) {

    this.keyType = keyType;
    this.dict = dict;
    if (keys) {
      this._keys = keys;
    }
    else {
      const strKeys = Object.keys(dict);
      this._keys = keyType == "string" ? strKeys : strKeys.map(i => i as any * 1) as any;
    }
    if (!sorted) {
      this._keys = this._keys.sort(sortPredicate);
    }
  }

  public get length(): number {
    return this._keys.length;
  }

  values() {
    return this._keys.map(k => this.dict[k]);
  }


  keyAtIndex(index: number) {
    return this._keys[index];
  }

  atIndex(index: number) {
    const key = this._keys[index];
    if (!key) return undefined;
    return this.get(key);
  }

  keys(doNotCopy = false) {
    return doNotCopy ? this._keys : [...this._keys];
  }

  public get firstKey(): KeyType | undefined {
    return this._keys[0];
  }

  public get lastKey(): KeyType | undefined {
    return this._keys.at(-1);
  }

  get(index: KeyType) {
    return this.dict[index];
  }

  set(index: KeyType, value: Type) {
    if (this.dict[index] === undefined) {
      const i = sortedIndex(this._keys, index);
      this._keys.splice(i, 0, index);
    }
    this.dict[index] = value;
  }

  static fromArray<Type>(array: Type[], startIndex = 1) {
    const keys: number[] = [];
    const dict = array.reduce((d, v, i) => {
      const key = startIndex + i;
      d[key] = v;
      keys.push(key);
      return d;
    }, {} as Record<string, Type>);
    return new SortedDictionary<number, Type>(dict, "int", true, keys);
  }

  /**
   * @param array ["key1", "key2", ..., "keyN"]
   * @returns //{"key1": "key1", "key2": "key2", ..., "keyN": "keyN"}
   */
  static fromKeys<KeyType extends string | number>(array: KeyType[]) {
    if (!array.length) throw new Error("array is empty");
    const keyType = typeof array[0] == "number" ? "int" : "string";
    const newKeys: KeyType[] = uniq(array);
    const dict = array.reduce((d, k) => {
      d[k] = k;
      return d;
    }, {} as Record<KeyType, KeyType>);
    return new SortedDictionary<KeyType, KeyType>(dict, keyType, false, newKeys);
  }

  /**
   * @param len 
   * @returns //{"1": 1, "2": 2, ..., "Len": Len}
   */
  static fromLenght(len: number) {
    return this.fromKeys<number>(Array.from(Array(len)).map((v, i) => i + 1));
  }

  /**
   * Removes a value at given index and returns the deleted value
   * @param key key to delete
   * @returns deleted value
   */
  pop(key?: KeyType) {
    let index;
    if (key === undefined) {
      index = 0;
      key = this._keys[0];
      if (!key) {
        return undefined;
      }
    }
    index = this._keys.indexOf(key);
    this._keys.splice(index, 1);
    const value = this.dict[key];
    delete this.dict[key];
    return value;
  }

  forEach(predicate: (val: Type, key: KeyType, index: number) => any) {
    this._keys.forEach((key, ind) => predicate(this.dict[key], key, ind));
  }

  splitByIndex(index: number): SortedDictionary<KeyType, Type> {
    const keys = this._keys.slice(index);
    const dict = keys.reduce((d, k) => {
      d[k] = this.dict[k];
      delete this.dict[k];
      return d;
    }, {} as Record<KeyType, Type>);
    this._keys = this._keys.slice(0, index);

    return new SortedDictionary<KeyType, Type>(dict, this.keyType, true, keys);
  }

  splitByKey(key: KeyType): SortedDictionary<KeyType, Type> {
    const index = sortedIndex(this._keys, key);
    return this.splitByIndex(index);
  }

  transform<NewType>(predicate: (value: Type, key: KeyType) => NewType): SortedDictionary<KeyType, NewType> {
    const newKeys = this.keys();
    const newDict = newKeys.reduce((d, k) => {
      d[k] = predicate(this.dict[k], k);
      return d;
    }, {} as Record<KeyType, NewType>);
    return new SortedDictionary<KeyType, NewType>(newDict, this.keyType, true, newKeys);
  }

  merge(dict: SortedDictionary<KeyType, Type>) {
    this._keys.push.apply(this._keys, dict._keys);
    this._keys = this._keys.sort(sortPredicate);
    // console.log(this._keys);
    Object.assign(this.dict, dict.dict);
  }

  drain(dict: SortedDictionary<KeyType, Type>) {
    this.merge(dict);
    dict._keys = [];
    dict.dict = {};
  }

  toJSON() {
    return this._keys.reduce((res, key) => {
      res[key] = this.dict[key];
      return res;
    }, {} as Record<KeyType, Type>);
  }

  raw() {
    return this.dict;
  }

  [Symbol.iterator](): Iterator<[Type, KeyType, number]> {
    let i = 0;
    const keys = this._keys;
    const dict = this.dict;
    return {
      next() {
        const key = keys[i];
        if (!key) return { value: undefined, done: true };

        const value = dict[key];
        return { value: [value, key, i++], done: false };
      },
    };
  }
}