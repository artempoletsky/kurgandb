
import sortedIndex from "lodash.sortedindex";

export default class SortedDictionary<KeyType extends string | number, Type> {
  protected dict: Record<string, Type> = {};
  protected _keys: KeyType[];
  protected keyType: "string" | "int";

  constructor(keyType: "string" | "int", dict: Record<string, Type> = {}, sorted = true, keys?: KeyType[]) {
    this.dict = dict;
    this.keyType = keyType;
    if (keys) {
      this._keys = keys;
    }
    else {
      const strKeys = Object.keys(dict);
      this._keys = keyType == "string" ? strKeys : strKeys.map(i => i as any * 1) as any;
      if (!sorted) {
        this._keys = this._keys.sort();
      }
    }

  }

  public get length(): number {
    return this._keys.length;
  }

  values() {
    return this._keys.map(k => this.dict[k as string]);
  }

  keys() {
    return [...this._keys];
  }

  public get firstKey(): KeyType | undefined {
    return this._keys[0];
  }

  public get lastKey(): KeyType | undefined {
    return this._keys.at(-1);
  }

  get(index: KeyType) {
    return this.dict[index as string];
  }

  set(index: KeyType, value: Type) {
    if (this.dict[index as string] === undefined) {
      const i = sortedIndex(this._keys, index);
      this._keys.splice(i, 0, index);
    }
    this.dict[index as string] = value;
  }

  static fromArray<Type>(array: Type[], startIndex = 1) {
    const keys: number[] = [];
    const dict = array.reduce((d, v, i) => {
      const key = startIndex + i;
      d[key] = v;
      keys.push(key);
      return d;
    }, {} as Record<string, Type>);
    return new SortedDictionary<number, Type>("int", dict, true, keys);
  }

  /**
   * @param array ["key1", "key2", ..., "keyN"]
   * @returns //{"key1": "key1", "key2": "key2", ..., "keyN": "keyN"}
   */
  static fromKeys<KeyType extends string | number>(array: KeyType[]) {
    if (!array.length) throw new Error("array is empty");
    const keyType = typeof array[0] == "number" ? "int" : "string";
    const newKeys: KeyType[] = [];
    const dict = array.reduce((d, k) => {
      if (d[k as string] === undefined) {
        d[k as string] = k;
        newKeys.push(k);
      }
      return d;
    }, {} as Record<string, KeyType>);
    return new SortedDictionary<KeyType, KeyType>(keyType, dict, false, newKeys);
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
   * @param index index to delete
   * @returns deleted value
   */
  pop(index: KeyType) {
    this._keys.splice(this._keys.indexOf(index), 1);
    const value = this.dict[index as string];
    delete this.dict[index as string];
    return value;
  }

  forEach(predicate: (val: Type, key: KeyType, index: number) => any) {
    this._keys.forEach((key, ind) => predicate(this.dict[key as string], key, ind));
  }

  splitByIndex(index: number): SortedDictionary<KeyType, Type> {
    const keys = this._keys.slice(index);
    const dict = keys.reduce((d, k) => {
      d[k as string] = this.dict[k as string];
      delete this.dict[k as string];
      return d;
    }, {} as Record<string, Type>);
    this._keys = this._keys.slice(0, index);

    return new SortedDictionary<KeyType, Type>(this.keyType, dict, true, keys);
  }

  splitByKey(key: KeyType): SortedDictionary<KeyType, Type> {
    const index = sortedIndex(this._keys, key);
    return this.splitByIndex(index);
  }

  transform<NewType>(predicate: (key: KeyType, value: Type) => NewType): SortedDictionary<KeyType, NewType> {
    const newKeys = this.keys();
    const newDict = newKeys.reduce((d, k) => {
      d[k as string] = predicate(k, this.dict[k as string]);
      return d;
    }, {} as Record<string, NewType>);
    return new SortedDictionary<KeyType, NewType>(this.keyType, newDict, true, newKeys);
  }

  toJSON() {
    return this._keys.reduce((res, key) => {
      res[key as string] = this.dict[key as string];
      return res;
    }, {} as Record<string, Type>);
  }
}