import { PlainObject } from "./globals";
import SortedDictionary from "./sorted_dictionary";

import vfs, { mkdirSync, existsSync, rmie } from "./virtual_fs";

function getEmptyKey<KeyType extends string | number>(keyType: KeyType extends string ? "string" : "int"): KeyType {
  return keyType == "string" ? "" : 0 as any;
}

export function getMetaFilepath(directory: string) {
  return `${directory}meta.json`;
}

export type FragmentedDictionarySettings<KeyType extends string | number> = {
  keyType: KeyType extends string ? "string" : "int"
  maxPartitionSize: number,
  maxPartitionLength: number,
  directory: string,
};


export type PartitionMeta<KeyType> = {
  length: number
  end: KeyType
  start: KeyType
}

export type FragDictMeta<KeyType extends string | number> = {
  length: number
  start: KeyType
  end: KeyType
  partitions: PartitionMeta<KeyType>[]
  custom: Record<string, any>
}

export type PartitionIterateMap<KeyType> = Map<number, KeyType[]>;

export type WhereRanges<KeyType> = [KeyType | undefined, KeyType | undefined][];

export type IterateRangesOptions<KeyType extends string | number, Type, ReturnType = Type> = {
  ranges?: WhereRanges<KeyType>
  filter?: (value: Type, id: KeyType) => boolean
  update?: (value: Type, id: KeyType) => Type | undefined
  select?: (value: Type, id: KeyType) => ReturnType
  limit?: number
  offset?: number
  invertRanges?: boolean
};

export type IDFilter<KeyType extends string | number> = (id: KeyType) => boolean;
export type PartitionFilter<KeyType extends string | number> = (start: KeyType, end: KeyType) => boolean;

export type IterateWhereOptions<KeyType extends string | number, Type, ReturnType = Type> = {
  partitionFilter?: PartitionFilter<KeyType>
  idFilter?: IDFilter<KeyType>
  filter?: (value: Type, id: KeyType) => boolean
  update?: (value: Type, id: KeyType) => Type | undefined
  select?: (value: Type, id: KeyType) => ReturnType
  limit?: number
  offset?: number
  invertRanges?: boolean
};

const DefaultFragmentDictionarySettings: FragmentedDictionarySettings<number> = {
  keyType: "int",
  maxPartitionLength: 10 * 1000,
  maxPartitionSize: 5 * Math.pow(1024, 3),
  directory: "",
};


export default class FragmentedDictionary<KeyType extends string | number = string | number, Type = any> {
  public readonly settings: FragmentedDictionarySettings<KeyType>;
  public meta: FragDictMeta<KeyType>;

  protected emptyKey: KeyType;
  protected _metaFilepath: string;

  static rename(oldDir: string, newDir: string) {
    // renameSync(oldDir, newDir);
    vfs.renameDir(oldDir, newDir);
  }

  static reset<KeyType extends string | number, Type>(dict: FragmentedDictionary<KeyType, Type>): FragmentedDictionary<KeyType, Type> {
    const settings = dict.settings;
    dict.destroy();
    return this.init(settings);
  }

  static init<KeyType extends string | number, Type>(settings: Partial<FragmentedDictionarySettings<KeyType>> = {}) {
    const opt: FragmentedDictionarySettings<KeyType> = Object.assign({}, DefaultFragmentDictionarySettings, settings);

    if (!settings.directory) throw new Error(`'directory' is a required parameter!`);
    if (existsSync(opt.directory)) throw new Error(`directory '${opt.directory}' already exists`);
    mkdirSync(opt.directory);


    const empty: FragDictMeta<KeyType> = {
      length: 0,
      start: getEmptyKey(opt.keyType),
      end: getEmptyKey(opt.keyType),
      partitions: [],
      custom: {},
    };

    vfs.writeFile(getMetaFilepath(opt.directory), empty);

    const optsToSave: Partial<FragmentedDictionarySettings<KeyType>> = Object.assign({}, opt);
    delete optsToSave.directory;
    vfs.writeFile(`${opt.directory}settings.json`, opt);
    return this.open<KeyType, Type>(opt.directory);
  }

  static open<KeyType extends string | number, Type>(directory: string) {
    const settings = vfs.readFile(`${directory}settings.json`);
    settings.directory = directory;
    return new FragmentedDictionary<KeyType, Type>(settings, vfs.readFile(getMetaFilepath(directory)));
  }

  constructor(settings: FragmentedDictionarySettings<KeyType>, meta: FragDictMeta<KeyType>) {
    this.settings = settings;
    if (!existsSync(settings.directory)) throw new Error(`directory '${settings.directory}' doesn't exists`);
    this.meta = meta;
    this.emptyKey = settings.keyType == "string" ? "" : 0 as any;

    this._metaFilepath = getMetaFilepath(this.settings.directory);

    this.meta.custom = new Proxy({
      ...this.meta.custom
    }, {
      set: (target, key: string, value) => {
        target[key] = value;
        vfs.writeFile(this._metaFilepath, this.meta);
        return true;
      },
      get: (target, key: string) => {
        return target[key];
      }
    });
  }

  destroy() {
    rmie(this.settings.directory);
  }

  public get length(): number {
    return this.meta.length;
  }

  static findDictEnd<KeyType extends string | number>(meta: FragDictMeta<KeyType>, emptyKey: KeyType) {
    const { partitions, length } = meta;
    if (!length || !partitions.length) return emptyKey;

    let index = partitions.length - 1;
    while (index >= 0) {
      if (partitions[index].end) {
        return partitions[index].end;
      }
      index--;
    }
    return emptyKey;
  }

  public get end(): KeyType {
    return FragmentedDictionary.findDictEnd(this.meta, this.emptyKey);
  }

  public get start(): KeyType {
    return this.meta.start;
  }

  public get numPartitions(): number {
    return this.meta.partitions.length;
  }

  findPartitionsForIds(ids: KeyType[]): PartitionIterateMap<KeyType> {
    const partitionsToOpen = new Map<number, KeyType[]>();
    for (const id of ids) {
      let partId = this.findPartitionForId(id);
      let pIds = partitionsToOpen.get(partId) || [];
      pIds.push(id);
      partitionsToOpen.set(partId, pIds);
    }
    return partitionsToOpen;
  }

  findDictMaxKey(dict: PlainObject): KeyType {
    let max = this.emptyKey;
    for (const idStr in dict) {
      if (idStr > max) {
        max = <KeyType>(this.settings.keyType == "string" ? idStr : <any>idStr * 1);
      }
    }
    return max;
  }

  findDictMinKey(dict: PlainObject): KeyType {
    let min: KeyType | undefined = undefined;
    for (const idStr in dict) {
      if (min === undefined || idStr < min) {
        min = <KeyType>(this.settings.keyType == "string" ? idStr : <any>idStr * 1);
      }
    }
    return min || this.emptyKey;
  }

  setOne(id: KeyType, value: Type) {
    if (!this.length) {
      return this.insertMany([id], [value]);
    }

    let partID = this.findPartitionForId(id);
    if (this.partitionExceedsSize(partID)) {
      partID = this.splitPartition(partID, id);
    }
    const partition = this.openPartition(partID);
    let metaIsDirty = partition.get(id) === undefined && value !== undefined;
    partition.set(id, value);

    this.savePartition(partID, partition);
    if (metaIsDirty) {
      const meta = this.meta.partitions[partID];
      meta.start = partition.firstKey || this.emptyKey;
      meta.end = partition.lastKey || this.emptyKey;
      meta.length = partition.length;
      this.saveMeta();
    }
  }

  getOne(id: KeyType): Type | undefined {
    const dict = this.iterateRanges({
      ranges: [[id, id]],
      limit: 1,
      select: val => val
    })[0];
    return dict[id];
  }

  /**
   * @deprecated 
   */
  editRanges(ranges: WhereRanges<KeyType>, predicate: (value: Type, id: KeyType) => Type | undefined, limit = 0) {
    this.iterateRanges({
      ranges,
      update: predicate,
      limit,
    });
  }

  static idsToRanges<KeyType extends string | number>(ids: KeyType[]): [KeyType, KeyType][] {
    return ids.map(id => [id, id]);
  }

  /**
   * @deprecated
   */
  filterSelect<NewType = Type>(ranges: WhereRanges<KeyType>, limit = 100, transform?: (value: Type, id: KeyType) => NewType | undefined) {
    const result = this.iterateRanges<NewType>({
      ranges,
      limit,
      filter: !transform ? undefined : (val: Type, id: KeyType) => transform(val, id) !== undefined,
      select: (transform ? transform : (val: Type) => val) as any,
    });
    return result[0];
  }

  /**
   * 
   * @param partId partition to split
   * @param key id split by
   * @returns ID of the partition with lesser length
   */
  splitPartition(partId: number, key: KeyType): number {
    const part1 = this.openPartition(partId);
    const part2 = part1.splitByKey(key);
    this.createNewPartition(partId);

    const meta1: PartitionMeta<KeyType> = this.meta.partitions[partId];
    const meta2: PartitionMeta<KeyType> = this.meta.partitions[partId + 1];
    meta1.length = part1.length;
    meta1.end = part1.lastKey || this.emptyKey;
    meta2.length = part2.length;
    meta2.end = part2.lastKey || this.emptyKey;

    this.savePartition(partId, part1);
    this.savePartition(partId + 1, part2);
    return meta1.length < meta2.length ? partId : partId + 1;
  }

  insertArray(arr: Type[]): KeyType[] {
    const { keyType } = this.settings;
    if (keyType == "string") throw new Error("'insertArray' is only supported on 'int' keyType dictionaries!");

    const lastID: number = <number>this.end;
    const dict = <SortedDictionary<KeyType, Type>>SortedDictionary.fromArray(arr, lastID + 1);
    const newKeys = dict.keys();
    this.insertSortedDict(dict);
    return newKeys;
  }

  saveMeta() {
    const { partitions } = this.meta;
    let _start: KeyType | undefined = undefined, _end: KeyType | undefined = undefined, _length = 0;
    for (const { start, end, length } of partitions) {
      if (length && (_start === undefined || start < _start)) {
        _start = start;
      }

      if (length && (_end === undefined || end > _end)) {
        _end = end;
      }
      _length += length;
    }
    this.meta.length = _length;
    this.meta.start = _start || this.emptyKey;
    this.meta.end = _end || this.emptyKey;

    vfs.writeFile(this._metaFilepath, this.meta);
  }

  insertSortedDict(dict: SortedDictionary<KeyType, Type>) {
    if (!dict.length) return;

    const { maxPartitionLength: maxPartitionLenght } = this.settings;
    const { partitions } = this.meta;
    const partitionsToOpen = this.findPartitionsForIds(dict.keys());


    Array.from(partitionsToOpen.keys()).sort().reverse().forEach(partId => {

      let tail: KeyType[] = partitionsToOpen.get(partId) || [];

      while (tail.length > 0) {
        let currentMeta = partitions[partId];
        if (!currentMeta) {
          currentMeta = this.createNewPartition(partId);
        }


        if (this.partitionExceedsSize(partId)) {
          if (tail[0] > currentMeta.end) {
            partId++;
            currentMeta = this.createNewPartition(partId);
          } else {
            partId = this.splitPartition(partId, tail[0]);
            currentMeta = partitions[partId];
          }
        }
        let capacity = maxPartitionLenght - currentMeta.length;

        let idsToInsert: KeyType[];
        if (tail.length > capacity) {
          idsToInsert = tail.slice(0, capacity);
          tail = tail.slice(capacity);
          // dict = keys.reduce(reducer, {});
        } else {
          idsToInsert = tail;
          tail = [];
        }

        const docs = this.openPartition(partId);
        for (const id of idsToInsert) {
          docs.set(id, <Type>dict.pop(id));
        }

        this.savePartition(partId, docs);
      }
    });


    this.saveMeta();
  }


  insertMany(dict: Record<string, Type>): void
  insertMany(keys: KeyType[], values: Type[]): void
  insertMany(keys: KeyType[], values: Type[], dict: Record<string, Type>): void
  insertMany(arg1: any, arg2?: any, arg3?: any): void {
    // const reducer = (d: Record<string, Type>, k: string | number) => { d[k] = dict[k]; return d; };
    const { keyType } = this.settings;


    let keys: KeyType[], values: Type[], dict: Record<string, Type>;
    if (arg1 instanceof Array) {
      keys = arg1.slice(0);
      values = arg2;
      dict = arg3 || keys.reduce((d: Record<string, Type>, k: KeyType, i: number) => {
        d[k as string] = values[i];
        return d;
      }, {});
    } else {
      dict = arg1;
      keys = Object.keys(dict) as any;
      if (keyType == "int") {
        keys = keys.map(i => (i as any) * 1) as any;
      }
      values = Object.values(dict);
    }

    if (!keys.length) return;

    this.insertSortedDict(new SortedDictionary(dict, keyType, false, keys));
  }

  edit(ids: KeyType[], predicate: (value: Type, id: KeyType) => Type | undefined) {
    this.editRanges(FragmentedDictionary.idsToRanges(ids), predicate, 0);
  }

  remove(...ids: KeyType[]) {
    this.iterateRanges({
      ranges: ids.map(id => [id, id]),
      update: () => undefined,
    });
  }

  static findPartitionForId<KeyType extends string | number>(id: KeyType, dictMeta: FragDictMeta<KeyType>): number {
    const { partitions } = dictMeta;
    if (!partitions.length) {
      return 0;
    }
    if (<number>id <= 0) {
      return 0;
    }

    // if (typeof id == "number") {
    //   const ratio = id / dictMeta.length;
    //   if (ratio <= 0) {
    //     return 0;
    //   }
    //   startPartitionID = ratio < 1 ? Math.floor(partitions.length * ratio) : partitions.length - 1;
    //   let newStartIndex = startIndex;
    //   while (startPartitionID > 0) {
    //     if (partitions[startPartitionID].length && newStartIndex <= id) {
    //       newStartIndex = <number>partitions[startPartitionID].start;
    //       startPartitionID--;
    //       break;
    //     }
    //   }

    //   if (newStartIndex != 0) {
    //     startIndex = newStartIndex;
    //   }
    // }

    let leftEnd: KeyType | undefined = undefined;
    let rightEnd: KeyType | undefined = undefined;
    function findRightEnd(index: number) {
      for (let i = index; i < partitions.length; i++) {
        if (partitions[i].start) return partitions[i].start;
      }
      return undefined;
    }

    for (let i = 0; i < partitions.length; i++) {
      const element = partitions[i];
      if (element.start <= id && id <= element.end) return i;

      if (element.length) {
        rightEnd = element.start;

        if ((leftEnd === undefined || id > leftEnd) && id < rightEnd) {
          return i;
        }

        leftEnd = element.end;
        if (i == 0 && id < element.start) return 0;
        if (i == partitions.length - 1 && id > element.end) return partitions.length - 1;

      } else {


        // empty partition in the middle or in the end
        rightEnd = findRightEnd(i + 1);
        if (!rightEnd) { // Further are only empty partitions
          return i;
        }

        if ((leftEnd === undefined || id > leftEnd) && id < rightEnd) {
          return i;
        }

      }
    }

    return -1;
    // throw new Error("can't find partition id");

    // return partitions.length - 1;
  }

  findPartitionForId(id: KeyType): number {
    return FragmentedDictionary.findPartitionForId<KeyType>(id, this.meta);
  }

  getPartitionDictFilename(id: number) {
    return `${this.settings.directory}part${id}_dict.json`;
  }

  getPartitionKeysFilename(id: number) {
    return `${this.settings.directory}part${id}_keys.json`;
  }

  partitionExceedsSize(id: number): boolean {
    const { maxPartitionLength: maxPartitionLenght, maxPartitionSize } = this.settings;
    // const { size, meta } = this.currentPartition;
    const length = this.meta.partitions[id].length;

    if (maxPartitionLenght && (length >= maxPartitionLenght)) {
      return true;
    }

    if (!maxPartitionSize) return false;
    const size = vfs.openFile(this.getPartitionDictFilename(id)).size();
    if (size > maxPartitionSize) {
      return true;
    }
    return false;
  }

  renamePartition(oldId: number, newId: number) {
    vfs.renameFile(this.getPartitionDictFilename(oldId), this.getPartitionDictFilename(newId));
    vfs.renameFile(this.getPartitionKeysFilename(oldId), this.getPartitionKeysFilename(newId));
  }

  savePartition(id: number, dict?: SortedDictionary<KeyType, Type>) {
    const meta = this.meta.partitions[id];
    if (!dict) {
      vfs.writeFile(this.getPartitionDictFilename(id), {});
      vfs.writeFile(this.getPartitionKeysFilename(id), []);
      meta.end = this.emptyKey;
      meta.start = this.emptyKey;
      meta.length = 0;
    } else {
      vfs.writeFile(this.getPartitionDictFilename(id), dict.raw());
      vfs.writeFile(this.getPartitionKeysFilename(id), dict.keys(true));
      meta.end = dict.lastKey || this.emptyKey;
      meta.start = dict.firstKey || this.emptyKey;
      meta.length = dict.length;
    }
  }

  createNewPartition(id: number): PartitionMeta<KeyType> {

    const meta: PartitionMeta<KeyType> = {
      length: 0,
      end: this.emptyKey,
      start: this.emptyKey,
    };

    const { partitions } = this.meta;
    if (partitions.length > 0)
      for (let i = partitions.length - 1; i >= id; i--) {
        this.renamePartition(i, i + 1);
      }
    this.meta.partitions.splice(id, 0, meta);

    this.savePartition(id);
    return meta;
  }

  readDictFile(index: number): Record<KeyType, Type> {
    return vfs.readFile(this.getPartitionDictFilename(index))
  }

  readKeysFile(index: number): KeyType[] {
    return vfs.readFile(this.getPartitionKeysFilename(index))
  }

  openPartition(index: number): SortedDictionary<KeyType, Type> {
    const meta: PartitionMeta<KeyType> | undefined = this.meta.partitions[index];
    if (!meta) {
      throw new Error(`partition '${index}' doesn't exists`);
    }

    return new SortedDictionary<KeyType, Type>(this.readDictFile(index), this.settings.keyType, true, this.readKeysFile(index));
  }


  loadAll(): SortedDictionary<KeyType, Type> {
    const result = new SortedDictionary<KeyType, Type>({}, this.settings.keyType);
    for (let i = 0; i < this.numPartitions; i++) {
      result.drain(this.openPartition(i));
    }
    return result;
  }

  keyAtIndex(index: number): KeyType | undefined {
    if (index < 0 || index >= this.length) return undefined;
    const { partitions } = this.meta;

    let totalLength = 0;
    let i: number;
    for (i = 0; i < partitions.length; i++) {
      const { length } = partitions[i];
      totalLength += length;
      if (length && totalLength > index) {
        totalLength -= length;
        break;
      }
    }

    let keys = this.readKeysFile(i);
    return keys[index - totalLength];
  }

  atindex(index: number): Type | undefined {
    const key = this.keyAtIndex(index);
    if (key === undefined) return undefined;
    return this.getOne(key);
  }

  static partitionMatchesRanges
    <KeyType extends string | number>(
      start: KeyType,
      end: KeyType,
      ranges: WhereRanges<KeyType>,
      invert = false
    ): boolean {

    for (const [min, max] of ranges) {
      if (max !== undefined && max < start) continue;
      if (min !== undefined && min > end) continue;
      return !invert;
    }
    return invert;
  }

  static idMatchesRanges
    <KeyType extends string | number>(
      id: KeyType,
      ranges: WhereRanges<KeyType>,
      invert = false
    ): boolean {
    for (const [min, max] of ranges) {
      if (min !== undefined && id < min) continue;
      if (max !== undefined && id > max) continue;
      return !invert;
    }
    return invert;
  }


  iterateRanges<ReturnType = Type>(options: IterateRangesOptions<KeyType, Type, ReturnType>): [Record<KeyType, ReturnType>, KeyType[]] {

    if (!this.length) {
      return [{} as any, []];
    }

    const {
      ranges,
      invertRanges,
    } = Object.assign({
      ranges: [[undefined, undefined]] as WhereRanges<KeyType>,
      invertRanges: false,
    }, options);

    return this.where(Object.assign({
      idFilter: (id: KeyType) => {
        return FragmentedDictionary.idMatchesRanges(id, ranges, invertRanges);
      },
      partitionFilter: (start: KeyType, end: KeyType) => {
        return invertRanges || FragmentedDictionary.partitionMatchesRanges(start, end, ranges);
      },
    }, options));
  }

  where<ReturnType = Type>(options: IterateWhereOptions<KeyType, Type, ReturnType>): [Record<KeyType, ReturnType>, KeyType[]] {
    if (!this.length) {
      return [{} as any, []];
    }

    const {
      partitionFilter,
      idFilter,
      filter,
      update,
      select,
      limit,
      offset, 
    } = Object.assign({

      limit: 0,
      offset: 0,
      invertRanges: false,
    }, options);

    const result: Record<KeyType, ReturnType> = {} as any;
    const removedIds: KeyType[] = [];

    const partitions = this.meta.partitions;
    let found = 0;
    let metaIsDirty = false;


    for (let i = 0; i < partitions.length; i++) {
      const partition = partitions[i];
      if (!partition.length) continue;
      if (partitionFilter && !partitionFilter(partition.start, partition.end)) continue;

      const keys: KeyType[] = this.readKeysFile(i);
      let values: Record<KeyType, Type> | undefined = undefined;
      let docs: SortedDictionary<KeyType, Type> | undefined = undefined;
      let isDirty = false;

      for (const id of keys) {
        if (idFilter && !idFilter(id)) continue;

        if (filter) {
          if (!values || !docs) {
            values = this.readDictFile(i);
            docs = new SortedDictionary(values, this.settings.keyType, true, keys.slice(0));
          }
          if (!filter(values[id], id)) continue;
        }

        found++;
        if (found <= offset) continue;

        if (!values || !docs) {
          values = this.readDictFile(i);
          docs = new SortedDictionary(values, this.settings.keyType, true, keys.slice(0));
        }
        const value = values[id];
        if (update) {
          const newValue = update(value, id);

          if (value !== newValue) {
            isDirty = true;
            if (newValue === undefined) {
              docs.pop(id);
              removedIds.push(id);
              metaIsDirty = true;
            } else {
              docs.set(id, newValue);
            }
          }
        }

        if (select) {
          result[id] = select(value, id);
        }

        if (limit && found - offset >= limit) {
          if (isDirty) {
            this.savePartition(i, docs);
          }
          if (metaIsDirty) this.saveMeta();
          return [result, removedIds];
        }

        if (isDirty) {
          isDirty = false;
          this.savePartition(i, docs);
        }
      }
    }

    if (metaIsDirty) this.saveMeta();
    return [result, removedIds];
  }

  hasAnyId(ids: KeyType[]): false | KeyType {
    const { partitions } = this.meta;
    for (let i = 0; i < partitions.length; i++) {
      const p = partitions[i];
      let openPartition = false;
      for (const id of ids) {
        if (!p.length || p.start > id || id > p.end) {
          continue;
        }
        openPartition = true;
        break;
      }
      if (openPartition) {
        const keys = this.readKeysFile(i);
        for (const id of keys) {
          if (ids.includes(id)) {
            return id;
          }
        }
      }
    }
    return false;
  }

  hasAllIds(ids: KeyType[]): boolean {
    const { partitions } = this.meta;

    const keysCache = new Map<number, KeyType[]>();
    for (const id of ids) {
      let hasValidPartition: number | false = false;
      for (let i = 0; i < partitions.length; i++) {
        const p = partitions[i];
        if (p.length && p.start <= id && id <= p.end) {
          hasValidPartition = i;
          break;
        }
      }
      if (hasValidPartition === false) return false;

      const keys = keysCache.get(hasValidPartition) || this.readKeysFile(hasValidPartition);
      keysCache.set(hasValidPartition, keys);
      if (!keys.includes(id)) {
        return false;
      }
    }

    return true;
  }

  keys(): KeyType[] {
    const { length } = this.meta.partitions;
    const result: KeyType[] = [];
    for (let i = 0; i < length; i++) {
      result.push(...this.readKeysFile(i));
    }
    return result;
  }
}