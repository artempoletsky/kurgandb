
import SortedDictionary from "./sorted_dictionary";
import { PlainObject, existsSync, mkdirSync, renameSync, rfs, rmie, statSync, wfs } from "./utils";


function getEmptyKey<KeyType extends string | number>(keyType: KeyType extends string ? "string" : "int"): KeyType {
  return keyType == "string" ? "" : 0 as any;
}

export function getMetaFilepath(directory: string) {
  return `${directory}meta.json`;
}

export type FragmentedDictionarySettings<KeyType extends string | number> = {
  keyType: KeyType extends string ? "string" : "int"
  maxPartitionSize: number,
  maxPartitionLenght: number,
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
}

export type PartitionIterateMap<KeyType> = Map<number, KeyType[]>;

export type WhereRanges<KeyType> = [KeyType | undefined, KeyType | undefined][];

const DefaultFragmentDictionarySettings: FragmentedDictionarySettings<number> = {
  keyType: "int",
  maxPartitionLenght: 10 * 1000,
  maxPartitionSize: 5 * Math.pow(1024, 3),
  directory: "",
};


export default class FragmentedDictionary<KeyType extends string | number, Type> {
  public readonly settings: FragmentedDictionarySettings<KeyType>;
  public meta: FragDictMeta<KeyType>;

  protected emptyKey: KeyType;

  static rename(oldDir: string, newDir: string) {
    renameSync(oldDir, newDir);
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
      partitions: []
    };

    wfs(getMetaFilepath(opt.directory), empty);
    const optsToSave: Partial<FragmentedDictionarySettings<KeyType>> = Object.assign({}, opt);
    delete optsToSave.directory;
    wfs(`${opt.directory}settings.json`, opt);
    return this.open<KeyType, Type>(opt.directory);
  }

  static open<KeyType extends string | number, Type>(directory: string) {
    const settings = rfs(`${directory}settings.json`);
    settings.directory = directory;
    return new FragmentedDictionary<KeyType, Type>(settings, rfs(getMetaFilepath(directory)));
  }

  constructor(settings: FragmentedDictionarySettings<KeyType>, meta: FragDictMeta<KeyType>) {
    this.settings = settings;
    if (!existsSync(settings.directory)) throw new Error(`directory '${settings.directory}' doesn't exists`);
    this.meta = meta;
    this.emptyKey = settings.keyType == "string" ? "" : 0 as any;
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


  static findDictStart<KeyType extends string | number>(meta: FragDictMeta<KeyType>, emptyKey: KeyType) {
    const { partitions, length } = meta;
    if (!length || !partitions.length) return emptyKey;

    for (let i = 0; i < partitions.length; i++) {
      const { start } = partitions[i];
      if (start) return start;
    }

    return emptyKey;
  }

  public get start(): KeyType {
    return FragmentedDictionary.findDictStart(this.meta, this.emptyKey);
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

  editRange(predicate: (id: KeyType, value: Type) => Type | undefined, min?: KeyType, max?: KeyType, limit = 100) {
    if (!this.length) {
      return;
    }
    if (min === undefined) {
      min = this.start;
    }
    if (max === undefined) {
      max = this.end;
    }

    let found = 0;
    let startPart = this.findPartitionForId(min);
    let endPart = this.findPartitionForId(max);


    const { keyType } = this.settings;
    let metaIsDirty = false;
    for (let i = startPart; i <= endPart; i++) {
      const docs = this.openPartition(i);

      let isDirty = false;
      const meta: PartitionMeta<KeyType> = this.meta.partitions[i];
      for (const idStr in docs) {
        const id = (keyType == "int" ? idStr as any * 1 : idStr) as KeyType;

        if (min <= id && id <= max) {


          const value = docs[idStr];
          const newValue = predicate(id, value);
          if (!isDirty) {
            if (value !== newValue) isDirty = true;
          }


          if (newValue === undefined) {

            delete docs[idStr];
            meta.length--;
            this.meta.length--;
            if (idStr == meta.end) {
              meta.end = this.findDictMaxKey(docs);
            }

            if (idStr == meta.start) {
              meta.start = this.findDictMaxKey(docs);
            }

            metaIsDirty = true;
          } else {
            docs[idStr] = newValue;
          }
          found++;
        }

        if (limit && found >= limit) break;
      }
      if (isDirty) {
        isDirty = false;
        wfs(this.getPartitionFilename(i), docs);
      }

      if (limit && found >= limit) break;
    }
    if (metaIsDirty) {
      this.saveMeta();
    }
  }

  removeRanges(ranges: WhereRanges<KeyType>, predicate?: (id: KeyType, value: Type) => boolean, limit = 0): KeyType[] {
    const result: KeyType[] = [];
    this.editRanges(ranges, (id, val) => {
      if (!predicate || predicate(id, val)) {
        result.push(id);
        return undefined;
      }
      return val;
    }, limit);
    return result;
  }

  setOne(id: KeyType, value: Type) {
    if (!this.length) {
      return this.insertMany([id], [value]);
    }

    let partID = this.findPartitionForId(id);
    if (this.partitionExceedsSize(partID)) {
      partID = this.splitPartition(partID, id);
    }
    const partition = this.openAsSortedDictionary(partID);
    let metaIsDirty = partition.get(id) === undefined && value !== undefined;
    partition.set(id, value);

    wfs(this.getPartitionFilename(partID), partition);
    if (metaIsDirty) {
      const meta = this.meta.partitions[partID];
      meta.start = partition.firstKey || this.emptyKey;
      meta.end = partition.lastKey || this.emptyKey;
      meta.length = partition.length;
      this.saveMeta();
    }
  }

  getOne(id: KeyType): Type | undefined {
    const dict = this.filterSelect([[id, id]]);
    return dict[id];
  }

  editRanges(ranges: WhereRanges<KeyType>, predicate: (id: KeyType, value: Type) => Type | undefined, limit = 0) {
    for (const [min, max] of ranges) {
      this.editRange(predicate, min, max, limit);
    }
  }

  static idsToRanges<KeyType extends string | number>(ids: KeyType[]): [KeyType, KeyType][] {
    return ids.map(id => [id, id]);
  }

  filterSelect<NewType = Type>(ranges: WhereRanges<KeyType>, limit = 100, transform?: (id: KeyType, value: Type) => NewType | undefined) {
    const result: Record<string | number, NewType | Type> = {};
    this.editRanges(ranges, (id: KeyType, value: Type) => {
      if (!transform) {
        result[id] = value;
      } else {
        const newValue = transform(id, value);
        if (newValue !== undefined) {
          result[id] = newValue;
        }
      }
      return value;
    }, limit);
    return result;
  }

  /**
   * 
   * @param partId partition to split
   * @param key id split by
   * @returns ID of the partition with lesser length
   */
  splitPartition(partId: number, key: KeyType): number {
    const part1 = this.openAsSortedDictionary(partId);
    const part2 = part1.splitByKey(key);
    this.createNewPartition(partId);

    const meta1: PartitionMeta<KeyType> = this.meta.partitions[partId];
    const meta2: PartitionMeta<KeyType> = this.meta.partitions[partId + 1];
    meta1.length = part1.length;
    meta1.end = part1.lastKey || this.emptyKey;
    meta2.length = part2.length;
    meta2.end = part2.lastKey || this.emptyKey;

    wfs(this.getPartitionFilename(partId), part1);
    wfs(this.getPartitionFilename(partId + 1), part2);
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
    let _start: KeyType | undefined = undefined, _end = this.emptyKey, _length = 0;
    for (const { start, end, length } of partitions) {
      if (start && (_start === undefined || start < _start)) {
        _start = start;
      }

      if (end && (end > _end)) {
        _end = end;
      }
      _length += length;
    }
    this.meta.length = _length;
    this.meta.start = _start || this.emptyKey;
    this.meta.end = _end;

    wfs(getMetaFilepath(this.settings.directory), this.meta);
  }

  insertSortedDict(dict: SortedDictionary<KeyType, Type>) {
    const firstKey = dict.firstKey;
    if (!firstKey) { //empty dict check
      return;
    }

    const { maxPartitionLenght, maxPartitionSize } = this.settings;
    const { partitions } = this.meta;
    const partitionsToOpen = this.findPartitionsForIds(dict.keys());


    Array.from(partitionsToOpen.keys()).sort().reverse().forEach(partId => {

      let tail: KeyType[] = partitionsToOpen.get(partId) || [];

      while (tail.length > 0) {
        let currentMeta = partitions[partId];
        if (!currentMeta) {
          currentMeta = this.createNewPartition(partId);
        }

        let capacity = maxPartitionLenght - currentMeta.length;
        let partitionExceedsSize = false;
        if (capacity <= 0) {
          partitionExceedsSize = true;
        } else if (maxPartitionSize) {
          const size = statSync(this.getPartitionFilename(partId)).size;
          if (size >= maxPartitionSize)
            partitionExceedsSize = true;
        }

        if (partitionExceedsSize) {
          if (tail[0] > currentMeta.end) {
            partId++;
            currentMeta = this.createNewPartition(partId);
          } else {
            partId = this.splitPartition(partId, tail[0]);
            currentMeta = partitions[partId];
          }
        }

        let idsToInsert: KeyType[];
        if (tail.length > capacity) {
          idsToInsert = tail.slice(0, capacity);
          tail = tail.slice(capacity);
          // dict = keys.reduce(reducer, {});
        } else {
          idsToInsert = tail;
          tail = [];
        }

        const docs = this.openAsSortedDictionary(partId);
        for (const id of idsToInsert) {
          docs.set(id, <Type>dict.pop(id));
        }


        currentMeta.start = docs.firstKey || this.emptyKey;
        currentMeta.end = docs.lastKey || this.emptyKey;
        currentMeta.length = docs.length;
        // console.log(docs.toJSON());

        wfs(this.getPartitionFilename(partId), docs);
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
      keys = arg1;
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

  edit(ids: KeyType[], predicate: (id: KeyType, value: Type) => Type | undefined) {
    this.editRanges(FragmentedDictionary.idsToRanges(ids), predicate, 0);
  }

  remove(ids: KeyType[]) {
    this.edit(ids, () => undefined);
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

  getPartitionFilename(id: number) {
    return `${this.settings.directory}part${id}.json`;
  }

  partitionExceedsSize(id: number): boolean {
    const { maxPartitionLenght, maxPartitionSize } = this.settings;
    // const { size, meta } = this.currentPartition;
    const length = this.meta.partitions[id].length;

    if (maxPartitionLenght && (length > maxPartitionLenght)) {
      return true;
    }

    if (!maxPartitionSize) return false;
    const size = statSync(this.getPartitionFilename(id)).size;
    if (size > maxPartitionSize) {
      return true;
    }
    return false;
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
        renameSync(this.getPartitionFilename(i), this.getPartitionFilename(i + 1));
      }
    // const id = this.meta.partitions.length;
    this.meta.partitions.splice(id, 0, meta);
    const fileName = this.getPartitionFilename(id);
    wfs(fileName, {});
    return meta;
  }

  openPartition(index: number): Record<string, Type> {
    const meta: PartitionMeta<KeyType> | undefined = this.meta.partitions[index];
    if (!meta) {
      throw new Error(`partition '${index}' doesn't exists`);
    }

    return rfs(this.getPartitionFilename(index));
  }

  openAsSortedDictionary(index: number) {
    return new SortedDictionary<KeyType, Type>(this.openPartition(index), this.settings.keyType);
  }

  loadAll(): SortedDictionary<KeyType, Type> {
    const result = new SortedDictionary<KeyType, Type>({}, this.settings.keyType);
    for (let i = 0; i < this.numPartitions; i++) {
      result.drain(this.openAsSortedDictionary(i));
    }
    return result;
  }
}