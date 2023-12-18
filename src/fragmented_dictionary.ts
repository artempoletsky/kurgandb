
import SortedDictionary from "./sorted_dictionary";
import { existsSync, mkdirSync, renameSync, rfs, rmie, statSync, wfs } from "./utils";


function getEmptyKey<KeyType extends string | number>(keyType: KeyType extends string ? "string" : "int"): KeyType {
  return keyType == "string" ? "" : 0 as any;
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
}

export type FragDictMeta<KeyType extends string | number> = {
  length: number
  start: KeyType
  partitions: PartitionMeta<KeyType>[]
}

export type PartitionIterateMap<KeyType> = Map<number, KeyType[]>;


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

    const metaFilename = `${opt.directory}meta.json`;
    const empty: FragDictMeta<KeyType> = {
      length: 0,
      start: opt.keyType == "string" ? "" : 0 as any,
      partitions: []
    };

    wfs(metaFilename, empty);
    wfs(`${opt.directory}settings.json`, opt);
    return this.open<KeyType, Type>(opt.directory);
  }

  static open<KeyType extends string | number, Type>(directory: string) {
    const metaFilename = `${directory}meta.json`;
    const settingsFilename = `${directory}settings.json`;
    return new FragmentedDictionary<KeyType, Type>(rfs(settingsFilename), rfs(metaFilename));
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

  public get lenght(): number {
    return this.meta.length;
  }

  public get end(): KeyType {

    const { partitions, length } = this.meta;
    if (!length || !partitions.length) return this.emptyKey;

    let index = partitions.length - 1;
    while (index >= 0) {
      if (partitions[index].end) {
        return partitions[index].end;
      }
      index--;
    }
    return this.emptyKey;

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

  editRange(predicate: (id: KeyType, value: Type) => Type | undefined, min?: KeyType, max?: KeyType, limit = 100) {
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
    for (let i = startPart; i <= endPart; i++) {
      const docs = this.openPartition(i);
      let isDirty = false;
      for (const idStr in docs) {
        const id = (keyType == "int" ? idStr as any * 1 : idStr) as KeyType;

        if (min <= id && id <= max) {


          const value = docs[idStr];
          const newValue = predicate(id, value);
          if (value !== newValue) isDirty = true;

          if (newValue === undefined) {
            delete docs[idStr];
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
  }

  removeRange(min?: KeyType, max?: KeyType, limit = 100): KeyType[] {
    const result: KeyType[] = [];
    this.editRange((id, val) => {
      result.push(id);
      return undefined;
    }, min, max, limit);
    return result;
  }

  getRange(min?: KeyType, max?: KeyType, limit = 100): Record<string, Type> {
    const result: Record<string, Type> = {};
    this.editRange((id, val) => {
      result[id as string] = val;
      return val;
    }, min, max, limit);

    return result;
  }

  getOne(id: KeyType): Type | undefined {
    const docs = this.openPartition(this.findPartitionForId(id));
    return docs[id as string];
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

  insertArray(arr: Type[]) {
    const { keyType } = this.settings;
    if (keyType == "string") throw new Error("'insertArray' is only supported on 'int' keyType dictionaries!");

    const lastID: number = <number>this.end;
    const dict = <SortedDictionary<KeyType, Type>>SortedDictionary.fromArray(arr, lastID + 1);

    this.insertSortedDict(dict);
    return dict.keys();
  }

  insertSortedDict(dict: SortedDictionary<KeyType, Type>) {
    const firstKey = dict.firstKey;
    if (!firstKey) { //empty dict check
      return;
    }

    const { maxPartitionLenght, maxPartitionSize } = this.settings;
    const { partitions } = this.meta;
    const partitionsToOpen = this.findPartitionsForIds(dict.keys());

    this.meta.length += dict.length;

    if (!this.start || this.start > firstKey) {
      this.meta.start = firstKey;
    }

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
          docs.set(id, dict.pop(id));
        }

        currentMeta.end = docs.lastKey || this.emptyKey;
        currentMeta.length = docs.length;
        // console.log(docs.toJSON());

        wfs(this.getPartitionFilename(partId), docs);
      }
    });


    wfs(`${this.settings.directory}meta.json`, this.meta);
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

    this.insertSortedDict(new SortedDictionary(keyType, dict, false, keys));
  }

  edit(ids: KeyType[], predicate: (id: KeyType, value: Type) => Type | undefined) {
    this.findPartitionsForIds(ids).forEach((ids, partitionId) => {
      const docs = this.openPartition(partitionId);
      for (const id of ids) {
        const newValue = predicate(id, docs[id as string]);
        if (newValue === undefined) {
          delete docs[id as string];
        } else {
          docs[id as string] = newValue;
        }
      }
      wfs(this.getPartitionFilename(partitionId), docs);
    });
  }

  remove(ids: KeyType[]) {
    this.edit(ids, () => undefined);
  }

  static findPartitionForId<KeyType extends string | number>(id: KeyType, dictMeta: FragDictMeta<KeyType>): number {
    const { partitions } = dictMeta;

    if (id <= dictMeta.start) {
      return 0;
    }
    if (!partitions.length) {
      return 0;
    }

    const lastPartitionID = partitions.length - 1;
    const lastPartitionMeta = partitions[lastPartitionID];
    if (id >= lastPartitionMeta.end) {
      return lastPartitionID;
    }

    let startPartitionID = 0;
    let startIndex: string | number = dictMeta.start;
    if (typeof id == "number") {
      const ratio = id / dictMeta.length;
      startPartitionID = Math.floor(partitions.length * ratio);

      while (startPartitionID > 0) {
        // startPartitionID--;
        startIndex = partitions[startPartitionID - 1].end;
        if (startIndex <= (id as (string | number))) {
          break;
        }
        startPartitionID--;
      }
    }

    for (let i = startPartitionID; i < partitions.length; i++) {
      const element = partitions[i];
      if (startIndex <= id && id <= element.end) return i;
      startIndex = element.end;
    }

    throw new Error(`couldn't find partition for id '${id}'`);
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
    const { keyType } = this.settings;
    const meta: PartitionMeta<KeyType> = { length: 0, end: getEmptyKey(keyType) };
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
    return new SortedDictionary<KeyType, Type>(this.settings.keyType, this.openPartition(index));
  }

}