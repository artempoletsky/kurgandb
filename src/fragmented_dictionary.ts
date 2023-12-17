
import { PlainObject, existsSync, mkdirSync, renameSync, rfs, rmie, statSync, wfs } from "./utils";


type ValueType = "array" | "value";
// type KeyType = ;

export type FragmentedDictionarySettings = {
  keyType: "int" | "string",
  maxPartitionSize: number,
  maxPartitionLenght: number,
  directory: string,
};


export type PartitionMeta = {
  length: number
  end: number | string
}

export type FragDictMeta = {
  length: number
  start: number | string
  partitions: PartitionMeta[]
}

export type PartitionIterateMap<KeyType> = Map<number, KeyType[]>;

const EMPTY_DICT_META: FragDictMeta = {
  length: 0,
  start: "",
  partitions: [],
};


const DefaultFragmentDictionarySettings: FragmentedDictionarySettings = {
  keyType: "int",
  maxPartitionLenght: 10 * 1000,
  maxPartitionSize: 5 * Math.pow(1024, 3),
  directory: "",
};


function maxKey<KeyType extends string | number>(i1: KeyType, i2: KeyType): KeyType {
  return i1 > i2 ? i1 : i2;
}

function minKey<KeyType extends string | number>(i1: KeyType, i2: KeyType): KeyType {
  return i1 < i2 ? i1 : i2;
}

export default class FragmentedDictionary<KeyType extends string | number, Type> {
  public readonly settings: FragmentedDictionarySettings;
  public meta: FragDictMeta;

  static reset<KeyType extends string | number, Type>(dict: FragmentedDictionary<KeyType, Type>): FragmentedDictionary<KeyType, Type> {
    const settings = dict.settings;
    dict.destroy();
    return this.init(settings);
  }

  static init<KeyType extends string | number, Type>(settings: Partial<FragmentedDictionarySettings> = {}) {
    const opt = Object.assign({}, DefaultFragmentDictionarySettings, settings);
    if (!settings.directory) throw new Error(`'directory' is a required parameter!`);
    if (existsSync(opt.directory)) throw new Error(`directory '${opt.directory}' already exists`);
    mkdirSync(opt.directory);
    const metaFilename = `${opt.directory}meta.json`;
    wfs(metaFilename, EMPTY_DICT_META);
    wfs(`${opt.directory}settings.json`, opt);
    return this.open<KeyType, Type>(opt.directory);
  }

  static open<KeyType extends string | number, Type>(directory: string) {
    const metaFilename = `${directory}meta.json`;
    const settingsFilename = `${directory}settings.json`;
    return new FragmentedDictionary<KeyType, Type>(rfs(settingsFilename), rfs(metaFilename));
  }

  constructor(settings: FragmentedDictionarySettings, meta: FragDictMeta) {
    this.settings = settings;
    if (!existsSync(settings.directory)) throw new Error(`directory '${settings.directory}' doesn't exists`);
    this.meta = meta;
  }

  destroy() {
    rmie(this.settings.directory);
  }

  public get lenght(): number {
    return this.meta.length;
  }

  public get end(): KeyType {
    const { partitions } = this.meta;
    const { keyType } = this.settings;
    if (!partitions.length) return (keyType == "int" ? 0 : "") as KeyType;
    return partitions[partitions.length - 1].end as KeyType;
  }

  public get start(): KeyType {
    return this.meta.start as KeyType;
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
    const { keyType } = this.settings;
    const docs = this.openPartition(partId);
    const part1: Record<string, Type> = {};
    const part2: Record<string, Type> = {};

    this.createNewPartition(partId);
    const meta1: PartitionMeta = this.meta.partitions[partId];
    const meta2: PartitionMeta = this.meta.partitions[partId + 1];
    meta2.length = 0;

    for (const idStr in docs) {
      const id = (keyType == "int" ? idStr as any * 1 : idStr) as KeyType;

      if (id < key) {
        meta1.length++;
        meta1.end = maxKey(id, meta1.end);
        part1[idStr] = docs[idStr];
      } else {
        meta2.length++;
        meta2.end = maxKey(id, meta2.end);
        part2[idStr] = docs[idStr];
      }
    }


    wfs(this.getPartitionFilename(partId), part1);
    wfs(this.getPartitionFilename(partId + 1), part2);
    return meta1.length < meta2.length ? partId : partId + 1;
  }

  insertArray(arr: Type[]) {
    const { keyType } = this.settings;
    if (keyType == "string") throw new Error("'insertArray' is only supported on 'int' keyType dictionaries!");

    const lastID = this.end as number;
    const ids = arr.map((val, i) => lastID + i + 1);
    this.insertMany(ids as any, arr);
    return ids;
  }

  insertMany(dict: Record<string, Type>): void
  insertMany(keys: KeyType[], values: Type[]): void
  insertMany(keys: KeyType[], values: Type[], dict: Record<string, Type>): void
  insertMany(arg1: any, arg2?: any, arg3?: any): void {
    // const reducer = (d: Record<string, Type>, k: string | number) => { d[k] = dict[k]; return d; };
    const { maxPartitionLenght, keyType, maxPartitionSize } = this.settings;
    const { partitions } = this.meta;

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

    const firstKey = keys[0];
    if (this.lenght == 0) {
      // TODO: sort keys
      this.meta.start = firstKey as any;
    }

    if (this.meta.start > firstKey) {
      this.meta.start = firstKey as any;
    }



    const partitionsToOpen = this.findPartitionsForIds(keys);
    Array.from(partitionsToOpen.keys()).sort().reverse().forEach(partId => {
      let tail: KeyType[] = partitionsToOpen.get(partId) as KeyType[];

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


        let idsToInsert;
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
          docs[id as string] = dict[id as string];
          currentMeta.end = maxKey(id, currentMeta.end);
          currentMeta.length++;
        }

        wfs(this.getPartitionFilename(partId), docs);
      }
    });

    this.meta.length += keys.length;

    wfs(`${this.settings.directory}meta.json`, this.meta);

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

  static findPartitionForId<KeyType>(id: KeyType, dictMeta: FragDictMeta): number {
    const { partitions } = dictMeta;

    if (id <= dictMeta.start) {
      return 0;
    }
    if (!partitions.length) {
      return 0;
    }

    const lastPartitionID = partitions.length - 1;
    const lastPartitionMeta = partitions[lastPartitionID];
    if (id > lastPartitionMeta.end) {
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

  createNewPartition(id: number): PartitionMeta {
    const { keyType } = this.settings;
    const meta: PartitionMeta = { length: 0, end: keyType == "int" ? 0 : "" };
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
    const meta: PartitionMeta | undefined = this.meta.partitions[index];
    if (!meta) {
      throw new Error(`partition '${index}' doesn't exists`);
    }

    return rfs(this.getPartitionFilename(index));
  }

}