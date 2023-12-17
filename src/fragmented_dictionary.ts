
import { PlainObject, existsSync, mkdirSync, renameSync, rfs, rmie, statSync, wfs } from "./utils";


type ValueType = "array" | "value";
type KeyType = "int" | "string";

export type FragmentedDictionarySettings = {
  keyType: KeyType,
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

export default class FragmentedDictionary<Type> {
  public readonly settings: FragmentedDictionarySettings;
  public meta: FragDictMeta;

  static init<Type>(settings: Partial<FragmentedDictionarySettings> = {}) {
    const opt = Object.assign({}, DefaultFragmentDictionarySettings, settings);
    if (!settings.directory) throw new Error(`'directory' is a required parameter!`);
    if (existsSync(opt.directory)) throw new Error(`directory '${opt.directory}' already exists`);
    mkdirSync(opt.directory);
    const metaFilename = `${opt.directory}meta.json`;
    wfs(metaFilename, EMPTY_DICT_META);
    wfs(`${opt.directory}settings.json`, opt);
    return this.open<Type>(opt.directory);
  }

  static open<Type>(directory: string) {
    const metaFilename = `${directory}meta.json`;
    const settingsFilename = `${directory}settings.json`;
    return new FragmentedDictionary<Type>(rfs(settingsFilename), rfs(metaFilename));
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

  public get lastID(): string | number {
    const { partitions } = this.meta;
    const { keyType } = this.settings;
    if (!partitions.length) return keyType == "int" ? 0 : "";
    return partitions[partitions.length - 1].end;
  }

  public get numPartitions(): number {
    return this.meta.partitions.length;
  }

  findPartitionsForIds<KeyType extends string | number>(ids: KeyType[]) {
    const partitionsToOpen = new Map<number, KeyType[]>();
    for (const id of ids) {
      let partId = this.findPartitionForId(id);
      if (partId === false) continue;
      let pIds = partitionsToOpen.get(partId) || [];
      pIds.push(id);
      partitionsToOpen.set(partId, pIds);
    }
    return partitionsToOpen;
  }

  dividePartition(id: number) {

  }

  insertArray(arr: Type[]) {
    const { keyType } = this.settings;
    if (keyType == "string") throw new Error("'insertArray' is only supported on 'int' keyType dictionaries!");

    const lastID = this.lastID as number;
    const ids = arr.map((val, i) => lastID + i + 1);
    this.insertMany(ids, arr);
    return ids;
  }

  insertMany(dict: Record<string, Type>): void
  insertMany<KeyType extends string | number>(keys: KeyType[], values: Type[]): void
  insertMany<KeyType extends string | number>(keys: KeyType[], values: Type[], dict: Record<string, Type>): void
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

    if (this.lenght == 0) {
      // TODO: sort keys
      this.meta.start = keys[0];
    }

    if (this.meta.start > keys[0]) {
      this.meta.start = keys[0];
    }



    const partitionsToOpen = this.findPartitionsForIds(keys);
    Array.from(partitionsToOpen.keys()).sort().reverse().forEach(partId => {
      let tail = partitionsToOpen.get(partId) as (string | number)[];

      while (tail.length > 0) {
        let currentMeta = partitions[partId];
        if (!currentMeta) {
          currentMeta = this.createNewPartition(partId);
        }

        let capacity = maxPartitionLenght - currentMeta.length;
        if (capacity <= 0) {
          if (tail[0] > currentMeta.end)
            partId++;

          currentMeta = this.createNewPartition(partId);
        } else if (maxPartitionSize) {
          const size = statSync(this.getPartitionFilename(partId)).size;
          if (size >= maxPartitionSize) {
            if (tail[0] > currentMeta.end)
              partId++;
            currentMeta = this.createNewPartition(partId);
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
          // if(id == "p") debugger;
          docs[id] = dict[id];
          currentMeta.end = id;
          currentMeta.length++;
        }

        wfs(this.getPartitionFilename(partId), docs);
      }
    });

    this.meta.length += keys.length;

    wfs(`${this.settings.directory}meta.json`, this.meta);

  }

  edit<T extends string | number>(ids: T[], predicate: (id: T, value: Type) => Type | undefined) {
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

  remove<T extends string | number>(ids: T[]) {
    this.edit<T>(ids, () => undefined);
  }

  static findPartitionForId(id: number | string, dictMeta: FragDictMeta): number {
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

  findPartitionForId(id: number | string): number | false {
    return FragmentedDictionary.findPartitionForId(id, this.meta);
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
    const meta: PartitionMeta = { length: 0, end: 0 };
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