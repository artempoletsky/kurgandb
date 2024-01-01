import flatten from "lodash.flatten";
import { Document, TDocument } from "./document";
import FragmentedDictionary, { WhereRanges } from "./fragmented_dictionary";
import { DocCallback, IndicesRecord, MainDict, Table } from "./table";
import { PlainObject } from "./utils";
import uniq from "lodash.uniq";
import SortedDictionary from "./sorted_dictionary";


// export type UpdatePredicate<Type> = (doc: TDocument<Type>) => void;
// export type SelectPredicate<Type, ReturnType = any> = (doc: TDocument<Type>) => ReturnType;

type WhereFilter<KeyType extends string | number, Type> = {
  fieldName: keyof Type & string,
  ranges: WhereRanges<KeyType>
}

export default class TableQuery<KeyType extends string | number, Type> {
  protected table: Table<KeyType, Type>;
  protected indices: IndicesRecord;
  protected mainDict: MainDict<KeyType>;

  protected whereFilter: WhereFilter<string | number, Type> | undefined;
  protected filters: DocCallback<KeyType, Type, boolean>[] = [];
  protected _offset: number = 0;
  protected _limit: number | undefined;

  protected _orderBy: string | undefined;
  protected orderDirection: "ASC" | "DESC" = "ASC";

  constructor(table: Table<KeyType, Type>, indices: IndicesRecord, mainDict: MainDict<KeyType>) {
    this.table = table;
    this.indices = indices;
    this.mainDict = mainDict;
  }

  whereRanges<ValueType extends string | number | Date | boolean>(fieldName: keyof Type & string, ranges: [ValueType | undefined, ValueType | undefined][]): TableQuery<KeyType, Type> {
    const type = this.table.scheme.fields[fieldName];
    const mappedRanges = ranges.map<[string | number | undefined, string | number | undefined]>(([min, max]) => {
      return [Document.storeValueOfType(min as any, type as any), Document.storeValueOfType(max as any, type as any)];
    });
    if (this.table.primaryKey == fieldName || this.table.fieldHasAnyTag(fieldName, "index", "unique")) {
      this.whereFilter = { fieldName, ranges: mappedRanges };
      return this;
    }


    return this.convertRangesToFilter(fieldName, mappedRanges);
  }

  protected convertRangesToFilter(fieldName: string, mappedRanges: WhereRanges<any>) {
    return this.filter(doc => {
      for (const [min, max] of mappedRanges) {
        if (min !== undefined && min > doc.get(fieldName as any)) {
          continue;
        }
        if (max !== undefined && max < doc.get(fieldName as any)) {
          continue;
        }
        return true;
      }
      return false;
    });
  }

  where<ValueType extends string | number | Date | boolean>(fieldName: keyof Type & string, value: ValueType) {
    return this.whereRange(fieldName, value, value);
  }

  whereRange<ValueType extends string | number | Date | boolean | undefined>(fieldName: keyof Type & string, min: ValueType, max: ValueType): TableQuery<KeyType, Type> {
    return this.whereRanges(fieldName, [[min, max]]);
  }

  filter(predicate: DocCallback<KeyType, Type, boolean>): TableQuery<KeyType, Type> {
    this.filters.push(predicate);
    return this;
  }

  getFilterFunction(): undefined | ((data: any[], id: KeyType) => boolean)
  getFilterFunction(useDocument: true): undefined | ((doc: TDocument<KeyType, Type>) => boolean)
  getFilterFunction(useDocument = false): any {
    if (!this.filter.length) return undefined;
    if (useDocument) {
      return (doc: TDocument<KeyType, Type>) => {
        for (let i = 0; i < this.filters.length; i++) {
          let filter = this.filters[i];

          if (!filter(doc)) {
            return false;
          }
        }
        return true;
      }
    }
    return (data: any[], id: KeyType) => {
      const doc = new Document(data, id, this.table, this.indices) as TDocument<KeyType, Type>;
      for (let i = 0; i < this.filters.length; i++) {
        let filter = this.filters[i];

        if (!filter(doc)) {
          return false;
        }
      }
      return true;
    }
  }

  getQueryRanges(): WhereRanges<KeyType> {
    if (!this.whereFilter) return [[undefined, undefined]];

    const { fieldName } = this.whereFilter;
    let ids: KeyType[];
    if (fieldName == this.table.primaryKey) {
      return this.whereFilter.ranges as WhereRanges<KeyType>;
    }

    const rec = this.indices[fieldName].filterSelect(this.whereFilter.ranges, 0);

    if (this.table.fieldHasAnyTag(fieldName, "unique")) {
      ids = Object.values(rec);
    } else {
      ids = uniq(flatten(Object.values(rec)));
    }

    return ids.map(id => [id, id]);
  }

  select<ReturnType = Type>(predicate?: DocCallback<KeyType, Type, ReturnType>): ReturnType[] {
    if (this._orderBy !== undefined) {
      return this.orderedSelect(predicate);
    }
    const select = (data: any[], id: KeyType) => {
      const doc = new Document(data, id, this.table, this.indices) as TDocument<KeyType, Type>;
      if (predicate) {
        return predicate(doc);
      }
      return doc.toJSON();
    }

    const res = this.mainDict.iterateRanges({
      ranges: this.getQueryRanges(),
      filter: this.getFilterFunction(),
      select,
      limit: this._limit === undefined ? 100 : this._limit,
      offset: this._offset,
    });

    return Object.values(res[0]);
  }

  orderBy(fieldName: string & (keyof Type | "id"), direction: "ASC" | "DESC" = "ASC") {
    this._orderBy = fieldName;
    this.orderDirection = direction;
    return this;
  }

  offset(offset: number) {
    this._offset = offset;
    return this;
  }

  limit(limit: number) {
    this._limit = limit;
    return this;
  }

  paginate(pageNum: number, pageSize: number) {
    this.offset(pageSize * (pageNum - 1));
    return this.limit(pageSize);
  }

  update(predicate: DocCallback<KeyType, Type, void>): void {

    const update = (data: any[], id: KeyType) => {
      const doc = new Document(data, id, this.table, this.indices) as TDocument<KeyType, Type>;
      if (!predicate) {
        throw new Error("Update predicate is undefined");
      }
      predicate(doc);
      return doc.serialize();
    }

    this.mainDict.iterateRanges({
      ranges: this.getQueryRanges(),
      filter: this.getFilterFunction(),
      update,
      limit: this._limit || 0,
      offset: this._offset || 0,
    });
  }

  delete() {
    const res = this.mainDict.iterateRanges({
      ranges: this.getQueryRanges(),
      filter: this.getFilterFunction(),
      update: (data: any[], id: KeyType) => {
        this.table.removeIdFromIndex(id, data);
        return undefined;
      },
      limit: this._limit || 0,
      offset: this._offset || 0,
    });
    return res[1];
  }

  throwInvalidIndex(name: string | undefined) {
    throw new Error(`${name} is an invalid index`);
  }

  protected orderedSelect<ReturnType = Type>(predicate?: DocCallback<KeyType, Type, ReturnType>): ReturnType[] {
    if (this._orderBy === undefined) {
      this.throwInvalidIndex(undefined);
      return [];
    }
    const { _orderBy: orderBy, mainDict, table, indices, _limit: limit, _offset: offset, orderDirection } = this;
    const index = indices[orderBy];
    if (!index) this.throwInvalidIndex(orderBy);

    if (this.whereFilter) {
      this.convertRangesToFilter(this.whereFilter.fieldName, this.whereFilter.ranges);
    }

    const all = index.loadAll();
    let allIds = all.values();
    if (orderDirection == "DESC") {
      allIds = allIds.reverse();
    }
    const isIndexUnique = table.fieldHasAnyTag(orderBy, "unique");
    const openedPartitions: Record<number, SortedDictionary<KeyType, any[]>> = {};
    const result: ReturnType[] = [];
    const filter = this.getFilterFunction(true);
    let found = 0;

    for (const ids of allIds) {
      const idsToiterate = isIndexUnique ? [ids] : ids;
      for (const id of idsToiterate) {
        const partId = mainDict.findPartitionForId(id);
        if (!openedPartitions[partId]) {
          openedPartitions[partId] = mainDict.openPartition(partId);
        }
        const partition = openedPartitions[partId];
        const doc: TDocument<KeyType, Type> = new Document(partition.get(id), id, table, indices) as any;
        if (filter && !filter(doc)) continue;
        found++;

        if (found > offset) result.push(predicate ? predicate(doc) : <any>doc.toJSON());
        if (limit && (result.length >= limit)) {
          return result;
        }
      }
    }

    return result;
  }
}