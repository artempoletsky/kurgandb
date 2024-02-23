import { Document, TDocument } from "./document";
import FragmentedDictionary, { IDFilter, PartitionFilter, WhereRanges } from "./fragmented_dictionary";
import { DocCallback, IndicesRecord, MainDict, Table } from "./table";

import { uniq, flatten } from "lodash";
import SortedDictionary from "./sorted_dictionary";


// export type UpdatePredicate<Type> = (doc: TDocument<Type>) => void;
// export type SelectPredicate<Type, ReturnType = any> = (doc: TDocument<Type>) => ReturnType;

type WhereFilter<KeyType extends string | number, Type> = {
  fieldName: keyof Type & string,
  ranges: WhereRanges<KeyType>
}


function idFilterFromSet<KeyType extends string | number>(ids: KeyType[]): IDFilter<KeyType> {
  return id => ids.includes(id);
}

function partitionFilterFromSet<KeyType extends string | number>(ids: KeyType[]): PartitionFilter<KeyType> {
  return (start, end) => {
    for (const id of ids) {
      if (start <= id || id <= end) return true;
    }
    return false;
  };
}

export function twoArgsToFilters<KeyType extends string | number>(args: any[]): [IDFilter<KeyType>, PartitionFilter<KeyType>] {
  let idFilter: IDFilter<KeyType>, partitionFilter: PartitionFilter<KeyType>;
  if (typeof args[0] == "function") {
    idFilter = args[0];
    partitionFilter = args[1];
  } else {
    idFilter = idFilterFromSet(args);
    partitionFilter = partitionFilterFromSet(args);
  }
  return [idFilter, partitionFilter];
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
  protected idFilter: IDFilter<any> | undefined;
  protected partitionFilter: PartitionFilter<any> | undefined;
  protected whereField: string | undefined;

  constructor(table: Table<KeyType, Type, any>, indices: IndicesRecord, mainDict: MainDict<KeyType>) {
    this.table = table;
    this.indices = indices;
    this.mainDict = mainDict;
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


  protected convertWhereToFilter<FieldType extends string | number>(fieldName: string, idFilter: IDFilter<FieldType>) {
    this.filters.push(doc => {
      return idFilter(doc.get(fieldName));
    });
    return this;
  }

  where<FieldType extends string | number>(fieldName: keyof Type | "id",
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): TableQuery<KeyType, Type>
  where<FieldType extends string | number>(fieldName: keyof Type | "id", ...values: FieldType[]): TableQuery<KeyType, Type>
  where<FieldType extends string | number>(fieldName: any, ...args: any[]) {
    let [idFilter, partitionFilter] = twoArgsToFilters<FieldType>(args);

    if (fieldName != this.table.primaryKey && (this.whereField || !this.table.fieldHasAnyTag(fieldName, "index", "unique"))) {
      return this.convertWhereToFilter<FieldType>(fieldName, idFilter);
    }

    this.whereField = fieldName;
    this.idFilter = idFilter;
    this.partitionFilter = partitionFilter;
    return this;
  }

  whereRange
    <FieldType extends string | number>(
      fieldName: keyof Type & string,
      min: FieldType | undefined,
      max: FieldType | undefined
    ): TableQuery<KeyType, Type> {
    return this.where<any>(fieldName, (val: any) => {
      if (min !== undefined && val < min) return false;
      if (max !== undefined && val > max) return false;
      return true;
    }, (start, end) => {
      return FragmentedDictionary.partitionMatchesRanges(start, end, [[min, max]]);
    });
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

  getQueryFilters(): [IDFilter<KeyType> | undefined, PartitionFilter<KeyType> | undefined] {
    let idFilter: IDFilter<KeyType> | undefined;
    let partitionFilter: PartitionFilter<KeyType> | undefined;
    const { whereField, table } = this;

    if (whereField) {
      if (whereField == table.primaryKey) {
        return [this.idFilter, this.partitionFilter];
      }
      if (!this.idFilter) throw new Error("id filter is undefined"); //must be imposible
      const index = this.indices[whereField];

      const rec = index.where({
        idFilter: this.idFilter,
        partitionFilter: this.partitionFilter,
        limit: 0,
        select: val => val
      })[0];

      let ids: KeyType[];
      if (table.fieldHasAnyTag(whereField, "unique")) {
        ids = Object.values(rec);
      } else {
        ids = uniq(flatten(Object.values(rec)));
      }
      idFilter = idFilterFromSet(ids);
      partitionFilter = partitionFilterFromSet(ids);
    }
    return [idFilter, partitionFilter];
  }

  select<ReturnType = Type & { id: number }>(predicate?: DocCallback<KeyType, Type, ReturnType>): ReturnType[] {
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

    const [idFilter, partitionFilter] = this.getQueryFilters();

    const res = this.mainDict.where({
      idFilter,
      partitionFilter,
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

    const [idFilter, partitionFilter] = this.getQueryFilters();

    this.mainDict.where({
      idFilter,
      partitionFilter,
      filter: this.getFilterFunction(),
      update,
      limit: this._limit || 0,
      offset: this._offset || 0,
    });
  }

  delete() {

    const [idFilter, partitionFilter] = this.getQueryFilters();
    const res = this.mainDict.where({
      idFilter,
      partitionFilter,
      filter: this.getFilterFunction(),
      update: (data: any[], id: KeyType) => {
        this.table.removeIdFromIndex(id, data); //TODO: refactor me
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

    if (this.whereField) {
      if (!this.idFilter) throw new Error("id filter is not defined");
      this.convertWhereToFilter(this.whereField, this.idFilter);
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
      const idsToiterate: KeyType[] = isIndexUnique ? [ids] : ids;
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