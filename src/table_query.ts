import { TableRecord, TRecord } from "./record";
import FragmentedDictionary, { IDFilter, PartitionFilter, WhereRanges } from "./fragmented_dictionary";
import { RecordCallback, IndicesRecord, Table, EventRecordsRemove } from "./table";

import { uniq, flatten } from "lodash";
import SortedDictionary from "./sorted_dictionary";
import { abortTransaction, stopTrackingTransaction, trackTransaction } from "./virtual_fs";
import TableUtils from "./table_utilities";



function idFilterFromSet<KeyType extends string | number>(ids: KeyType[]): IDFilter<KeyType> {
  return id => ids.includes(id);
}

export function partitionFilterFromSet<KeyType extends string | number>(ids: KeyType[]): PartitionFilter<KeyType> {
  return (start, end) => {
    for (const id of ids) {
      if (start <= id || id <= end) return true;
    }
    return false;
  };
}

export function twoArgsToFilters<KeyType>(args: any[]): [IDFilter<KeyType>, PartitionFilter<KeyType>] {
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

export default class TableQuery<T, idT extends string | number, LightT, VisibleT> {
  protected table: Table<T, idT, any, any, LightT, VisibleT>;

  protected utils: TableUtils<T, idT>;

  protected filters: RecordCallback<T, idT, boolean, LightT, VisibleT>[] = [];
  protected _offset: number = 0;
  protected _limit: number | undefined;

  protected _orderBy: string | undefined;
  protected orderDirection: "ASC" | "DESC" = "ASC";
  protected idFilter: IDFilter<any> | undefined;
  protected partitionFilter: PartitionFilter<any> | undefined;
  protected whereField: string | undefined;

  constructor(table: Table<T, idT, any, any, LightT, VisibleT>, utils: TableUtils<T, idT>) {
    this.table = table;
    this.utils = utils;
  }

  protected convertRangesToFilter(fieldName: string, mappedRanges: WhereRanges<any>) {
    return this.filter(rec => {
      for (const [min, max] of mappedRanges) {
        if (min !== undefined && min > rec.$get(fieldName as any)) {
          continue;
        }
        if (max !== undefined && max < rec.$get(fieldName as any)) {
          continue;
        }
        return true;
      }
      return false;
    });
  }


  protected convertWhereToFilter<FieldType>(fieldName: string, idFilter: IDFilter<FieldType>) {
    this.filters.push(rec => {
      return idFilter(rec.$get(fieldName));
    });
    return this;
  }

  where<T1 extends keyof T>(fieldName: T1,
    idFilter: IDFilter<T[T1]>,
    partitionFilter?: PartitionFilter<T[T1]>): typeof this
  where<T1 extends keyof T>(fieldName: T1, ...values: T[T1][]): typeof this
  where<T1 extends keyof T>(fieldName: any, ...args: any[]) {
    let [idFilter, partitionFilter] = twoArgsToFilters<T[T1]>(args);

    if (fieldName != this.table.primaryKey && (this.whereField || !this.utils.fieldHasAnyTag(fieldName, "index", "unique"))) {
      return this.convertWhereToFilter(fieldName, idFilter);
    }

    this.whereField = fieldName;
    this.idFilter = idFilter;
    this.partitionFilter = partitionFilter;
    return this;
  }

  whereRange
    <T1 extends keyof T>(
      fieldName: T1,
      min: string | number | undefined,
      max: string | number | undefined
    ): typeof this {
    return this.where<T1>(fieldName, (val: any) => {
      if (min !== undefined && val < (min)) return false;
      if (max !== undefined && val > (max)) return false;
      return true;
    }, (start, end) => {
      return FragmentedDictionary.partitionMatchesRanges(start as any, end, [[min, max]]);
    });
  }

  filter(predicate: RecordCallback<T, idT, boolean, LightT, VisibleT>): typeof this {
    this.filters.push(predicate);
    return this;
  }

  /**
   *  @param useTableRecord return a predicate that uses TableRecord instead of raw data array
   */
  getFilterFunction(): undefined | ((data: any[], id: idT) => boolean)
  getFilterFunction(useTableRecord: true): undefined | ((rec: TRecord<T, idT, LightT, VisibleT>) => boolean)
  getFilterFunction(useTableRecord = false): any {
    if (!this.filter.length) return undefined;
    if (useTableRecord) {
      return (rec: TRecord<T, idT, LightT, VisibleT>) => {
        for (let i = 0; i < this.filters.length; i++) {
          let filter = this.filters[i];

          if (!filter(rec)) {
            return false;
          }
        }
        return true;
      }
    }
    return (data: any[], id: idT) => {
      const rec = new TableRecord(data, id, this.table, this.utils) as TRecord<T, idT, LightT, VisibleT>;
      for (let i = 0; i < this.filters.length; i++) {
        let filter = this.filters[i];

        if (!filter(rec)) {
          return false;
        }
      }
      return true;
    }
  }

  getQueryFilters(): [IDFilter<idT> | undefined, PartitionFilter<idT> | undefined] {
    let idFilter: IDFilter<idT> | undefined;
    let partitionFilter: PartitionFilter<idT> | undefined;
    const { whereField, table, utils } = this;

    if (whereField) {
      if (whereField == table.primaryKey) {
        return [this.idFilter, this.partitionFilter];
      }
      if (!this.idFilter) throw new Error("id filter is undefined"); //must be imposible
      const index = this.utils.indices[whereField];

      const rec = index.where({
        idFilter: this.idFilter,
        partitionFilter: this.partitionFilter,
        limit: 0,
        select: val => val
      })[0];

      let ids: idT[];
      if (utils.fieldHasAnyTag(whereField, "unique")) {
        ids = Object.values(rec);
      } else {
        ids = uniq(flatten(Object.values(rec)));
      }
      idFilter = idFilterFromSet(ids);
      partitionFilter = partitionFilterFromSet(ids);
    }
    return [idFilter, partitionFilter];
  }

  select<ReturnType = VisibleT>(predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>): ReturnType[] {
    if (this._orderBy !== undefined) {
      return this.orderedSelect(predicate);
    }
    const select = (data: any[], id: idT) => {
      const rec = new TableRecord(data, id, this.table, this.utils) as TRecord<T, idT, LightT, VisibleT>;
      if (predicate) {
        return predicate(rec);
      }
      return rec.toJSON();
    }

    const [idFilter, partitionFilter] = this.getQueryFilters();

    const res = this.utils.mainDict.where({
      idFilter,
      partitionFilter,
      filter: this.getFilterFunction(),
      select,
      limit: this._limit === undefined ? 100 : this._limit,
      offset: this._offset,
    });

    return Object.values(res[0]);
  }

  orderBy(fieldName: string & (keyof T | "id"), direction: "ASC" | "DESC" = "ASC") {
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

  update(predicate: RecordCallback<T, idT, void, LightT, VisibleT>): void {

    const newIds: {
      new: idT[];
      old: idT[];
      values: any[];
      records: LightT[];
    } = {
      new: [],
      old: [],
      values: [],
      records: [],
    }

    const update = (data: any[], id: idT) => {
      const rec = new TableRecord(data, id, this.table, this.utils) as TRecord<T, idT, LightT, VisibleT>;

      predicate(rec);
      const newId = rec.$id;

      if (id == newId) return rec.$serialize();

      newIds.new.push(newId);
      newIds.old.push(id);
      newIds.values.push(rec.$serialize());
      newIds.records.push(rec.$light());
      return undefined;
    }

    const [idFilter, partitionFilter] = this.getQueryFilters();

    const transactionID = trackTransaction();
    try {
      this.utils.mainDict.where({
        idFilter,
        partitionFilter,
        filter: this.getFilterFunction(),
        update,
        limit: this._limit || 0,
        offset: this._offset || 0,
      });

      if (newIds.new.length) {
        this.utils.mainDict.insertMany(newIds.new, newIds.values);
        const [indexData, oldIndexData] = this.utils.buildIndexDataForRecords(newIds.records as Partial<T>[], newIds.old);
        this.utils.removeIndexData(oldIndexData);
        this.utils.insertIndexData(indexData);
        this.utils.renameHeavyFiles(newIds.old, newIds.new);
        if (this.table.autoId) {
          this.utils.updateLastId(newIds.new as number[]);
        }
      }
    } catch (err) {
      abortTransaction(transactionID);
      throw err;
    }
    stopTrackingTransaction(transactionID);
  }

  delete() {

    const [idFilter, partitionFilter] = this.getQueryFilters();
    const removed: LightT[] = [];

    const hasHeavyRemoveListener = this.table.hasEventListener("recordsRemove");
    const removedFull: T[] = [];

    const transactionID = trackTransaction();
    let ids: idT[];
    try {
      [, ids] = this.utils.mainDict.where({
        idFilter,
        partitionFilter,
        filter: this.getFilterFunction(),
        update: (data: any[], id: idT) => {
          let rec = new TableRecord(data, id, this.table, this.utils);
          removed.push(rec.$light());
          if (hasHeavyRemoveListener) {
            removedFull.push(rec.$full());
          }
          return undefined;
        },
        limit: this._limit || 0,
        offset: this._offset || 0,
      });

      const indexData = this.utils.buildIndexDataForRecords(removed as Partial<T>[]);
      this.utils.removeIndexData(indexData);
    } catch (err) {
      abortTransaction(transactionID);
      throw err;
    }
    stopTrackingTransaction(transactionID);
    this.utils.removeHeavyFilesForEachID(ids);
    if (hasHeavyRemoveListener)
      this.table.triggerEvent("recordsRemove", {
        records: removedFull
      });


    this.table.triggerEvent("recordsRemoveLight", {
      records: removed,
    });
    return removed;
  }

  throwInvalidIndex(name: string | undefined) {
    throw new Error(`${name} is an invalid index`);
  }

  protected orderedSelect<ReturnType = T>(predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>): ReturnType[] {
    if (this._orderBy === undefined) {
      this.throwInvalidIndex(undefined);
      return [];
    }
    const { _orderBy: orderBy, table, _limit: limit, _offset: offset, orderDirection, utils } = this;
    const { mainDict, indices } = this.utils;
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
    const isIndexUnique = utils.fieldHasAnyTag(orderBy, "unique");
    const openedPartitions: Record<number, SortedDictionary<idT, any[]>> = {};
    const result: ReturnType[] = [];
    const filter = this.getFilterFunction(true);
    let found = 0;

    for (const ids of allIds) {
      const idsToiterate: idT[] = isIndexUnique ? [ids] : ids;
      for (const id of idsToiterate) {
        const partId = mainDict.findPartitionForId(id);
        if (!openedPartitions[partId]) {
          openedPartitions[partId] = mainDict.openPartition(partId);
        }
        const partition = openedPartitions[partId];
        const rec: TRecord<T, idT, LightT, VisibleT> = new TableRecord(partition.get(id) as any, id, table, this.utils) as any;
        if (filter && !filter(rec)) continue;
        found++;

        if (found > offset) result.push(predicate ? predicate(rec) : <any>rec.toJSON());
        if (limit && (result.length >= limit)) {
          return result;
        }
      }
    }

    return result;
  }
}