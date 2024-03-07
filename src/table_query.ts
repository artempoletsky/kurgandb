import { TableRecord, TRecord } from "./record";
import FragmentedDictionary, { IDFilter, PartitionFilter, WhereRanges } from "./fragmented_dictionary";
import { RecordCallback, IndicesRecord, Table } from "./table";

import { uniq, flatten } from "lodash";
import SortedDictionary from "./sorted_dictionary";
import { $ } from "./globals";
import { abortOperation, stopTrackingOperation, trackOperation } from "./virtual_fs";



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

export default class TableQuery<T, idT extends string | number, LightT, VisibleT> {
  protected table: Table<T, idT, any, any, LightT, VisibleT>;
  protected indices: IndicesRecord;
  protected mainDict: FragmentedDictionary<idT>;

  protected filters: RecordCallback<T, idT, boolean, LightT, VisibleT>[] = [];
  protected _offset: number = 0;
  protected _limit: number | undefined;

  protected _orderBy: string | undefined;
  protected orderDirection: "ASC" | "DESC" = "ASC";
  protected idFilter: IDFilter<any> | undefined;
  protected partitionFilter: PartitionFilter<any> | undefined;
  protected whereField: string | undefined;

  constructor(table: Table<T, idT, any, any, LightT, VisibleT>, indices: IndicesRecord, mainDict: FragmentedDictionary<idT>) {
    this.table = table;
    this.indices = indices;
    this.mainDict = mainDict;
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


  protected convertWhereToFilter<FieldType extends string | number>(fieldName: string, idFilter: IDFilter<FieldType>) {
    this.filters.push(rec => {
      return idFilter(rec.$get(fieldName));
    });
    return this;
  }

  where<FieldType extends string | number>(fieldName: keyof T,
    idFilter: IDFilter<FieldType>,
    partitionFilter?: PartitionFilter<FieldType>): typeof this
  where<FieldType extends string | number>(fieldName: keyof T, ...values: FieldType[]): typeof this
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
      fieldName: keyof T & string,
      min: FieldType | undefined,
      max: FieldType | undefined
    ): typeof this {
    return this.where<any>(fieldName, (val: any) => {
      if (min !== undefined && val < min) return false;
      if (max !== undefined && val > max) return false;
      return true;
    }, (start, end) => {
      return FragmentedDictionary.partitionMatchesRanges(start, end, [[min, max]]);
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
      const rec = new TableRecord(data, id, this.table, this.indices) as TRecord<T, idT, LightT, VisibleT>;
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

      let ids: idT[];
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

  select<ReturnType = VisibleT>(predicate?: RecordCallback<T, idT, ReturnType, LightT, VisibleT>): ReturnType[] {
    if (this._orderBy !== undefined) {
      return this.orderedSelect(predicate);
    }
    const select = (data: any[], id: idT) => {
      const rec = new TableRecord(data, id, this.table, this.indices) as TRecord<T, idT, LightT, VisibleT>;
      if (predicate) {
        return predicate(rec);
      }
      return rec.toJSON();
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

    const update = (data: any[], id: idT) => {
      const rec = new TableRecord(data, id, this.table, this.indices) as TRecord<T, idT, LightT, VisibleT>;
      predicate(rec);
      return rec.$serialize();
    }

    const [idFilter, partitionFilter] = this.getQueryFilters();

    const operationID = trackOperation();
    try {
      this.mainDict.where({
        idFilter,
        partitionFilter,
        filter: this.getFilterFunction(),
        update,
        limit: this._limit || 0,
        offset: this._offset || 0,
      });
    } catch (err) {
      abortOperation(operationID);
      throw err;
    }
    stopTrackingOperation(operationID);
  }

  delete() {

    const [idFilter, partitionFilter] = this.getQueryFilters();
    const removed: LightT[] = []

    const operationID = trackOperation();
    try {
      const [, ids] = this.mainDict.where({
        idFilter,
        partitionFilter,
        filter: this.getFilterFunction(),
        update: (data: any[], id: idT) => {
          let rec = new TableRecord(data, id, this.table, this.indices);
          removed.push(rec.$light());
          return undefined;
        },
        limit: this._limit || 0,
        offset: this._offset || 0,
      });

      this.table.removeFromIndex(removed);
      this.table.removeHeavyFilesForEachID(ids);
    } catch (err) {
      abortOperation(operationID);
      throw err;
    }
    stopTrackingOperation(operationID);
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
        const rec: TRecord<T, idT, LightT, VisibleT> = new TableRecord(partition.get(id), id, table, indices) as any;
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