import flatten from "lodash.flatten";
import { Document, TDocument } from "./document";
import FragmentedDictionary, { WhereRanges } from "./fragmented_dictionary";
import { DocCallback, IndicesRecord, MainDict, Table } from "./table";
import { PlainObject } from "./utils";
import uniq from "lodash.uniq";


// export type UpdatePredicate<Type> = (doc: TDocument<Type>) => void;
// export type SelectPredicate<Type, ReturnType = any> = (doc: TDocument<Type>) => ReturnType;

type WhereFilter<KeyType extends string | number, Type> = {
  fieldName: keyof Type & string,
  ranges: WhereRanges<KeyType>
}

export default class TableQuery<KeyType extends string | number, Type> {
  protected table: Table<KeyType, Type>;
  protected whereFilter: WhereFilter<string | number, Type> | undefined;
  protected filters: DocCallback<KeyType, Type, boolean>[] = [];
  protected indices: IndicesRecord;
  protected mainDict: MainDict<KeyType>;
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


    return this.filter(doc => {
      for (const [min, max] of mappedRanges) {
        if (min !== undefined && min > doc[fieldName]) {
          continue;
        }
        if (max !== undefined && max < doc[fieldName]) {
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

  whereRange<ValueType extends string | number | Date | boolean>(fieldName: keyof Type & string, min: ValueType, max: ValueType): TableQuery<KeyType, Type> {
    return this.whereRanges(fieldName, [[min, max]]);
  }

  filter(predicate: DocCallback<KeyType, Type, boolean>): TableQuery<KeyType, Type> {
    this.filters.push(predicate);
    return this;
  }

  getFilterFunction() {
    if (!this.filter.length) return undefined;
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
    if (this.whereFilter.fieldName == "float") debugger;
    const rec = this.indices[fieldName].filterSelect(this.whereFilter.ranges, 0);

    if (this.table.fieldHasAnyTag(fieldName, "unique")) {
      ids = Object.values(rec);
    } else {
      ids = uniq(flatten(Object.values(rec)));
    }

    return ids.map(id => [id, id]);
  }

  select(limit?: number): Type[]
  select<ReturnType>(limit: number, predicate?: DocCallback<KeyType, Type, ReturnType>): ReturnType[]
  select<ReturnType = Type>(limit = 100, predicate?: DocCallback<KeyType, Type, ReturnType>): (ReturnType | Type)[] {
    const select = (data: any[], id: KeyType) => {
      const doc = new Document(data, id, this.table, this.indices) as TDocument<KeyType, Type>;
      if (predicate) {
        return predicate(doc);
      }
      return doc.toJSON();
    }
    if (this.whereFilter?.ranges[0][0] === 0) debugger;
    const res = this.mainDict.iterateRanges(this.getQueryRanges(), this.getFilterFunction(), undefined, select, limit);
    return Object.values(res[0]);
  }

  update(limit = 0, predicate: DocCallback<KeyType, Type, void>): void {

    const update = (data: any[], id: KeyType) => {
      const doc = new Document(data, id, this.table, this.indices) as TDocument<KeyType, Type>;
      if (!predicate) {
        throw new Error("Update predicate is undefined");
      }
      predicate(doc);
      return doc.serialize();
    }

    this.mainDict.iterateRanges(this.getQueryRanges(), this.getFilterFunction(), update, undefined, limit);
  }

  delete(limit = 0) {
    const res = this.mainDict.iterateRanges(this.getQueryRanges(), this.getFilterFunction(), (data: any[], id: KeyType) => {
      this.table.removeIdFromIndex(id, data);
      return undefined;
    }, undefined, limit);
    return res[1];
  }
}