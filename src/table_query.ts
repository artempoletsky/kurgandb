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
    // Document.retrieveValueOfType()
    const type = this.table.scheme.fields[fieldName];
    const mappedRanges = ranges.map<[string | number | undefined, string | number | undefined]>(([min, max]) => {
      return [Document.storeValueOfType(min as any, type), Document.storeValueOfType(max as any, type)];
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

  exec(limit: number, operation: "update", predicate: Function): void
  exec(limit: number, operation: "select", predicate?: Function): any[]
  exec(limit: number, operation: "delete", predicate?: undefined): string[] | number[]
  exec(limit = 100, operation: "select" | "update" | "delete", predicate?: Function) {
    const execfn = (id: string | number, value: any[]) => {
      const doc = new Document(value, id, this.table, this.indices) as TDocument<KeyType, Type>;
      for (let i = 0; i < this.filters.length; i++) {
        let filter = this.filters[i];

        if (!filter(doc)) {
          if (operation == "update") {
            return value;
          } else if (operation == "delete") {
            return false;
          } else {
            return undefined;
          }
        }
      }
      if (operation == "delete") {
        return true;
      } else if (operation == "select") {
        if (predicate) {
          return predicate(doc);
        }
        return doc.toJSON();
      }
      if (!predicate) {
        throw new Error("Update predicate is undefined");
      }
      predicate(doc);
      return doc.serialize();
    }

    if (this.whereFilter) {
      const { fieldName, ranges } = this.whereFilter;
      let ids: KeyType[];
      if (fieldName == this.table.primaryKey) {
        if (operation == "select") {
          return Object.values(this.mainDict.filterSelect(ranges as any, limit, execfn));
        } else if (operation == "update") {
          this.mainDict.editRanges(ranges as any, execfn, limit);
          return;
        } else {
          return this.mainDict.removeRanges(ranges as any, execfn, limit);
        }
      } else {
        const rec = this.indices[fieldName].filterSelect(ranges, 0);

        if (this.table.fieldHasAnyTag(fieldName, "unique")) {
          ids = Object.values(rec);
        } else {
          ids = uniq(flatten(Object.values(rec)));
        }

        if (operation == "select") {
          return Object.values(this.mainDict.filterSelect(ids.map(id => [id, id]), limit, execfn));
        } else if (operation == "update") {
          this.mainDict.editRanges(ids.map(id => [id, id]), execfn, limit);
          return;
        } else {
          return this.mainDict.removeRanges(ids.map(id => [id, id]), execfn, limit);
        }
      }
    }

    if (operation == "select") {
      return Object.values(this.mainDict.filterSelect([[undefined, undefined]], limit, execfn));
    } else if (operation == "update") {
      this.mainDict.editRanges([[undefined, undefined]], execfn, limit);
      return;
    } else {
      return this.mainDict.removeRanges([[undefined, undefined]],)
    }

  }

  select(limit?: number): Type[]
  select<ReturnType>(limit: number, predicate?: DocCallback<KeyType, Type, ReturnType>): ReturnType[]
  select<ReturnType = Type>(limit = 100, predicate?: DocCallback<KeyType, Type, ReturnType>): (ReturnType | Type)[] {
    return this.exec(limit, "select", predicate);
  }

  update(limit = 0, predicate: DocCallback<KeyType, Type, void>): void {
    this.exec(limit, "update", predicate);
  }

  delete(limit = 0) {
    return this.exec(limit, "delete");
  }
}