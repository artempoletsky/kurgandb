import { PlainObject } from "./utils";

export const LightTypes = ["string", "number", "Date"] as const;
export const HeavyTypes = ["Text", "File"] as const;
export type LightType = typeof LightTypes[number];
export type HeavyType = typeof HeavyTypes[number];
export type DocType = LightType | HeavyType;


export type DocumentScheme = {
  fields: Record<string, DocType>
  settings: Record<string, any>
};
export interface IDocument extends Record<string, any> {
  toJSON: () => PlainObject
}

export class Document implements IDocument {
  protected _scheme: DocumentScheme;
  constructor(obj: PlainObject, scheme: DocumentScheme, id: number) {
    this._scheme = scheme;
    return new Proxy(obj, {
      set: (target: PlainObject, key: string, value: any) => {
        const type = scheme.fields[key];

        if (!type || type in LightTypes) {// if not type extend object normally
          target[key] = value;
        } else {
          throw new Error("not implemented yet");
        }
        return true;
      },
      get: (target: PlainObject, key: string, value: any) => {
        const type = scheme.fields[key];
        if (!type) {

        }
        if (type in LightTypes) {
          target[key] = value;
        } else {
          throw new Error("not implemented yet");
        }
        return true;
      }
    }) as Document;
  }

  public toJSON() {
    const result: PlainObject = {};
    for (const key in this._scheme.fields) {
      const type = this._scheme.fields[key];
      if (type in HeavyTypes) continue;
      result[key] = (this as PlainObject)[key];
    }
    return result;
  }
};