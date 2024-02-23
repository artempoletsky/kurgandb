import type { PlainObject } from "./globals";
import type { TableScheme } from "./table";


export function formToDocument(form: HTMLFormElement, scheme: TableScheme): PlainObject {
  const result: PlainObject = {};
  const formData = new FormData(form);

  for (const fieldName in scheme.fields) {
    const type = scheme.fields[fieldName];
    const value = formData.get(fieldName)?.toString() || "";
    if (type == "number") {
      result[fieldName] = parseFloat(value);
    } if (type == "json") {
      result[fieldName] = JSON.parse(value);
    } else {
      result[fieldName] = value;
    }
  }
  return result;
}