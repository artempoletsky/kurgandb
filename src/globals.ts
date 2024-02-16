export const FieldTypes = ["string", "number", "date", "boolean", "json"] as const;

export type FieldType = typeof FieldTypes[number];

export const FieldTags = ["primary", "unique", "index", "memory", "textarea", "heavy", "hidden", "autoinc"] as const;

export type FieldTag = typeof FieldTags[number];