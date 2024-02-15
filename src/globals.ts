export const FieldTypes = ["string", "number", "date", "boolean", "json"] as const;

export type FieldType = typeof FieldTypes[number];