export const KONRO_ERROR_CODES = {
  // General Errors
  E001: 'An unexpected error occurred: {{details}}',

  // Storage Errors
  E100: 'Invalid storage strategy configuration.',
  E101: "The '{{format}}' format requires the '{{dependency}}' package to be installed. Please run 'npm install {{dependency}}'.",
  E102: 'Unsupported or invalid format specified: {{format}}.',
  E103: 'Failed to parse file at "{{filepath}}". It may be corrupt or not a valid {{format}} file. Original error: {{details}}',
  E104: "The 'on-demand' mode requires the 'multi-file' or 'per-record' storage strategy.",
  E105: `The 'per-record' strategy only supports 'json' or 'yaml' formats.`,
  E106: `The '{{format}}' format only supports 'on-demand' mode with a 'multi-file' strategy.`,
  E107: `Invalid file adapter options: missing storage strategy.`,

  // Schema & Data Errors
  E200: 'Table "{{tableName}}" does not exist in the database state.',
  E201: 'Schema for table "{{tableName}}" not found.',
  E202: `Table "{{tableName}}" must have an 'id' column for 'per-record' storage.`,
  E203: 'Aggregation `{{aggType}}` requires a column.',

  // Validation Errors
  E300: `Validation Error: Value '{{value}}' for column '{{columnName}}' must be unique.`,
  E301: `Validation Error: Missing required value for column '{{columnName}}'.`,
  E302: `Validation Error: Invalid value for column '{{columnName}}'. Expected {{expected}}, got {{got}}.`,
  E303: `Value '{{value}}' for column '{{columnName}}' is not a valid email.`,
  E304: `Number {{value}} for column '{{columnName}}' is too small (min: {{min}}).`,
  E305: `Number {{value}} for column '{{columnName}}' is too large (max: {{max}}).`,

  // DB Context Errors
  E400: "The method '{{methodName}}' is not supported in 'on-demand' mode.",
};

export type KonroErrorCode = keyof typeof KONRO_ERROR_CODES;