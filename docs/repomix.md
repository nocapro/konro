# Directory Structure
```
src/
  utils/
    constants.ts
    error.codes.ts
    error.util.ts
    predicate.util.ts
    serializer.util.ts
  adapter.ts
  db.ts
  fs.ts
  index.ts
  operations.ts
  schema.ts
  types.ts
test/
  e2e/
    ErrorAndEdgeCases/
      Pagination.test.ts
      Transaction.test.ts
    MultiFileYaml/
      FullLifecycle.test.ts
    Operations/
      Aggregate.test.ts
      Delete.test.ts
      Insert.test.ts
      Query-With.test.ts
      Query.test.ts
      Update.test.ts
    SingleFileJson/
      FullLifecycle.test.ts
  integration/
    Adapters/
      MultiFileYaml.test.ts
      OnDemand.test.ts
      PerRecord.test.ts
      Read.test.ts
      SingleFileJson.test.ts
    DBContext/
      Initialization.test.ts
    InMemoryFlow/
      CrudCycle.test.ts
    Types/
      InferredTypes.test-d.ts
  unit/
    Core/
      Aggregate.test.ts
      Delete.test.ts
      Insert.test.ts
      Query-With.test.ts
      Query.test.ts
      Update.test.ts
    Schema/
      ColumnHelpers.test.ts
      CreateSchema.test.ts
      RelationHelpers.test.ts
    Validation/
      Constraints.test.ts
  konro-test-import.ts
  util.ts
package.json
tsconfig.build.json
tsconfig.json
```

# Files

## File: src/utils/constants.ts
```typescript
export const TEMP_FILE_SUFFIX = '.tmp';
```

## File: src/utils/error.codes.ts
```typescript
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
  E203: 'Record ID is undefined or null in table "{{tableName}}". {{details}}',

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
```

## File: src/utils/error.util.ts
```typescript
import { KONRO_ERROR_CODES, type KonroErrorCode } from './error.codes';

type ErrorContext = Record<string, string | number | undefined | null>;

const renderTemplate = (template: string, context: ErrorContext): string => {
  return template.replace(/\{\{([^}]+)\}\}/g, (_match, key) => {
    const value = context[key.trim()];
    return value !== undefined && value !== null ? String(value) : `{{${key}}}`;
  });
};

// Per user request: no classes. Using constructor functions for errors.
const createKonroError = (name: string) => {
  function KonroErrorConstructor(messageOrContext: string | ({ code: KonroErrorCode } & ErrorContext)) {
    let message: string;
    let code: KonroErrorCode | undefined;

    if (typeof messageOrContext === 'string') {
      message = messageOrContext;
    } else {
      code = messageOrContext.code;
      const template = KONRO_ERROR_CODES[code] || 'Unknown error code.';
      message = `[${code}] ${renderTemplate(template, messageOrContext)}`;
    }

    const error = new Error(message) as Error & { code?: KonroErrorCode };
    error.name = name;
    error.code = code;
    Object.setPrototypeOf(error, KonroErrorConstructor.prototype);
    return error;
  }
  Object.setPrototypeOf(KonroErrorConstructor.prototype, Error.prototype);
  return KonroErrorConstructor;
};

/** Base constructor for all Konro-specific errors. */
export const KonroError = createKonroError('KonroError');

/** Thrown for storage adapter-related issues. */
export const KonroStorageError = createKonroError('KonroStorageError');

/** Thrown for schema validation errors. */
export const KonroValidationError = createKonroError('KonroValidationError');

/** Thrown when a resource is not found. */
export const KonroNotFoundError = createKonroError('KonroNotFoundError');
```

## File: src/utils/predicate.util.ts
```typescript
import { KRecord } from '../types';

/** Creates a predicate function from a partial object for equality checks, avoiding internal casts. */
export const createPredicateFromPartial = <T extends KRecord>(partial: Partial<T>): ((record: T) => boolean) => {
  // `Object.keys` is cast because TypeScript types it as `string[]` instead of `(keyof T)[]`.
  const keys = Object.keys(partial) as (keyof T)[];
  return (record: T): boolean => keys.every(key => record[key] === partial[key]);
};
```

## File: src/utils/serializer.util.ts
```typescript
import { KonroStorageError } from './error.util';
import type { ColumnDefinition, Serializer } from '../types';

const loadOptional = <T>(name: string): T | undefined => {
  try {
    return require(name);
  } catch {
    return undefined;
  }
};

const yaml = loadOptional<{ load: (str: string) => unknown; dump: (obj: any, options?: any) => string }>('js-yaml');
const papaparse = loadOptional<{ parse: (str: string, config?: any) => { data: any[] }; unparse: (data: any[] | object) => string; }>('papaparse');
const xlsx = loadOptional<{ read: (data: any, opts: any) => any; utils: { sheet_to_json: <T>(ws: any) => T[]; json_to_sheet: (json: any) => any; book_new: () => any; book_append_sheet: (wb: any, ws: any, name: string) => void; }; write: (wb: any, opts: any) => any; }>('xlsx');

/** For tabular formats (CSV/XLSX), metadata isn't stored. We derive lastId from the data itself. */
const deriveLastIdFromRecords = (records: any[], tableSchema: Record<string, ColumnDefinition<any>>): number => {
  const idColumn = Object.keys(tableSchema).find((key) => tableSchema[key]?.dataType === 'id' && tableSchema[key]?.options?._pk_strategy !== 'uuid');
  if (!idColumn) return 0;

  return records.reduce((maxId: number, record: any) => {
    const id = record[idColumn];
    return typeof id === 'number' && id > maxId ? id : maxId;
  }, 0);
};

export const getSerializer = (format: 'json' | 'yaml' | 'csv' | 'xlsx'): Serializer => {
  switch (format) {
    case 'json':
      return {
        parse: <T>(data: string): T => JSON.parse(data),
        stringify: (obj: any): string => JSON.stringify(obj, null, 2),
      };
    case 'yaml':
      if (!yaml) throw KonroStorageError({ code: 'E101', format: 'yaml', dependency: 'js-yaml' });
      return {
        parse: <T>(data: string): T => yaml.load(data) as T,
        stringify: (obj: any): string => yaml.dump(obj),
      };
    case 'csv':
      if (!papaparse) throw KonroStorageError({ code: 'E101', format: 'csv', dependency: 'papaparse' });
      return {
        parse: <T>(data: string, tableSchema?: Record<string, ColumnDefinition<any>>): T => {
          const { data: records } = papaparse.parse(data, { header: true, dynamicTyping: true, skipEmptyLines: true });
          const lastId = tableSchema ? deriveLastIdFromRecords(records, tableSchema) : 0;
          return { records, meta: { lastId } } as T;
        },
        stringify: (obj: any): string => papaparse.unparse(obj.records || []),
      };
    case 'xlsx':
      if (!xlsx) throw KonroStorageError({ code: 'E101', format: 'xlsx', dependency: 'xlsx' });
      return {
        parse: <T>(data: string, tableSchema?: Record<string, ColumnDefinition<any>>): T => {
          const workbook = xlsx.read(data, { type: 'base64' });
          const sheetName = workbook.SheetNames[0];
          if (!sheetName) return { records: [], meta: { lastId: 0 } } as T;
          const worksheet = workbook.Sheets[sheetName];
          const records = xlsx.utils.sheet_to_json(worksheet);
          const lastId = tableSchema ? deriveLastIdFromRecords(records, tableSchema) : 0;
          return { records, meta: { lastId } } as T;
        },
        stringify: (obj: any): string => {
          const worksheet = xlsx.utils.json_to_sheet(obj.records || []);
          const workbook = xlsx.utils.book_new();
          xlsx.utils.book_append_sheet(workbook, worksheet, 'data');
          return xlsx.write(workbook, { bookType: 'xlsx', type: 'base64' });
        },
      };
    default:
      throw KonroStorageError({ code: 'E102', format });
  }
};
```

## File: src/adapter.ts
```typescript
import path from 'path';
import type {
  DatabaseState,
  KRecord,
  TableState,
  StorageAdapter,
  FileStorageAdapter,
  FileAdapterOptions,
  ColumnDefinition,
  SingleFileStrategy,
  MultiFileStrategy,
  PerRecordStrategy,
  KonroSchema,
  Serializer,
  FsProvider,
} from './types';
import { createEmptyState } from './operations';
import { getSerializer } from './utils/serializer.util';
import { defaultFsProvider, writeAtomic } from './fs';
import { KonroError, KonroStorageError } from './utils/error.util';
import { TEMP_FILE_SUFFIX } from './utils/constants';

export function createFileAdapter(options: FileAdapterOptions & { mode: 'on-demand' }): FileStorageAdapter & { mode: 'on-demand' };
export function createFileAdapter(options: FileAdapterOptions & { mode?: 'in-memory' | undefined }): FileStorageAdapter & { mode: 'in-memory' };
export function createFileAdapter(options: FileAdapterOptions): FileStorageAdapter;
export function createFileAdapter(options: FileAdapterOptions): FileStorageAdapter {
  const serializer = getSerializer(options.format);
  const fileExtension = `.${options.format}`;
  const fs = options.fs ?? defaultFsProvider;
  const mode = options.mode ?? 'in-memory';

  if (options.perRecord && options.format !== 'json' && options.format !== 'yaml') {
    throw KonroError({ code: 'E105' });
  }

  const isTabular = options.format === 'csv' || options.format === 'xlsx';
  if (isTabular && (mode !== 'on-demand' || !options.multi)) {
    throw KonroError({ code: 'E106', format: options.format });
  }

  if (mode === 'on-demand' && options.single) {
    throw KonroError({ code: 'E104' });
  }

  const strategy = createStrategy(options, { fs, serializer, fileExtension, mode });

  return {
    options,
    fs,
    serializer,
    fileExtension,
    mode,
    ...strategy,
  } as FileStorageAdapter;
}

type FileStrategy = Pick<StorageAdapter, 'read' | 'write'>;
type StrategyContext = {
  fs: FsProvider;
  serializer: Serializer;
  fileExtension: string;
  mode: 'in-memory' | 'on-demand';
};

/** Chooses and creates the appropriate file strategy based on adapter options. */
function createStrategy(options: FileAdapterOptions, context: StrategyContext): FileStrategy {
  if (options.single) {
    return createSingleFileStrategy(options.single, context);
  }
  if (options.multi) {
    return createMultiFileStrategy(options.multi, context);
  }
  if (options.perRecord) {
    return createPerRecordStrategy(options.perRecord, context);
  }
  // This case should be prevented by the types, but as a safeguard:
  throw KonroError({ code: 'E107' });
}

/** Creates the strategy for reading/writing the entire database to a single file. */
function createSingleFileStrategy(options: SingleFileStrategy['single'], context: StrategyContext): FileStrategy {
  const { fs, serializer } = context;

  const parseFile = async <T>(filepath: string, schema?: Record<string, ColumnDefinition<unknown>>): Promise<T | undefined> => {
    const data = await fs.readFile(filepath);
    if (!data) return undefined;
    try {
      return serializer.parse<T>(data, schema);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      throw KonroStorageError({ code: 'E103', filepath, format: context.fileExtension.slice(1), details: message });
    }
  };

  return {
    read: async <S extends KonroSchema<any, any>>(schema: S) => {
      // We parse into a generic DatabaseState because the exact type is only known by the caller.
      const state = await parseFile<DatabaseState>(options.filepath);
      return (state ?? createEmptyState(schema)) as DatabaseState<S>;
    },
    write: (state: DatabaseState<any>) => writeAtomic(options.filepath, serializer.stringify(state), fs),
  };
}

/** Creates the strategy for reading/writing each table to its own file in a directory. */
function createMultiFileStrategy(options: MultiFileStrategy['multi'], context: StrategyContext): FileStrategy {
  const { fs, serializer, fileExtension } = context;
  const parseFile = async <T>(filepath: string, schema?: Record<string, ColumnDefinition<unknown>>): Promise<T | undefined> => {
    const data = await fs.readFile(filepath);
    if (!data) return undefined;
    try {
      return serializer.parse<T>(data, schema);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      throw KonroStorageError({ code: 'E103', filepath, format: fileExtension.slice(1), details: message });
    }
  };

  return {
    read: async <S extends KonroSchema<any, any>>(schema: S) => {
      await context.fs.mkdir(options.dir, { recursive: true });
      const state = createEmptyState(schema);
      await Promise.all(
        Object.keys(schema.tables).map(async (tableName) => {
          const filepath = path.join(options.dir, `${tableName}${context.fileExtension}`);
          const tableState = await parseFile<TableState>(filepath, schema.tables[tableName]);
          if (tableState) (state as any)[tableName] = tableState;
        })
      );
      return state;
    },
    write: async (state: DatabaseState<any>) => {
      await context.fs.mkdir(options.dir, { recursive: true });
      const writes = Object.entries(state).map(([tableName, tableState]) => {
        const filepath = path.join(options.dir, `${tableName}${context.fileExtension}`);
        return writeAtomic(filepath, context.serializer.stringify(tableState), context.fs);
      });
      await Promise.all(writes);
    },
  };
}

/** Creates the strategy for reading/writing each record to its own file. */
function createPerRecordStrategy(options: PerRecordStrategy['perRecord'], context: StrategyContext): FileStrategy {
  const { fs, serializer, fileExtension } = context;

  const parseFile = async <T>(filepath: string): Promise<T | undefined> => {
    const data = await fs.readFile(filepath);
    if (!data) return undefined;
    try {
      return serializer.parse<T>(data);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      throw KonroStorageError({ code: 'E103', filepath, format: context.fileExtension.slice(1), details: message });
    }
  };

  return {
    read: async <S extends KonroSchema<any, any>>(schema: S) => {
      await fs.mkdir(options.dir, { recursive: true });
      const state = createEmptyState(schema);
      await Promise.all(
        Object.keys(schema.tables).map(async (tableName) => {
          const tableDir = path.join(options.dir, tableName);
          const currentTableState = state[tableName as keyof typeof state];
          if (!currentTableState) return;

          await fs.mkdir(tableDir, { recursive: true });

          const metaContent = await fs.readFile(path.join(tableDir, '_meta.json')).catch(() => null);
          if (metaContent) currentTableState.meta = JSON.parse(metaContent);

          const files = await fs.readdir(tableDir);
          const recordFiles = files.filter((f) => !f.startsWith('_meta'));
          const records = (await Promise.all(recordFiles.map((file) => parseFile<KRecord>(path.join(tableDir, file))))).filter((r): r is KRecord => r != null);
          currentTableState.records = records as any;

          if (currentTableState.meta.lastId === 0) {
            const idColumn = Object.keys(schema.tables[tableName]).find((k) => schema.tables[tableName][k]?.options?._pk_strategy === 'auto-increment');
            if (idColumn) {
              currentTableState.meta.lastId = records.reduce((maxId: number, record: KRecord) => {
                const id = record[idColumn];
                return typeof id === 'number' && id > maxId ? id : maxId;
              }, 0);
            }
          }
        })
      );
      return state;
    },
    write: async (state: DatabaseState<any>, schema: KonroSchema<any, any>) => {
      await fs.mkdir(options.dir, { recursive: true });
      await Promise.all(Object.entries(state).map(async ([tableName, tableState]) => {
        const tableDir = path.join(options.dir, tableName as string);
        await fs.mkdir(tableDir, { recursive: true });
        await writeAtomic(path.join(tableDir, '_meta.json'), JSON.stringify(tableState.meta, null, 2), fs);

        const idColumn = Object.keys(schema.tables[tableName]).find((k) => schema.tables[tableName][k]?.dataType === 'id');
        if (!idColumn) throw KonroError({ code: 'E202', tableName });

        const currentFiles = new Set(tableState.records.map((r: KRecord) => {
          const idValue = r[idColumn];
          if (!idValue) {
            throw KonroError({ code: 'E203', tableName, details: 'Record ID is undefined or null' });
          }
          return `${idValue}${fileExtension}`;
        }));
        const existingFiles = (await fs.readdir(tableDir)).filter(f => !f.startsWith('_meta') && !f.endsWith(TEMP_FILE_SUFFIX));

        const recordWrites = tableState.records.map((r) => {
          const idValue = r[idColumn];
          if (!idValue) {
            throw KonroError({ code: 'E203', tableName, details: 'Record ID is undefined or null' });
          }
          return writeAtomic(path.join(tableDir, `${idValue}${fileExtension}`), serializer.stringify(r), fs);
        });
        const recordDeletes = existingFiles.filter(f => !currentFiles.has(f)).map(f => fs.unlink(path.join(tableDir, f as string)));
        await Promise.all([...recordWrites, ...recordDeletes]);
      }));
    }
  };
}
```

## File: src/db.ts
```typescript
import path from 'path';
import type {
  AggregationDefinition,
  KonroSchema,
  StorageAdapter,
  FileStorageAdapter,
  DatabaseState,
  KRecord,
  TableState,
  QueryDescriptor,
  AggregationDescriptor,
  WithArgument,
  ResolveWith,
  ChainedQueryBuilder,
  QueryBuilder,
  UpdateBuilder,
  DeleteBuilder,
  InMemoryDbContext,
  OnDemandChainedQueryBuilder,
  OnDemandQueryBuilder,
  OnDemandUpdateBuilder,
  OnDemandDeleteBuilder,
  OnDemandDbContext,
  DbContext,
} from './types';
import {
  _queryImpl,
  _insertImpl,
  _updateImpl,
  _deleteImpl,
  createEmptyState as createEmptyStateImpl,
  _aggregateImpl,
} from './operations';
import { createPredicateFromPartial } from './utils/predicate.util';
import { KonroError, KonroStorageError } from './utils/error.util';
import { writeAtomic } from './fs';

export type { InMemoryDbContext, OnDemandDbContext, DbContext };

// --- CORE LOGIC (STATELESS & PURE) ---

/**
 * A helper to normalize a predicate argument into a function.
 */
const normalizePredicate = <T extends KRecord>(
  predicate: Partial<T> | ((record: T) => boolean)
): ((record: KRecord) => boolean) =>
  // The cast is necessary due to function argument contravariance.
  // The internal operations work on the wider `KRecord`, while the fluent API provides the specific `T`.
  (typeof predicate === 'function' ? predicate : createPredicateFromPartial(predicate)) as (record: KRecord) => boolean;

/**
 * Creates the core, stateless database operations.
 * These operations are pure functions that take a database state and return a new state,
 * forming the foundation for both in-memory and on-demand modes.
 */
function createCoreDbContext<S extends KonroSchema<any, any>>(schema: S) {
  const query = (state: DatabaseState<S>): QueryBuilder<S> => ({
    from: <TName extends keyof S['tables']>(tableName: TName): ChainedQueryBuilder<S, TName, S['base'][TName]> => {
      const createBuilder = <TReturn>(currentDescriptor: QueryDescriptor): ChainedQueryBuilder<S, TName, TReturn> => ({
        select(fields) { return createBuilder<TReturn>({ ...currentDescriptor, select: fields as QueryDescriptor['select'] }); },
        where(predicate) { return createBuilder<TReturn>({ ...currentDescriptor, where: normalizePredicate(predicate) }); },
        withDeleted() { return createBuilder<TReturn>({ ...currentDescriptor, withDeleted: true }); },
        with<W extends WithArgument<S, TName>>(relations: W) {
          const newWith = { ...currentDescriptor.with, ...(relations as QueryDescriptor['with']) };
          return createBuilder<TReturn & ResolveWith<S, TName, W>>({ ...currentDescriptor, with: newWith });
        },
        limit(count: number) { return createBuilder<TReturn>({ ...currentDescriptor, limit: count }); },
        offset(count: number) { return createBuilder<TReturn>({ ...currentDescriptor, offset: count }); },
        all: (): TReturn[] => _queryImpl(state as DatabaseState, schema, currentDescriptor) as TReturn[],
        first: (): TReturn | null => (_queryImpl(state as DatabaseState, schema, { ...currentDescriptor, limit: 1 })[0] ?? null) as TReturn | null,
        aggregate: <TAggs extends Record<string, AggregationDefinition>>(aggregations: TAggs) => {
          const aggDescriptor: AggregationDescriptor = { ...currentDescriptor, aggregations };
          return _aggregateImpl(state as DatabaseState, schema, aggDescriptor) as { [K in keyof TAggs]: number | null };
        },
      });
      return createBuilder<S['base'][TName]>({ tableName: tableName as string });
    },
  });

  const insert = <T extends keyof S['tables']>(
    state: DatabaseState<S>, tableName: T, values: S['create'][T] | Readonly<S['create'][T]>[]
  ): [DatabaseState<S>, S['base'][T] | S['base'][T][]] => {
    const valsArray = Array.isArray(values) ? values : [values];
    const [newState, inserted] = _insertImpl(state as DatabaseState, schema, tableName as string, valsArray as KRecord[]);
    const result = Array.isArray(values) ? inserted : inserted[0];
    return [newState as DatabaseState<S>, result] as [DatabaseState<S>, S['base'][T] | S['base'][T][]];
  };

  const update = <T extends keyof S['tables']>(state: DatabaseState<S>, tableName: T): UpdateBuilder<S, S['base'][T], S['create'][T]> => ({
    set: (data: Partial<S['create'][T]>) => ({
      where: (predicate: Partial<S['base'][T]> | ((record: S['base'][T]) => boolean)): [DatabaseState<S>, S['base'][T][]] => {
        const [newState, updatedRecords] = _updateImpl(state as DatabaseState, schema, tableName as string, data as Partial<KRecord>, normalizePredicate(predicate));
        return [newState as DatabaseState<S>, updatedRecords as S['base'][T][]];
      },
    }),
  });

  const del = <T extends keyof S['tables']>(state: DatabaseState<S>, tableName: T): DeleteBuilder<S, S['base'][T]> => ({
    where: (predicate: Partial<S['base'][T]> | ((record: S['base'][T]) => boolean)): [DatabaseState<S>, S['base'][T][]] => {
      const [newState, deletedRecords] = _deleteImpl(state as DatabaseState, schema, tableName as string, normalizePredicate(predicate));
      return [newState as DatabaseState<S>, deletedRecords as S['base'][T][]];
    },
  });

  return { query, insert, update, delete: del };
}

// --- ON-DEMAND CONTEXT (STATEFUL WRAPPER) ---

type CoreDbContext<S extends KonroSchema<any, any>> = ReturnType<typeof createCoreDbContext<S>>;

/** Defines the contract for file I/O operations in on-demand mode. */
interface OnDemandIO<S extends KonroSchema<any, any>> {
  getFullState(): Promise<DatabaseState<S>>;
  insert(core: CoreDbContext<S>, tableName: string, values: any): Promise<any>;
  update(core: CoreDbContext<S>, tableName: string, data: Partial<unknown>, predicate: (record: KRecord) => boolean): Promise<KRecord[]>;
  delete(core: CoreDbContext<S>, tableName: string, predicate: (record: KRecord) => boolean): Promise<KRecord[]>;
}

/**
 * Creates a generic, unified `OnDemandDbContext` from an I/O strategy.
 * This function is the key to removing duplication between 'multi-file' and 'per-record' modes.
 */
function createOnDemandDbContext<S extends KonroSchema<any, any>>(
  schema: S,
  adapter: StorageAdapter,
  core: CoreDbContext<S>,
  io: OnDemandIO<S>
): OnDemandDbContext<S> {
  const query = (): OnDemandQueryBuilder<S> => ({
    from: <TName extends keyof S['tables']>(tableName: TName): OnDemandChainedQueryBuilder<S, TName, S['base'][TName]> => {
      const createBuilder = <TReturn>(currentDescriptor: QueryDescriptor): OnDemandChainedQueryBuilder<S, TName, TReturn> => ({
        select(fields) { return createBuilder<TReturn>({ ...currentDescriptor, select: fields as QueryDescriptor['select'] }); },
        where(predicate) { return createBuilder<TReturn>({ ...currentDescriptor, where: normalizePredicate(predicate) }); },
        withDeleted() { return createBuilder<TReturn>({ ...currentDescriptor, withDeleted: true }); },
        with<W extends WithArgument<S, TName>>(relations: W) {
          const newWith = { ...currentDescriptor.with, ...(relations as QueryDescriptor['with']) };
          return createBuilder<TReturn & ResolveWith<S, TName, W>>({ ...currentDescriptor, with: newWith });
        },
        limit(count: number) { return createBuilder<TReturn>({ ...currentDescriptor, limit: count }); },
        offset(count: number) { return createBuilder<TReturn>({ ...currentDescriptor, offset: count }); },
        all: async (): Promise<TReturn[]> => {
          const state = await io.getFullState();
          return _queryImpl(state, schema, currentDescriptor) as TReturn[];
        },
        first: async (): Promise<TReturn | null> => {
          const state = await io.getFullState();
          return (_queryImpl(state, schema, { ...currentDescriptor, limit: 1 })[0] ?? null) as TReturn | null;
        },
        aggregate: async <TAggs extends Record<string, AggregationDefinition>>(aggregations: TAggs) => {
          const state = await io.getFullState();
          const aggDescriptor: AggregationDescriptor = { ...currentDescriptor, aggregations };
          return _aggregateImpl(state, schema, aggDescriptor) as { [K in keyof TAggs]: number | null };
        },
      });
      return createBuilder<S['base'][TName]>({ tableName: tableName as string });
    },
  });

  const insert = <T extends keyof S['tables']>(tableName: T, values: S['create'][T] | Readonly<S['create'][T]>[]): Promise<any> =>
    io.insert(core, tableName as string, values);

  const update = <T extends keyof S['tables']>(tableName: T): OnDemandUpdateBuilder<S['base'][T], S['create'][T]> => ({
    set: (data: Partial<S['create'][T]>) => ({
      where: (predicate: Partial<S['base'][T]> | ((record: S['base'][T]) => boolean)) => io.update(core, tableName as string, data, normalizePredicate(predicate)) as Promise<S['base'][T][]>,
    }),
  });

  const del = <T extends keyof S['tables']>(tableName: T): OnDemandDeleteBuilder<S['base'][T]> => ({
    where: (predicate: Partial<S['base'][T]> | ((record: S['base'][T]) => boolean)) => io.delete(core, tableName as string, normalizePredicate(predicate)) as Promise<S['base'][T][]>,
  });

  const notSupported = (methodName: string) => () => Promise.reject(KonroError({ code: 'E400', methodName }));

  return {
    schema,
    adapter,
    createEmptyState: () => createEmptyStateImpl(schema),
    read: notSupported('read'),
    write: notSupported('write'),
    query,
    insert,
    update,
    delete: del
  };
}


// --- DATABASE FACTORY ---

export function createDatabase<
  S extends KonroSchema<any, any>,
  TAdapter extends StorageAdapter,
>(
  options: { schema: S; adapter: TAdapter }
): TAdapter['mode'] extends 'on-demand' ? OnDemandDbContext<S> : InMemoryDbContext<S>;
export function createDatabase<S extends KonroSchema<any, any>>(
  options: { schema: S; adapter: StorageAdapter }
): DbContext<S> {
  const { schema, adapter } = options;
  const core = createCoreDbContext(schema);

  // --- In-Memory Mode ---
  if (adapter.mode === 'in-memory') {
    return {
      ...core,
      schema, adapter,
      read: () => adapter.read(schema),
      write: (state) => adapter.write(state, schema),
      createEmptyState: () => createEmptyStateImpl(schema),
    } as InMemoryDbContext<S>;
  }

  // --- On-Demand Mode ---
  const fileAdapter = adapter as FileStorageAdapter; // We can be sure it's a FileStorageAdapter due to checks
  const { fs, serializer, fileExtension } = fileAdapter;

  // The `read` method from the adapter provides the canonical way to get the full state.
  const getFullState = (): Promise<DatabaseState<S>> => adapter.read(schema);

  // --- I/O Strategy for Multi-File ---
  const createMultiFileIO = (): OnDemandIO<S> => {
    const { dir } = fileAdapter.options.multi!;
    const getTablePath = (tableName: string) => path.join(dir, `${tableName}${fileExtension}`);

    const readTableState = async (tableName: string): Promise<TableState> => {
      const data = await fs.readFile(getTablePath(tableName));
      if (!data) return { records: [], meta: { lastId: 0 } };
      try {
        return serializer.parse(data, schema.tables[tableName]);
      } catch (e: any) {
        throw KonroStorageError({ code: 'E103', filepath: getTablePath(tableName), format: fileExtension.slice(1), details: e.message });
      }
    };

    const writeTableState = async (tableName: string, tableState: TableState): Promise<void> => {
      await fs.mkdir(dir, { recursive: true });
      await writeAtomic(getTablePath(tableName), serializer.stringify(tableState), fs);
    };

    return {
      getFullState,
      insert: async (core, tableName, values) => {
        const state = createEmptyStateImpl(schema);
        (state as any)[tableName] = await readTableState(tableName);
        const [newState, result] = core.insert(state, tableName as keyof S['tables'], values as any);
        await writeTableState(tableName, newState[tableName]!);
        return result;
      },
      update: async (core, tableName, data, predicate) => {
        const state = createEmptyStateImpl(schema);
        (state as any)[tableName] = await readTableState(tableName);
        const [newState, result] = core.update(state, tableName as keyof S['tables']).set(data).where(predicate as any);
        await writeTableState(tableName, newState[tableName]!);
        return result;
      },
      delete: async (core, tableName, predicate) => {
        const state = createEmptyStateImpl(schema);
        (state as any)[tableName] = await readTableState(tableName);
        const [newState, result] = core.delete(state, tableName as keyof S['tables']).where(predicate as any);
        await writeTableState(tableName, newState[tableName]!);
        return result;
      }
    };
  };

  // --- I/O Strategy for Per-Record ---
  const createPerRecordIO = (): OnDemandIO<S> => {
    const { dir } = fileAdapter.options.perRecord!;
    const getTableDir = (tableName: string) => path.join(dir, tableName);
    const getRecordPath = (tableName: string, id: any) => path.join(getTableDir(tableName), `${id}${fileExtension}`);
    const getMetaPath = (tableName: string) => path.join(getTableDir(tableName), '_meta.json');
    const getIdColumn = (tableName: string) => {
      const idCol = Object.keys(schema.tables[tableName]).find((k) => schema.tables[tableName][k]?.options?._pk_strategy === 'auto-increment' || schema.tables[tableName][k]?.dataType === 'id');
      if (!idCol) throw KonroError({ code: 'E202', tableName });
      return idCol;
    };

    // const writeTableState = async (tableName: string, tableState: TableState, idColumn: string): Promise<void> => {
    //   const tableDir = getTableDir(tableName);
    //   await fs.mkdir(tableDir, { recursive: true });
    //   await writeAtomic(getMetaPath(tableName), JSON.stringify(tableState.meta, null, 2), fs);

    //   const currentFiles = new Set(tableState.records.map((r) => `${(r as KRecord)[idColumn]}${fileExtension}`));
    //   const existingFiles = (await fs.readdir(tableDir)).filter(f => !f.startsWith('_meta') && !f.endsWith(TEMP_FILE_SUFFIX));

    //   const recordWrites = tableState.records.map((r) =>
    //     writeAtomic(getRecordPath(tableName, (r as KRecord)[idColumn]), serializer.stringify(r), fs)
    //   );
    //   const recordDeletes = existingFiles.filter(f => !currentFiles.has(f)).map(f =>
    //     fs.unlink(path.join(tableDir, f as string))
    //   );
    //   await Promise.all([...recordWrites, ...recordDeletes]);
    // };

    /*
    const readTableState = async (tableName: string): Promise<TableState> => {
      const tableDir = getTableDir(tableName);
      await fs.mkdir(tableDir, { recursive: true });

      const metaPath = getMetaPath(tableName);
      const metaContent = await fs.readFile(metaPath).catch(() => null);
      const meta = metaContent ? JSON.parse(metaContent) : { lastId: 0 };

      const files = await fs.readdir(tableDir);
      const recordFiles = files.filter((f) => !f.startsWith('_meta') && !f.endsWith(TEMP_FILE_SUFFIX));

      const records = (
        await Promise.all(recordFiles.map(async (file) => {
          const data = await fs.readFile(path.join(tableDir, file));
          if (!data) return null;
          try {
            return serializer.parse<KRecord>(data);
          } catch (e: any) {
            throw KonroStorageError({ code: 'E103', filepath: path.join(tableDir, file), format: fileExtension.slice(1), details: e.message });
          }
        }))
      ).filter((r): r is KRecord => r != null);

      if (meta.lastId === 0) {
        const idCol = getIdColumn(tableName);
        if (idCol) {
          meta.lastId = records.reduce((maxId: number, record: KRecord) => {
            const id = record[idCol];
            return typeof id === 'number' && id > maxId ? id : maxId;
          }, 0);
        }
      }

      return { meta, records: records as any[] };
    };
    */

    return {
      getFullState,
      insert: async (core, tableName, values) => {
        const idColumn = getIdColumn(tableName);
        const metaPath = getMetaPath(tableName);
        const metaContent = await fs.readFile(metaPath).catch(() => null);
        const meta = metaContent ? JSON.parse(metaContent) : { lastId: 0 };
        const state = {
          [tableName]: { meta, records: [] },
        } as unknown as DatabaseState<S>;
        const [newState, result] = core.insert(state, tableName as keyof S['tables'], values as any);
        const newMeta = newState[tableName]!.meta;
        if (newMeta.lastId !== meta.lastId) {
          await fs.mkdir(getTableDir(tableName), { recursive: true });
          await writeAtomic(metaPath, JSON.stringify(newMeta, null, 2), fs);
        }
        const insertedRecords = Array.isArray(result) ? result : [result];
        await Promise.all(
          insertedRecords.map((r: any) =>
            writeAtomic(getRecordPath(tableName, r[idColumn]), serializer.stringify(r), fs)
          )
        );
        return result;
      },
      update: async (core, tableName, data, predicate) => {
        const idColumn = getIdColumn(tableName);
        const state = await getFullState();
        const [newState, result] = core.update(state, tableName as keyof S['tables']).set(data).where(predicate as any);

        // Only update the specific records that were modified, don't delete other files
        const updatedRecords = Array.isArray(result) ? result : [result];
        await Promise.all(
          updatedRecords.map((r: any) =>
            writeAtomic(getRecordPath(tableName, r[idColumn]), serializer.stringify(r), fs)
          )
        );

        return result;
      },
      delete: async (core, tableName, predicate) => {
        const idColumn = getIdColumn(tableName);
        const state = await getFullState();
        const [newState, result] = core.delete(state, tableName as keyof S['tables']).where(predicate as any);

        const deletedIds = new Set(result.map((r: any) => String(r[idColumn])));
        const tableDir = getTableDir(tableName);
        const files = await fs.readdir(tableDir);
        const toDelete = files.filter(f => deletedIds.has(path.parse(f).name));
        await Promise.all(toDelete.map(f => fs.unlink(path.join(tableDir, f))));

        // Also update meta if it changed (e.g., due to cascades)
        const newMeta = newState[tableName]?.meta;
        if (newMeta && JSON.stringify(newMeta) !== JSON.stringify(state[tableName]?.meta)) {
          await writeAtomic(getMetaPath(tableName), JSON.stringify(newMeta, null, 2), fs);
        }

        return result;
      }
    };
  };

  const io = fileAdapter.options.multi ? createMultiFileIO() : createPerRecordIO();
  return createOnDemandDbContext(schema, adapter, core, io);
}
```

## File: src/fs.ts
```typescript
import { promises as fs } from 'fs';
import path from 'path';
import { TEMP_FILE_SUFFIX } from './utils/constants';
import type { FsProvider } from './types';

export const defaultFsProvider: FsProvider = {
  readFile: async (filepath: string): Promise<string | null> => {
    try {
      return await fs.readFile(filepath, 'utf-8');
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        return null;
      }
      throw error;
    }
  },
  writeFile: (filepath: string, content: string, encoding: 'utf-8'): Promise<void> => {
    return fs.writeFile(filepath, content, encoding);
  },
  rename: fs.rename,
  mkdir: fs.mkdir,
  readdir: fs.readdir,
  unlink: fs.unlink,
};

export const writeAtomic = async (
  filepath: string,
  content: string,
  fsProvider: FsProvider,
): Promise<void> => {
    // Adding Date.now() for uniqueness in case of concurrent operations
    const tempFilepath = `${filepath}.${Date.now()}${TEMP_FILE_SUFFIX}`;
    await fsProvider.mkdir(path.dirname(filepath), { recursive: true });
    await fsProvider.writeFile(tempFilepath, content, 'utf-8');
    await fsProvider.rename(tempFilepath, filepath);
};
```

## File: src/index.ts
```typescript
import { createDatabase } from './db';
import { createFileAdapter } from './adapter';
import { createSchema, id, uuid, string, number, boolean, date, createdAt, updatedAt, deletedAt, object, one, many, count, sum, avg, min, max } from './schema';

export type {
  // --- Core & Schema ---
  KonroSchema,
  DatabaseState,
  KRecord,
  // Schema Definition
  ColumnDefinition,
  RelationDefinition,
  OneRelationDefinition,
  ManyRelationDefinition,
  BaseRelationDefinition,
  AggregationDefinition,

  // --- DB Contexts ---
  DbContext,
  InMemoryDbContext,
  OnDemandDbContext,

  // --- Fluent Query Builders ---
  QueryBuilder,
  ChainedQueryBuilder,
  UpdateBuilder,
  DeleteBuilder,
  OnDemandQueryBuilder,
  OnDemandChainedQueryBuilder,
  OnDemandUpdateBuilder,
  OnDemandDeleteBuilder,
  WithArgument,

  // --- Adapters & I/O ---
  StorageAdapter,
  FileStorageAdapter,
  FileAdapterOptions,
  SingleFileStrategy,
  MultiFileStrategy,
  PerRecordStrategy,
  FsProvider,
  Serializer,
} from './types';

/**
 * The main Konro object, providing access to all core functionalities
 * for schema definition, database creation, and adapter configuration.
 */
export const konro = {
  /**
   * Defines the structure, types, and relations of your database.
   * This is the single source of truth for both runtime validation and static types.
   */
  createSchema,
  /**
   * Creates the main `db` context, which is the primary interface for all
   * database operations (read, write, query, etc.).
   */
  createDatabase,
  /**
   * Creates a file-based storage adapter for persisting the database state
   * to a JSON or YAML file.
   */
  createFileAdapter,
  // --- Column Definition Helpers ---
  id,
  uuid,
  string,
  number,
  boolean,
  date,
  createdAt,
  updatedAt,
  deletedAt,
  object,
  // --- Relationship Definition Helpers ---
  one,
  many,
  // --- Aggregation Definition Helpers ---
  count,
  sum,
  avg,
  min,
  max,
};
```

## File: src/schema.ts
```typescript
//
// Konro: The Type-Safe, Functional ORM for JSON/YAML
//
// ## Pillar I: The Recipe (Schema Definition)
//
// This file contains the core logic for defining a database schema. It is designed to be
// both the runtime source of truth for validation and the static source of truth for
// TypeScript types. By using phantom types and inference, we can create a fully-typed
// `db` object from a single schema definition object, eliminating the need for manual
// type declarations (`interface User { ... }`) and ensuring they never get out of sync.
//

import type {
  KonroSchema,
  ColumnDefinition,
  OneRelationDefinition,
  ManyRelationDefinition,
  AggregationDefinition
} from './types';

// --- SCHEMA BUILDER FUNCTION ---

/**
 * Defines the structure, types, and relations of your database.
 * This is the single source of truth for both runtime validation and static types.
 *
 * @param schemaDef The schema definition object.
 * @returns A processed schema object with inferred types attached.
 */
export const createSchema = <
  const TDef extends {
    tables: Record<string, Record<string, ColumnDefinition<any>>>;
    relations?: (tables: TDef['tables']) => Record<string, Record<string, OneRelationDefinition | ManyRelationDefinition>>;
  }
>(
  schemaDef: TDef
): KonroSchema<TDef['tables'], TDef['relations'] extends (...args: any) => any ? ReturnType<TDef['relations']> : {}> => { // eslint-disable-line
  const relations = schemaDef.relations ? schemaDef.relations(schemaDef.tables) : {};
  return {
    tables: schemaDef.tables,
    relations: relations as any, // Cast to bypass complex conditional type issue
    // Types are applied via the return type annotation, these are just placeholders at runtime.
    types: null as any,
    base: {} as any,
    create: {} as any,
  };
};


// --- COLUMN DEFINITION HELPERS ---

const createColumn = <T>(dataType: ColumnDefinition<T>['dataType'], options: object | undefined, tsType: T): ColumnDefinition<T> => ({
  _type: 'column',
  dataType,
  options,
  _tsType: tsType,
});

/** A managed, auto-incrementing integer primary key. This is the default strategy. */
export const id = () => createColumn<number>('id', { unique: true, _pk_strategy: 'auto-increment' }, 0);
/** A managed, universally unique identifier (UUID) primary key. Stored as a string. */
export const uuid = () => createColumn<string>('id', { unique: true, _pk_strategy: 'uuid' }, '');

// A shared base type for options to avoid repetition in overloads.
type BaseStringOptions = {
  unique?: boolean;
  min?: number;
  max?: number;
  format?: 'email' | 'uuid' | 'url';
};
/** A string column with optional validation. */
export function string(options: BaseStringOptions & { optional: true; default?: string | null | (() => string | null) }): ColumnDefinition<string | null>;
export function string(options?: BaseStringOptions & { optional?: false; default?: string | (() => string) }): ColumnDefinition<string>;
export function string(options?: BaseStringOptions & { optional?: boolean; default?: unknown }): ColumnDefinition<string> | ColumnDefinition<string | null> {
  if (options?.optional) {
    return createColumn<string | null>('string', options, null);
  }
  return createColumn<string>('string', options, '');
}

type BaseNumberOptions = {
  unique?: boolean;
  min?: number;
  max?: number;
  type?: 'integer';
};
/** A number column with optional validation. */
export function number(options: BaseNumberOptions & { optional: true; default?: number | null | (() => number | null) }): ColumnDefinition<number | null>;
export function number(options?: BaseNumberOptions & { optional?: false; default?: number | (() => number) }): ColumnDefinition<number>;
export function number(options?: BaseNumberOptions & { optional?: boolean; default?: unknown }): ColumnDefinition<number> | ColumnDefinition<number | null> {
  if (options?.optional) {
    return createColumn<number | null>('number', options, null);
  }
  return createColumn<number>('number', options, 0);
}

/** A boolean column. */
export function boolean(options: { optional: true; default?: boolean | null | (() => boolean | null) }): ColumnDefinition<boolean | null>;
export function boolean(options?: { optional?: false; default?: boolean | (() => boolean) }): ColumnDefinition<boolean>;
export function boolean(options?: { optional?: boolean; default?: unknown }): ColumnDefinition<boolean> | ColumnDefinition<boolean | null> {
  if (options?.optional) {
    return createColumn<boolean | null>('boolean', options, null);
  }
  return createColumn<boolean>('boolean', options, false);
}

/** A generic date column. Consider using `createdAt` or `updatedAt` for managed timestamps. */
export function date(options: { optional: true; default?: Date | null | (() => Date | null) }): ColumnDefinition<Date | null>;
export function date(options?: { optional?: false; default?: Date | (() => Date) }): ColumnDefinition<Date>;
export function date(options?: { optional?: boolean; default?: unknown }): ColumnDefinition<Date> | ColumnDefinition<Date | null> {
  if (options?.optional) {
    return createColumn<Date | null>('date', options, null);
  }
  return createColumn<Date>('date', options, new Date());
}

/** A managed timestamp set when a record is created. */
export const createdAt = (): ColumnDefinition<Date> => createColumn<Date>('date', { _konro_sub_type: 'createdAt', default: () => new Date() }, new Date());
/** A managed timestamp set when a record is created and updated. */
export const updatedAt = (): ColumnDefinition<Date> => createColumn<Date>('date', { _konro_sub_type: 'updatedAt', default: () => new Date() }, new Date());
/** A managed, nullable timestamp for soft-deleting records. */
export const deletedAt = (): ColumnDefinition<Date | null> => createColumn<Date | null>('date', { _konro_sub_type: 'deletedAt', default: null }, null);
/** A column for storing arbitrary JSON objects, with a generic for type safety. */
export function object<T extends object>(options: { optional: true; default?: T | null | (() => T | null) }): ColumnDefinition<T | null>;
export function object<T extends object>(options?: { optional?: false; default?: T | (() => T) }): ColumnDefinition<T>;
export function object<T extends object>(options?: { optional?: boolean; default?: unknown }
): ColumnDefinition<T | null> | ColumnDefinition<T> {
  if (options?.optional) {
    // The cast here is to satisfy the generic constraint on the implementation.
    // The phantom type will be `T | null`.
    return { _type: 'column', dataType: 'object', options } as ColumnDefinition<T | null>;
  }
  return { _type: 'column', dataType: 'object', options };
}


// --- RELATIONSHIP DEFINITION HELPERS ---

/** Defines a `one-to-one` or `many-to-one` relationship. */
export const one = <T extends string>(targetTable: T, options: { on: string; references: string; onDelete?: 'CASCADE' | 'SET NULL' }): OneRelationDefinition & { targetTable: T } => ({
  _type: 'relation',
  relationType: 'one',
  targetTable,
  ...options,
});

/** Defines a `one-to-many` relationship. */
export const many = <T extends string>(targetTable: T, options: { on: string; references: string; onDelete?: 'CASCADE' | 'SET NULL' }): ManyRelationDefinition & { targetTable: T } => ({
  _type: 'relation',
  relationType: 'many',
  targetTable,
  ...options,
});


// --- AGGREGATION DEFINITION HELPERS ---

/** Aggregation to count records. */
export const count = (): AggregationDefinition => ({ _type: 'aggregation', aggType: 'count' });
/** Aggregation to sum a numeric column. */
export const sum = (column: string): AggregationDefinition => ({ _type: 'aggregation', aggType: 'sum', column });
/** Aggregation to average a numeric column. */
export const avg = (column: string): AggregationDefinition => ({ _type: 'aggregation', aggType: 'avg', column });
/** Aggregation to find the minimum value in a numeric column. */
export const min = (column: string): AggregationDefinition => ({ _type: 'aggregation', aggType: 'min', column });
/** Aggregation to find the maximum value in a numeric column. */
export const max = (column: string): AggregationDefinition => ({ _type: 'aggregation', aggType: 'max', column });
```

## File: src/types.ts
```typescript
// --- Schema Definition Types (from schema.ts) ---

/** The definition for a database column, created by helpers like `konro.string()`. */
export interface ColumnDefinition<T> {
  readonly _type: 'column';
  readonly dataType: 'id' | 'string' | 'number' | 'boolean' | 'date' | 'object';
  readonly options: any;
  readonly _tsType?: T; // Phantom type, does not exist at runtime
}

/** The definition for a table relationship, created by `konro.one()` or `konro.many()`. */
export interface BaseRelationDefinition {
  readonly _type: 'relation';
  readonly targetTable: string;
  readonly on: string;
  readonly references: string;
  readonly onDelete?: 'CASCADE' | 'SET NULL';
}

export interface OneRelationDefinition extends BaseRelationDefinition {
  readonly relationType: 'one';
}

export interface ManyRelationDefinition extends BaseRelationDefinition {
  readonly relationType: 'many';
}

export type RelationDefinition = OneRelationDefinition | ManyRelationDefinition;

/** The definition for a data aggregation, created by `konro.count()`, `konro.sum()`, etc. */
export interface AggregationDefinition {
  readonly _type: 'aggregation';
  readonly aggType: 'count' | 'sum' | 'avg' | 'min' | 'max';
  readonly column?: string;
}

/** Infers the underlying TypeScript type from a `ColumnDefinition`. e.g., `ColumnDefinition<string>` => `string`. */
type InferColumnType<C> = C extends ColumnDefinition<infer T> ? T : never;

/** A mapping of table names to their base model types (columns only, no relations). */
export type BaseModels<TTables extends Record<string, any>> = {
  [TableName in keyof TTables]: {
    [ColumnName in keyof TTables[TableName]]: InferColumnType<TTables[TableName][ColumnName]>;
  };
};

/** A mapping of table names to their full model types, including relations. */
type Models<
  TTables extends Record<string, any>,
  TRelations extends Record<string, any>,
  TBaseModels extends Record<keyof TTables, any>
> = {
  [TableName in keyof TTables]: TBaseModels[TableName] &
    (TableName extends keyof TRelations
      ? {
          [RelationName in keyof TRelations[TableName]]?: TRelations[TableName][RelationName] extends OneRelationDefinition
            ? Models<TTables, TRelations, TBaseModels>[TRelations[TableName][RelationName]['targetTable']] | null
            : TRelations[TableName][RelationName] extends ManyRelationDefinition
            ? Models<TTables, TRelations, TBaseModels>[TRelations[TableName][RelationName]['targetTable']][]
            : never;
        }
      : {});
} & { [key: string]: any };

/** Finds all column names in a table definition that are optional for insertion (i.e., `id`, has a `default`, or is `optional`). */
type OptionalCreateKeys<TTableDef> = {
  [K in keyof TTableDef]: TTableDef[K] extends { dataType: 'id' }
    ? K
    : TTableDef[K] extends { options: { default: unknown } }
    ? K
    : TTableDef[K] extends { options: { optional: true } }
    ? K
    : never;
}[keyof TTableDef];

/** A mapping of table names to their "create" types, used for `db.insert`. */
type CreateModels<
  TTables extends Record<string, any>,
  TBaseModels extends Record<keyof TTables, any>
> = {
  [TableName in keyof TTables]: Omit<
    {
      // Required fields
      [K in Exclude<keyof TBaseModels[TableName], OptionalCreateKeys<TTables[TableName]>>]: TBaseModels[TableName][K];
    } & {
      // Optional fields
      [K in OptionalCreateKeys<TTables[TableName]>]?: TBaseModels[TableName][K];
    },
    // 'id' is always omitted from create types
    'id'
  >;
};

/** The publicly exposed structure of a fully-processed Konro schema. */
export interface KonroSchema<
  TTables extends Record<string, any>,
  TRelations extends Record<string, any>
> {
  tables: TTables;
  relations: TRelations;
  /** The full, relational types for each table model. */
  types: Models<TTables, TRelations, BaseModels<TTables>>;
  /** The base types for each table model, without any relations. */
  base: BaseModels<TTables>;
  /** The types for creating new records, with defaults and `id` made optional. */
  create: CreateModels<TTables, BaseModels<TTables>>;
}


// --- Generic & Core Types ---

/** A generic representation of a single record within a table. It uses `unknown` for values to enforce type-safe access. */
export type KRecord = Record<string, unknown>;

/** Represents the state of a single table, including its records and metadata. */
export type TableState<T extends KRecord = KRecord> = {
  records: T[];
  meta: {
    lastId: number;
  };
};

/** The in-memory representation of the entire database. It is a plain, immutable object. */
export type DatabaseState<S extends KonroSchema<any, any> | unknown = unknown> = S extends KonroSchema<any, any>
  ? {
      [TableName in keyof S['tables']]: TableState<BaseModels<S['tables']>[TableName]>;
    }
  : {
      [tableName: string]: TableState;
    };


// --- FS Provider Types (from fs.ts) ---
export interface FsProvider {
  readFile(filepath: string): Promise<string | null>;
  writeFile(filepath: string, content: string, encoding: 'utf-8'): Promise<void>;
  rename(oldPath: string, newPath: string): Promise<void>;
  mkdir(dir: string, options: { recursive: true }): Promise<string | undefined>;
  readdir(dir: string): Promise<string[]>;
  unlink(filepath: string): Promise<void>;
}


// --- Serializer Types (from utils/serializer.util.ts) ---
export type Serializer = {
  parse: <T>(data: string, tableSchema?: Record<string, ColumnDefinition<any>>) => T;
  stringify: (obj: any) => string;
};


// --- Storage Adapter Types (from adapter.ts) ---

export interface StorageAdapter {
  read<S extends KonroSchema<any, any>>(schema: S): Promise<DatabaseState<S>>;
  write<S extends KonroSchema<any, any>>(state: DatabaseState<S>, schema: S): Promise<void>;
  readonly mode: 'in-memory' | 'on-demand';
}

export interface FileStorageAdapter extends StorageAdapter {
  readonly options: FileAdapterOptions;
  readonly fs: FsProvider;
  readonly serializer: Serializer;
  readonly fileExtension: string;
}

export type SingleFileStrategy = { single: { filepath: string }; multi?: never; perRecord?: never };
export type MultiFileStrategy = { multi: { dir: string }; single?: never; perRecord?: never };
export type PerRecordStrategy = { perRecord: { dir: string }; single?: never; multi?: never };

export type FileAdapterOptions = {
  format: 'json' | 'yaml' | 'csv' | 'xlsx';
  fs?: FsProvider;
  /**
   * Defines the data access strategy.
   * - `in-memory`: (Default) Loads the entire database into memory on init. Fast for small/medium datasets.
   * - `on-demand`: Reads from the file system for each query. Slower but supports larger datasets. Requires 'multi-file' or 'per-record' strategy.
   */
  mode?: 'in-memory' | 'on-demand';
} & (SingleFileStrategy | MultiFileStrategy | PerRecordStrategy);


// --- Operation Descriptor Types (from operations.ts) ---

export type WithClause = Record<string, boolean | {
  where?: (record: KRecord) => boolean;
  select?: Record<string, ColumnDefinition<unknown>>;
  with?: WithClause;
}>;

export interface QueryDescriptor {
  tableName: string;
  select?: Record<string, ColumnDefinition<unknown> | RelationDefinition>;
  where?: (record: KRecord) => boolean;
  with?: WithClause;
  limit?: number;
  offset?: number;
  withDeleted?: boolean;
}

export interface AggregationDescriptor extends QueryDescriptor {
  aggregations: Record<string, AggregationDefinition>;
}


// --- DB Context & Fluent API Types (from db.ts) ---

export type WithArgument<
  S extends KonroSchema<any, any>,
  TName extends keyof S['tables']
> = {
  [K in keyof S['relations'][TName]]?: boolean | ({
    where?: (record: S['base'][S['relations'][TName][K]['targetTable']]) => boolean;
  } & (
    | { select: Record<string, ColumnDefinition<unknown>>; with?: never }
    | { select?: never; with?: WithArgument<S, S['relations'][TName][K]['targetTable']> }
  ));
};

export type ResolveWith<
  S extends KonroSchema<any, any>,
  TName extends keyof S['tables'],
  TWith extends WithArgument<S, TName>
> = {
  [K in keyof TWith & keyof S['relations'][TName]]:
 S['relations'][TName][K] extends { relationType: 'many' }
    ? TWith[K] extends { select: infer TSelect }
      ? { [P in keyof TSelect]: InferColumnType<TSelect[P]> }[]
      : TWith[K] extends { with: infer TNestedWith }
      ? (S['base'][S['relations'][TName][K]['targetTable']] &
          ResolveWith<S, S['relations'][TName][K]['targetTable'], TNestedWith & WithArgument<S, S['relations'][TName][K]['targetTable']>>)[]
      : S['base'][S['relations'][TName][K]['targetTable']][]
    : S['relations'][TName][K] extends { relationType: 'one' }
    ? TWith[K] extends { select: infer TSelect }
      ? { [P in keyof TSelect]: InferColumnType<TSelect[P]> } | null
      : TWith[K] extends { with: infer TNestedWith }
      ? (S['base'][S['relations'][TName][K]['targetTable']] &
          ResolveWith<S, S['relations'][TName][K]['targetTable'], TNestedWith & WithArgument<S, S['relations'][TName][K]['targetTable']>>) | null
      : S['base'][S['relations'][TName][K]['targetTable']] | null
    : never
;
};

export interface ChainedQueryBuilder<S extends KonroSchema<any, any>, TName extends keyof S['tables'], TReturn> {
  select(fields: Record<string, ColumnDefinition<unknown> | RelationDefinition>): this;
  where(predicate: Partial<S['base'][TName]> | ((record: S['base'][TName]) => boolean)): this;
  withDeleted(): this;
  with<W extends WithArgument<S, TName>>(relations: W): ChainedQueryBuilder<S, TName, TReturn & ResolveWith<S, TName, W>>;
  limit(count: number): this;
  offset(count: number): this;
  all(): TReturn[];
  first(): TReturn | null;
  aggregate<TAggs extends Record<string, AggregationDefinition>>(
    aggregations: TAggs
  ): { [K in keyof TAggs]: number | null };
}

export interface QueryBuilder<S extends KonroSchema<any, any>> {
  from<T extends keyof S['tables']>(tableName: T): ChainedQueryBuilder<S, T, S['base'][T]>;
}

export interface UpdateBuilder<S extends KonroSchema<any, any>, TBase, TCreate> {
  set(data: Partial<TCreate>): {
    where(predicate: Partial<TBase> | ((record: TBase) => boolean)): [DatabaseState<S>, TBase[]];
  };
}

export interface DeleteBuilder<S extends KonroSchema<any, any>, TBase> {
  where(predicate: Partial<TBase> | ((record: TBase) => boolean)): [DatabaseState<S>, TBase[]];
}

export interface InMemoryDbContext<S extends KonroSchema<any, any>> {
  schema: S;
  adapter: StorageAdapter;
  read(): Promise<DatabaseState<S>>;
  write(state: DatabaseState<S>): Promise<void>;
  createEmptyState(): DatabaseState<S>;

  query(state: DatabaseState<S>): QueryBuilder<S>;
  insert<T extends keyof S['tables']>(state: DatabaseState<S>, tableName: T, values: S['create'][T]): [DatabaseState<S>, S['base'][T]];
  insert<T extends keyof S['tables']>(state: DatabaseState<S>, tableName: T, values: Readonly<S['create'][T]>[]): [DatabaseState<S>, S['base'][T][]];
  update<T extends keyof S['tables']>(state: DatabaseState<S>, tableName: T): UpdateBuilder<S, S['base'][T], S['create'][T]>;
  delete<T extends keyof S['tables']>(state: DatabaseState<S>, tableName: T): DeleteBuilder<S, S['base'][T]>;
}

export interface OnDemandChainedQueryBuilder<S extends KonroSchema<any, any>, TName extends keyof S['tables'], TReturn> {
  select(fields: Record<string, ColumnDefinition<unknown> | RelationDefinition>): this;
  where(predicate: Partial<S['base'][TName]> | ((record: S['base'][TName]) => boolean)): this;
  withDeleted(): this;
  with<W extends WithArgument<S, TName>>(relations: W): OnDemandChainedQueryBuilder<S, TName, TReturn & ResolveWith<S, TName, W>>;
  limit(count: number): this;
  offset(count: number): this;
  all(): Promise<TReturn[]>;
  first(): Promise<TReturn | null>;
  aggregate<TAggs extends Record<string, AggregationDefinition>>(
    aggregations: TAggs
  ): Promise<{ [K in keyof TAggs]: number | null }>;
}

export interface OnDemandQueryBuilder<S extends KonroSchema<any, any>> {
  from<T extends keyof S['tables']>(tableName: T): OnDemandChainedQueryBuilder<S, T, S['base'][T]>;
}

export interface OnDemandUpdateBuilder<TBase, TCreate> {
  set(data: Partial<TCreate>): {
    where(predicate: Partial<TBase> | ((record: TBase) => boolean)): Promise<TBase[]>;
  };
}

export interface OnDemandDeleteBuilder<TBase> {
  where(predicate: Partial<TBase> | ((record: TBase) => boolean)): Promise<TBase[]>;
}

export interface OnDemandDbContext<S extends KonroSchema<any, any>> {
  schema: S;
  adapter: StorageAdapter;
  read(): Promise<never>;
  write(): Promise<never>;
  createEmptyState(): DatabaseState<S>;

  query(): OnDemandQueryBuilder<S>;
  insert<T extends keyof S['tables']>(tableName: T, values: S['create'][T]): Promise<S['base'][T]>;
  insert<T extends keyof S['tables']>(tableName: T, values: Readonly<S['create'][T]>[]): Promise<S['base'][T][]>;
  update<T extends keyof S['tables']>(tableName: T): OnDemandUpdateBuilder<S['base'][T], S['create'][T]>;
  delete<T extends keyof S['tables']>(tableName: T): OnDemandDeleteBuilder<S['base'][T]>;
}

export type DbContext<S extends KonroSchema<any, any>> = InMemoryDbContext<S> | OnDemandDbContext<S>;
```

## File: test/e2e/ErrorAndEdgeCases/Pagination.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > ErrorAndEdgeCases > Pagination', () => {
  const dbFilePath = path.join(TEST_DIR, 'pagination_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

    beforeEach(async () => {
        await ensureTestDir();
        let state = db.createEmptyState();
        const usersToInsert = [];
        for (let i = 1; i <= 100; i++) {
            usersToInsert.push({
                name: `User ${i}`,
                email: `user${i}@test.com`,
                age: 20 + (i % 30),
                isActive: true
            });
        }
        [state] = db.insert(state, 'users', usersToInsert);
        await db.write(state);
    });
    afterEach(cleanup);

  it('should correctly paginate through a large set of records from a file', async () => {
    const state = await db.read();
    expect(state.users!.records.length).toBe(100);

    // Get page 1 (items 1-10)
    const page1 = await db.query(state).from('users').limit(10).offset(0).all();
    expect(page1.length).toBe(10);
    expect(page1[0]?.name).toBe('User 1');
    expect(page1[9]?.name).toBe('User 10');

    // Get page 2 (items 11-20)
    const page2 = await db.query(state).from('users').limit(10).offset(10).all();
    expect(page2.length).toBe(10);
    expect(page2[0]?.name).toBe('User 11');
    expect(page2[9]?.name).toBe('User 20');

    // Get the last page, which might be partial
    const lastPage = await db.query(state).from('users').limit(10).offset(95).all();
    expect(lastPage.length).toBe(5);
    expect(lastPage[0]?.name).toBe('User 96');
    expect(lastPage[4]?.name).toBe('User 100');

    // Get an empty page beyond the end
    const emptyPage = await db.query(state).from('users').limit(10).offset(100).all();
    expect(emptyPage.length).toBe(0);
  });
});
```

## File: test/e2e/ErrorAndEdgeCases/Transaction.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('E2E > ErrorAndEdgeCases > Transaction', () => {
  const dbFilePath = path.join(TEST_DIR, 'transaction_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(async () => {
    await ensureTestDir();
    // Start with a clean slate for each test
    await db.write(db.createEmptyState());
  });
  afterEach(cleanup);

    it('should not write to disk if an operation fails mid-transaction', async () => {
        // 1. Get initial state with one user
        let state = await db.read();
        [state] = db.insert(state, 'users', { name: 'Good User', email: 'good@test.com', age: 30, isActive: true });
        await db.write(state);

    const contentBefore = await fs.readFile(dbFilePath, 'utf-8');

    // 2. Start a "transaction": read, then perform multiple operations
    let transactionState = await db.read();

        // This one is fine
        [transactionState] = db.insert(transactionState, 'users', { name: 'Another User', email: 'another@test.com', age: 31, isActive: true });

        // This one will fail due to unique constraint
        const failingOperation = () => {
            db.insert(transactionState, 'users', { name: 'Bad User', email: 'good@test.com', age: 32, isActive: true });
        };
        expect(failingOperation).toThrow(KonroValidationError);

    // Even if the error is caught, the developer should not write the tainted `transactionState`.
    // The file on disk should remain untouched from before the transaction started.
    const contentAfter = await fs.readFile(dbFilePath, 'utf-8');
    expect(contentAfter).toEqual(contentBefore);
  });

    it('should not change the database file if an update matches no records', async () => {
        let state = await db.read();
        [state] = db.insert(state, 'users', { name: 'Initial User', email: 'initial@test.com', age: 50, isActive: true });
        await db.write(state);

    const contentBefore = await fs.readFile(dbFilePath, 'utf-8');

    // Read the state to perform an update
    let currentState = await db.read();
    const [newState] = await db.update(currentState, 'users')
      .set({ name: 'This Should Not Be Set' })
      .where({ id: 999 }); // This matches no records

    await db.write(newState);

    const contentAfter = await fs.readFile(dbFilePath, 'utf-8');

    // The content should be identical because the state object itself shouldn't have changed meaningfully.
    expect(contentAfter).toEqual(contentBefore);
  });
});
```

## File: test/e2e/MultiFileYaml/FullLifecycle.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';

describe('E2E > MultiFileYaml > FullLifecycle', () => {
  const dbDirPath = path.join(TEST_DIR, 'e2e_yaml_db');
  const adapter = konro.createFileAdapter({
    format: 'yaml',
    multi: { dir: dbDirPath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  it('should handle a full data lifecycle across multiple YAML files', async () => {
    // 1. Initialize empty database files
    let state = db.createEmptyState();
    await db.write(state);

    // Check that empty files are created
    const usersFilePath = path.join(dbDirPath, 'users.yaml');
    let usersFileContent = await fs.readFile(usersFilePath, 'utf-8');
    expect(yaml.load(usersFileContent)).toEqual({ records: [], meta: { lastId: 0 } });

    // 2. Insert data and write to disk
    const [s1, user] = db.insert(state, 'users', { name: 'E2E Yaml', email: 'yaml.e2e@test.com', age: 50, isActive: true });
    const [s2] = db.insert(s1, 'posts', { title: 'YAML Post', content: '...', authorId: user.id, publishedAt: new Date() });
    await db.write(s2);

    // 3. Read back and verify integrity from separate files
    const readState = await db.read();
    expect(readState.users!.records.length).toBe(1);
    expect(readState.posts!.records.length).toBe(1);
    expect(readState.users!.records[0]?.id).toBe(user.id);

    // 4. Query with relations
    const userWithPosts = db.query(readState).from('users').where({ id: user.id }).with({ posts: true }).first();
    expect(userWithPosts).toBeDefined();
    if (userWithPosts) {
      expect(userWithPosts.posts).toBeDefined();
      expect(userWithPosts.posts?.length).toBe(1);
      expect(userWithPosts.posts?.[0]?.title).toBe('YAML Post');
    }

    // 5. Update and write
    const [s3] = await db.update(readState, 'users').set({ name: 'Updated Yaml User' }).where({ id: user.id });
    await db.write(s3);
    const stateAfterUpdate = await db.read();
    expect(stateAfterUpdate.users!.records[0]?.name).toBe('Updated Yaml User');

    // 6. Delete and write
    const [s4] = await db.delete(stateAfterUpdate, 'posts').where({ authorId: user.id });
    await db.write(s4);
    const finalState = await db.read();
    expect(finalState.posts!.records.length).toBe(0);
    expect(finalState.users!.records.length).toBe(1);
  });
});
```

## File: test/e2e/Operations/Aggregate.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Aggregation', () => {
  const dbFilePath = path.join(TEST_DIR, 'aggregation_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(async () => {
    await ensureTestDir();
    let state = db.createEmptyState();
    const usersToInsert = [
      { name: 'User 1', email: 'u1@test.com', age: 20, isActive: true },
      { name: 'User 2', email: 'u2@test.com', age: 25, isActive: true },
      { name: 'User 3', email: 'u3@test.com', age: 30, isActive: false },
      { name: 'User 4', email: 'u4@test.com', age: 35, isActive: true },
      { name: 'User 5', email: 'u5@test.com', age: 40, isActive: false },
    ];
    [state] = db.insert(state, 'users', usersToInsert);
    await db.write(state);
  });
  afterEach(cleanup);

  it('should correctly calculate count, sum, avg, min, and max', async () => {
    const state = await db.read();

    const stats = db.query(state)
      .from('users')
      .aggregate({
        totalUsers: konro.count(),
        totalAge: konro.sum('age'),
        averageAge: konro.avg('age'),
        minAge: konro.min('age'),
        maxAge: konro.max('age'),
      });

    expect(stats.totalUsers).toBe(5);
    expect(stats.totalAge).toBe(20 + 25 + 30 + 35 + 40); // 150
    expect(stats.averageAge).toBe(150 / 5); // 30
    expect(stats.minAge).toBe(20);
    expect(stats.maxAge).toBe(40);
  });

  it('should correctly calculate aggregations with a where clause', async () => {
    const state = await db.read();

    const stats = db.query(state)
      .from('users')
      .where({ isActive: true })
      .aggregate({
        activeUsers: konro.count(),
        totalAgeActive: konro.sum('age'),
      });

    expect(stats.activeUsers).toBe(3);
    expect(stats.totalAgeActive).toBe(20 + 25 + 35); // 80
  });

  it('should handle aggregations on empty sets', async () => {
    const state = await db.read();

    const stats = db.query(state)
      .from('users')
      .where({ name: 'Nonexistent' })
      .aggregate({
        count: konro.count(),
        sum: konro.sum('age'),
        avg: konro.avg('age'),
        min: konro.min('age'),
        max: konro.max('age'),
      });

    expect(stats.count).toBe(0);
    expect(stats.sum).toBe(0); // sum of empty set is 0
    expect(stats.avg).toBeNull();
    expect(stats.min).toBeNull();
    expect(stats.max).toBeNull();
  });
});
```

## File: test/e2e/Operations/Delete.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Delete', () => {
  const dbFilePath = path.join(TEST_DIR, 'delete_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(async () => {
    await ensureTestDir();
    let state = db.createEmptyState();
    const usersToInsert = [
      { name: 'User A', email: 'a@test.com', age: 20 },
      { name: 'User B', email: 'b@test.com', age: 30 },
      { name: 'User C', email: 'c@test.com', age: 40 },
    ];
    [state] = db.insert(state, 'users', usersToInsert);
    await db.write(state);
  });
  afterEach(cleanup);

  it('should delete a single record matching a predicate object', async () => {
    let state = await db.read();
    expect(db.query(state).from('users').all().length).toBe(3);

    let deletedUsers;
    [state, deletedUsers] = db.delete(state, 'users').where({ email: 'b@test.com' });

    expect(deletedUsers.length).toBe(1);
    expect(deletedUsers[0]?.name).toBe('User B');

    const remainingUsers = db.query(state).from('users').all();
    expect(remainingUsers.length).toBe(2);
    expect(remainingUsers.find(u => u.email === 'b@test.com')).toBeUndefined();
  });

  it('should delete multiple records matching a predicate function', async () => {
    let state = await db.read();
    expect(db.query(state).from('users').all().length).toBe(3);

    let deletedUsers;
    [state, deletedUsers] = db.delete(state, 'users').where(user => user.age < 35);

    expect(deletedUsers.length).toBe(2);
    expect(deletedUsers.map(u => u.name).sort()).toEqual(['User A', 'User B']);

    const remainingUsers = db.query(state).from('users').all();
    expect(remainingUsers.length).toBe(1);
    expect(remainingUsers[0]?.name).toBe('User C');
  });

  it('should return an empty array and unchanged state if no records match', async () => {
    const initialState = await db.read();
    
    const [newState, deletedUsers] = db.delete(initialState, 'users').where({ name: 'Nonexistent' });

    expect(deletedUsers.length).toBe(0);
    expect(newState).toBe(initialState); // Should be the exact same object reference
  });

  it('should persist deletions to disk', async () => {
    let state = await db.read();
    [state] = db.delete(state, 'users').where({ id: 1 });
    await db.write(state);

    const stateAfterWrite = await db.read();
    const users = db.query(stateAfterWrite).from('users').all();
    expect(users.length).toBe(2);
    expect(users.find(u => u.id === 1)).toBeUndefined();
  });
});
```

## File: test/e2e/Operations/Insert.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('E2E > Operations > Insert', () => {
  const dbFilePath = path.join(TEST_DIR, 'insert_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(async () => {
    await ensureTestDir();
    await db.write(db.createEmptyState());
  });
  afterEach(cleanup);

  it('should insert a single record and return it', async () => {
    const state = await db.read();
    const [newState, newUser] = db.insert(state, 'users', {
      name: 'John Doe',
      email: 'john@test.com',
      age: 30,
    });

    expect(newUser.id).toBe(1);
    expect(newUser.name).toBe('John Doe');
    expect(newUser.isActive).toBe(true); // default value

    const usersInState = db.query(newState).from('users').all();
    expect(usersInState.length).toBe(1);
    expect(usersInState[0]).toEqual(newUser);
  });

  it('should insert multiple records and return them', async () => {
    const state = await db.read();
    const usersToInsert = [
      { name: 'Jane Doe', email: 'jane@test.com', age: 28 },
      { name: 'Peter Pan', email: 'peter@test.com', age: 100, isActive: false },
    ];
    const [newState, newUsers] = db.insert(state, 'users', usersToInsert);

    expect(newUsers.length).toBe(2);
    expect(newUsers[0]?.id).toBe(1);
    expect(newUsers[1]?.id).toBe(2);
    expect(newUsers[0]?.name).toBe('Jane Doe');
    expect(newUsers[1]?.isActive).toBe(false);

    const usersInState = db.query(newState).from('users').all();
    expect(usersInState.length).toBe(2);
  });

  it('should auto-increment IDs correctly across multiple inserts', async () => {
    let state = await db.read();
    let newUser;

    [state, newUser] = db.insert(state, 'users', { name: 'First', email: 'first@test.com', age: 20 });
    expect(newUser.id).toBe(1);

    [state, newUser] = db.insert(state, 'users', { name: 'Second', email: 'second@test.com', age: 21 });
    expect(newUser.id).toBe(2);
  });

  it('should throw validation error for duplicate unique fields', async () => {
    let state = await db.read();
    [state] = db.insert(state, 'users', { name: 'Unique User', email: 'unique@test.com', age: 40 });

    const insertDuplicate = () => {
      db.insert(state, 'users', { name: 'Another User', email: 'unique@test.com', age: 41 });
    };

    expect(insertDuplicate).toThrow("Value 'unique@test.com' for column 'email' must be unique");
  });

  it('should throw validation error for constraint violations', async () => {
    const state = await db.read();
    const insertInvalid = () => {
      db.insert(state, 'users', { name: 'A', email: 'bademail.com', age: 17 });
    };
    // It should throw on the first failure it finds. Order not guaranteed.
    // In this case, 'name' length < 2
    expect(insertInvalid).toThrow(KonroValidationError);
  });
});
```

## File: test/e2e/Operations/Query-With.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Query with Relations', () => {
  const dbFilePath = path.join(TEST_DIR, 'query_with_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  let userId1: number, userId2: number;
  let postId1: number;

  beforeEach(async () => {
    await ensureTestDir();
    let state = db.createEmptyState();
    
    // Insert users
    let u1, u2;
    [state, u1] = db.insert(state, 'users', { name: 'Alice', email: 'alice@test.com', age: 30 });
    [state, u2] = db.insert(state, 'users', { name: 'Bob', email: 'bob@test.com', age: 35 });
    userId1 = u1.id; 
    userId2 = u2.id;
    
    // Insert posts
    let p1;
    [state, p1] = db.insert(state, 'posts', { title: 'Alice Post 1', content: '...', authorId: userId1 });
    [state] = db.insert(state, 'posts', { title: 'Bob Post 1', content: '...', authorId: userId2 });
    [state] = db.insert(state, 'posts', { title: 'Alice Post 2', content: '...', authorId: userId1 });
    postId1 = p1.id;

    // Insert profiles
    [state] = db.insert(state, 'profiles', { bio: 'Bio for Alice', userId: userId1 });

    await db.write(state);
  });
  afterEach(cleanup);

  it('should eager-load a one-to-many relationship', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ id: userId1 }).with({ posts: true }).first();

    expect(user).toBeDefined();
    expect(user?.posts).toBeDefined();
    expect(user?.posts?.length).toBe(2);
    expect(user?.posts?.map(p => p.title).sort()).toEqual(['Alice Post 1', 'Alice Post 2']);
  });

  it('should eager-load a many-to-one relationship', async () => {
    const state = await db.read();
    const post = db.query(state).from('posts').where({ id: postId1 }).with({ author: true }).first();

    expect(post).toBeDefined();
    expect(post?.author).toBeDefined();
    expect(post?.author?.name).toBe('Alice');
  });

  it('should eager-load a one-to-one relationship', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ id: userId1 }).with({ profile: true }).first();
    
    expect(user).toBeDefined();
    expect(user?.profile).toBeDefined();
    expect(user?.profile?.bio).toBe('Bio for Alice');
  });

  it('should return null for a one-relation if no related record exists', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ id: userId2 }).with({ profile: true }).first();
    
    expect(user).toBeDefined();
    expect(user?.profile).toBeNull();
  });

  it('should return an empty array for a many-relation if no related records exist', async () => {
    let state = await db.read();
    let newUser;
    [state, newUser] = db.insert(state, 'users', { name: 'Charlie', email: 'charlie@test.com', age: 40 });
    
    const user = db.query(state).from('users').where({ id: newUser.id }).with({ posts: true }).first();
    expect(user).toBeDefined();
    expect(user?.posts).toEqual([]);
  });

  it('should handle nested eager-loading', async () => {
    const state = await db.read();
    const post = db.query(state)
      .from('posts')
      .where({ id: postId1 })
      .with({
        author: {
          with: {
            posts: true,
            profile: true,
          },
        },
      })
      .first();

    expect(post?.author?.name).toBe('Alice');
    expect(post?.author?.profile?.bio).toBe('Bio for Alice');
    expect(post?.author?.posts?.length).toBe(2);
  });

  it('should filter related records with a `where` clause', async () => {
    const state = await db.read();
    const user = db.query(state)
      .from('users')
      .where({ id: userId1 })
      .with({
        posts: {
          where: (post) => post.title.includes('Post 2'),
        }
      })
      .first();

    expect(user?.posts?.length).toBe(1);
    expect(user?.posts?.[0]?.title).toBe('Alice Post 2');
  });

  it('should select specific fields from related records', async () => {
    const state = await db.read();
    const user = db.query(state)
        .from('users')
        .where({ id: userId1 })
        .with({
            posts: {
                select: {
                    postTitle: testSchema.tables.posts.title,
                }
            }
        })
        .first();

    expect(user?.posts?.length).toBe(2);
    expect(user?.posts?.[0]).toEqual({ postTitle: 'Alice Post 1' });
    expect(user?.posts?.[1]).toEqual({ postTitle: 'Alice Post 2' });
    // @ts-expect-error - content should not exist on the selected type
    expect(user?.posts?.[0]?.content).toBeUndefined();
  });
});
```

## File: test/e2e/Operations/Query.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Query', () => {
  const dbFilePath = path.join(TEST_DIR, 'query_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(async () => {
    await ensureTestDir();
    let state = db.createEmptyState();
    const usersToInsert = [
      { name: 'Alice', email: 'alice@test.com', age: 29, isActive: true },
      { name: 'Bob', email: 'bob@test.com', age: 35, isActive: false },
      { name: 'Charlie', email: 'charlie@test.com', age: 30, isActive: true },
    ];
    [state] = db.insert(state, 'users', usersToInsert);
    await db.write(state);
  });
  afterEach(cleanup);

  it('should return all records from a table', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').all();
    expect(users.length).toBe(3);
  });

  it('should filter records using a `where` object predicate', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').where({ age: 30, isActive: true }).all();
    expect(users.length).toBe(1);
    expect(users[0]?.name).toBe('Charlie');
  });

  it('should filter records using a `where` function predicate', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').where(u => u.name.startsWith('A') || u.name.startsWith('B')).all();
    expect(users.length).toBe(2);
    expect(users.map(u => u.name).sort()).toEqual(['Alice', 'Bob']);
  });

  it('should return a single record using `first()`', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ email: 'bob@test.com' }).first();
    expect(user).toBeDefined();
    expect(user?.name).toBe('Bob');
  });

  it('should return null from `first()` if no record matches', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ name: 'Zelda' }).first();
    expect(user).toBeNull();
  });

  it('should limit the number of results', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').limit(2).all();
    expect(users.length).toBe(2);
  });

  it('should offset the results for pagination', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').offset(1).all();
    expect(users.length).toBe(2);
    expect(users[0]?.name).toBe('Bob');
  });

  it('should combine limit and offset', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').limit(1).offset(1).all();
    expect(users.length).toBe(1);
    expect(users[0]?.name).toBe('Bob');
  });

  it('should select and rename specific fields', async () => {
    const state = await db.read();
    const partialUsers = db.query(state)
      .from('users')
      .where({ name: 'Alice' })
      .select({
        userName: testSchema.tables.users.name,
        userEmail: testSchema.tables.users.email,
      })
      .all();

    expect(partialUsers.length).toBe(1);
    const user = partialUsers[0];
    expect(user as any).toEqual({ userName: 'Alice', userEmail: 'alice@test.com' });
    expect((user as any).age).toBeUndefined();
  });
});
```

## File: test/e2e/Operations/Update.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('E2E > Operations > Update', () => {
  const dbFilePath = path.join(TEST_DIR, 'update_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(async () => {
    await ensureTestDir();
    let state = db.createEmptyState();
    const usersToInsert = [
      { name: 'User A', email: 'a@test.com', age: 20 },
      { name: 'User B', email: 'b@test.com', age: 30 },
      { name: 'User C', email: 'c@test.com', age: 40 },
    ];
    [state] = db.insert(state, 'users', usersToInsert);
    await db.write(state);
  });
  afterEach(cleanup);

  it('should update a single record and return it', async () => {
    let state = await db.read();
    let updatedUsers;
    [state, updatedUsers] = db.update(state, 'users')
      .set({ name: 'User A Updated', age: 21 })
      .where({ email: 'a@test.com' });

    expect(updatedUsers.length).toBe(1);
    const updatedUser = updatedUsers[0];
    expect(updatedUser?.name).toBe('User A Updated');
    expect(updatedUser?.age).toBe(21);
    expect(updatedUser?.id).toBe(1); // ID should be unchanged

    const userInState = db.query(state).from('users').where({ id: 1 }).first();
    expect(userInState?.name).toBe('User A Updated');
  });

  it('should update multiple records and return them', async () => {
    let state = await db.read();
    let updatedUsers;
    [state, updatedUsers] = db.update(state, 'users')
      .set({ isActive: false })
      .where(user => user.age < 35);

    expect(updatedUsers.length).toBe(2);
    updatedUsers.forEach(u => expect(u.isActive).toBe(false));

    const usersInState = db.query(state).from('users').all();
    expect(usersInState.filter(u => !u.isActive).length).toBe(2);
  });

  it('should not allow updating the primary key', async () => {
    let state = await db.read();
    let updatedUsers;

    [state, updatedUsers] = db.update(state, 'users')
      // @ts-expect-error - ID is not a valid key in the update type
      .set({ id: 99, name: 'ID Test' })
      .where({ id: 1 });
    
    expect(updatedUsers.length).toBe(1);
    expect(updatedUsers[0]?.id).toBe(1); // ID unchanged
    expect(updatedUsers[0]?.name).toBe('ID Test');
  });
  
  it('should throw validation error on update', async () => {
    let state = await db.read();
    
    // Make 'c@test.com' unavailable
    const failUpdate = () => {
      db.update(state, 'users')
        .set({ email: 'c@test.com' }) // Try to use an existing unique email
        .where({ id: 1 });
    };

    expect(failUpdate).toThrow(KonroValidationError);
  });

  it('should return an empty array if no records match the update predicate', async () => {
    let state = await db.read();
    let updatedUsers;
    [state, updatedUsers] = db.update(state, 'users')
      .set({ name: 'Should not be set' })
      .where({ id: 999 });

    expect(updatedUsers.length).toBe(0);
  });
});
```

## File: test/e2e/SingleFileJson/FullLifecycle.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';

describe('E2E > SingleFileJson > FullLifecycle', () => {
  const dbFilePath = path.join(TEST_DIR, 'e2e_db.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  it('should handle a full data lifecycle: write, read, insert, query, update, delete', async () => {
    // 1. Initialize an empty database file
    let state = db.createEmptyState();
    await db.write(state);
    let fileContent = await fs.readFile(dbFilePath, 'utf-8');
    expect(JSON.parse(fileContent).users.records.length).toBe(0);

    // 2. Read state, insert a user and a post, and write back
    state = await db.read();
    const [s1, user] = db.insert(state, 'users', {
      name: 'E2E User',
      email: 'e2e@test.com',
      age: 42,
      isActive: true,
    });
    const [s2, post] = db.insert(s1, 'posts', {
      title: 'E2E Post',
      content: 'Live from the disk',
      authorId: user.id,
      publishedAt: new Date(),
    });
    await db.write(s2);

    // 3. Read back and verify data integrity
    let readState = await db.read();
    expect(readState.users!.records.length).toBe(1);
    expect(readState.posts!.records.length).toBe(1);
    expect(readState.users!.records[0]?.name).toBe('E2E User');

    // 4. Perform a complex query with relations on the re-read state
    const userWithPosts = await db.query(readState)
      .from('users')
      .where({ id: user.id })
      .with({ posts: true })
      .first();

    expect(userWithPosts).toBeDefined();
    if (userWithPosts) {
      expect(userWithPosts.posts).toBeDefined();
      expect(userWithPosts.posts?.length).toBe(1);
      expect(userWithPosts.posts?.[0]?.title).toBe('E2E Post');
    }

    // 5. Update a record, write the change, and read back to confirm
    const [s3, updatedPosts] = await db.update(readState, 'posts')
      .set({ title: 'Updated E2E Post' })
      .where({ id: post.id });
    expect(updatedPosts.length).toBe(1);
    await db.write(s3);

    let stateAfterUpdate = await db.read();
    const updatedPostFromDisk = db.query(stateAfterUpdate).from('posts').where({ id: post.id }).first();
    expect(updatedPostFromDisk?.title).toBe('Updated E2E Post');

    // 6. Delete a record, write, and confirm it's gone
    const [s4, deletedUsers] = db.delete(stateAfterUpdate, 'users')
      .where({ id: user.id });
    expect(deletedUsers.length).toBe(1);
    await db.write(s4);

    let finalState = await db.read();
    expect(finalState.users!.records.length).toBe(0);
    // The post should also effectively be orphaned, let's check it's still there
    expect(finalState.posts!.records.length).toBe(1);
  });
});
```

## File: test/integration/Adapters/MultiFileYaml.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';

describe('Integration > Adapters > MultiFileYaml', () => {
  const dbDirPath = path.join(TEST_DIR, 'yaml_db');
  const adapter = konro.createFileAdapter({
    format: 'yaml',
    multi: { dir: dbDirPath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  it('should correctly write each table to a separate YAML file', async () => {
    let state = db.createEmptyState();
    [state] = db.insert(state, 'users', {
      name: 'YAML User',
      email: 'yaml@test.com',
      age: 44,
    });
    [state] = db.insert(state, 'posts', {
      title: 'YAML Post',
      content: 'Content here',
      authorId: 1,
    });

    await db.write(state);

    const usersFilePath = path.join(dbDirPath, 'users.yaml');
    const postsFilePath = path.join(dbDirPath, 'posts.yaml');

    const usersFileExists = await fs.access(usersFilePath).then(() => true).catch(() => false);
    const postsFileExists = await fs.access(postsFilePath).then(() => true).catch(() => false);
    expect(usersFileExists).toBe(true);
    expect(postsFileExists).toBe(true);

    const usersFileContent = await fs.readFile(usersFilePath, 'utf-8');
    const postsFileContent = await fs.readFile(postsFilePath, 'utf-8');

    const parsedUsers = yaml.load(usersFileContent) as { records: unknown[], meta: unknown };
    const parsedPosts = yaml.load(postsFileContent) as { records: unknown[], meta: unknown };

    expect(parsedUsers.records.length).toBe(1);
    expect((parsedUsers.records[0] as { name: string }).name).toBe('YAML User');
    expect((parsedUsers.meta as { lastId: number }).lastId).toBe(1);

    expect(parsedPosts.records.length).toBe(1);
    expect((parsedPosts.records[0] as { title: string }).title).toBe('YAML Post');
    expect((parsedPosts.meta as { lastId: number }).lastId).toBe(1);
  });

  it('should correctly serialize and deserialize dates', async () => {
    let state = db.createEmptyState();
    const testDate = new Date('2023-10-27T10:00:00.000Z');

    [state] = db.insert(state, 'posts', {
      title: 'Dated Post',
      content: '...',
      authorId: 1,
      publishedAt: testDate,
    });

    await db.write(state);

    const readState = await db.read();

    expect(readState.posts.records[0]?.publishedAt).toBeInstanceOf(Date);
    expect((readState.posts.records[0]?.publishedAt as Date).getTime()).toBe(testDate.getTime());
  });
});
```

## File: test/integration/Adapters/OnDemand.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir, uuidTestSchema } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';
import papaparse from 'papaparse';
import xlsx from 'xlsx';
import { KonroError } from '../../../src/utils/error.util';
import type { OnDemandDbContext } from '../../../src/db';

describe('Integration > Adapters > OnDemand', () => {
  const dbDirPath = path.join(TEST_DIR, 'on_demand_db');

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  describe('Initialization', () => {
    it('should successfully create an on-demand db context with a multi-file adapter', () => {
      const adapter = konro.createFileAdapter({
        format: 'yaml',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({
        schema: testSchema,
        adapter,
      });

      expect(db).toBeDefined();
      expect(db.adapter.mode).toBe('on-demand');
      expect(typeof db.insert).toBe('function');
      expect(typeof db.query).toBe('function');
    });

    it('should throw an error when creating an on-demand db context with a single-file adapter', () => {
      expect(() => {
        konro.createFileAdapter({
          format: 'json',
          mode: 'on-demand',
          single: { filepath: path.join(dbDirPath, 'db.json') },
        });
      }).toThrow(KonroError({ code: 'E104' }));
    });
  });

  describe('Unsupported Operations', () => {
    const adapter = konro.createFileAdapter({
      format: 'yaml',
      mode: 'on-demand',
      multi: { dir: dbDirPath },
    });
    const db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });
    
    it('should reject db.read()', async () => {
      expect(db.read()).rejects.toThrow(KonroError({ code: 'E400', methodName: 'read' }));
    });

    it('should reject db.write()', async () => {
      expect(db.write()).rejects.toThrow(KonroError({ code: 'E400', methodName: 'write' }));
    });
  });

  describe('CRUD Operations', () => {
    let db: OnDemandDbContext<typeof testSchema>;

    beforeEach(() => {
      const adapter = konro.createFileAdapter({
        format: 'yaml',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      db = konro.createDatabase({
        schema: testSchema,
        adapter,
      });
    });

    it('should insert a record and write it to the correct file', async () => {
      const user = await db.insert('users', {
        name: 'OnDemand User',
        email: 'ondemand@test.com',
        age: 25,
      });

      expect(user.id).toBe(1);
      expect(user.name).toBe('OnDemand User');

      const userFilePath = path.join(dbDirPath, 'users.yaml');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsedContent = yaml.load(fileContent) as any;

      expect(parsedContent.records.length).toBe(1);
      expect(parsedContent.records[0].name).toBe('OnDemand User');
      expect(parsedContent.meta.lastId).toBe(1);
    });

    it('should query for records', async () => {
      await db.insert('users', { name: 'Query User', email: 'q@test.com', age: 30 });
      
      const user = await db.query().from('users').where({ name: 'Query User' }).first();
      expect(user).toBeDefined();
      expect(user?.name).toBe('Query User');

      const allUsers = await db.query().from('users').all();
      expect(allUsers.length).toBe(1);
    });

    it('should update a record', async () => {
      const user = await db.insert('users', { name: 'Update Me', email: 'u@test.com', age: 40 });
      
      const updatedUsers = await db.update('users')
        .set({ name: 'Updated Name' })
        .where({ id: user.id });

      expect(updatedUsers.length).toBe(1);
      expect(updatedUsers[0]?.name).toBe('Updated Name');

      const userFilePath = path.join(dbDirPath, 'users.yaml');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsedContent = yaml.load(fileContent) as any;
      
      expect(parsedContent.records[0].name).toBe('Updated Name');
    });

    it('should delete a record', async () => {
      const user = await db.insert('users', { name: 'Delete Me', email: 'd@test.com', age: 50 });
      
      await db.delete('users').where({ id: user.id });

      const users = await db.query().from('users').all();
      expect(users.length).toBe(0);

      const userFilePath = path.join(dbDirPath, 'users.yaml');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsedContent = yaml.load(fileContent) as any;
      
      expect(parsedContent.records.length).toBe(0);
    });
    
    it('should query with relations', async () => {
      const user = await db.insert('users', { name: 'Author', email: 'author@test.com', age: 35 });
      await db.insert('posts', { title: 'Post by Author', content: '...', authorId: user.id });
      await db.insert('posts', { title: 'Another Post', content: '...', authorId: user.id });
      
      const userWithPosts = await db.query().from('users').where({ id: user.id }).with({ posts: true }).first();
      
      expect(userWithPosts).toBeDefined();
      expect(userWithPosts?.name).toBe('Author');
      expect(userWithPosts?.posts).toBeInstanceOf(Array);
      expect(userWithPosts?.posts?.length).toBe(2);
      expect(userWithPosts?.posts?.[0]?.title).toBe('Post by Author');
    });

    it('should perform aggregations', async () => {
      await db.insert('users', { name: 'Agg User 1', email: 'agg1@test.com', age: 20 });
      await db.insert('users', { name: 'Agg User 2', email: 'agg2@test.com', age: 30 });
      
      const result = await db.query().from('users').aggregate({
        count: konro.count(),
        avgAge: konro.avg('age'),
        sumAge: konro.sum('age'),
      });
      
      expect(result.count).toBe(2);
      expect(result.avgAge).toBe(25);
      expect(result.sumAge).toBe(50);
    });
  });

  describe('ID Generation', () => {
    it('should auto-increment IDs for new CSV files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'csv_db');
      const adapter = konro.createFileAdapter({
        format: 'csv',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const user1 = await db.insert('users', { name: 'CSV User 1', email: 'csv1@test.com', age: 20 });
      expect(user1.id).toBe(1);

      const user2 = await db.insert('users', { name: 'CSV User 2', email: 'csv2@test.com', age: 21 });
      expect(user2.id).toBe(2);

      // Verify file content
      const userFilePath = path.join(dbDirPath, 'users.csv');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsed = papaparse.parse(fileContent, { header: true, dynamicTyping: true, skipEmptyLines: true });
      expect(parsed.data).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 1, name: 'CSV User 1', email: 'csv1@test.com', age: 20, isActive: true }),
          expect.objectContaining({ id: 2, name: 'CSV User 2', email: 'csv2@test.com', age: 21, isActive: true }),
        ])
      );
    });

    it('should auto-increment IDs for new XLSX files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'xlsx_db');
      const adapter = konro.createFileAdapter({
        format: 'xlsx',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const user1 = await db.insert('users', { name: 'XLSX User 1', email: 'xlsx1@test.com', age: 20 });
      expect(user1.id).toBe(1);

      const user2 = await db.insert('users', { name: 'XLSX User 2', email: 'xlsx2@test.com', age: 21 });
      expect(user2.id).toBe(2);

      // Verify file content
      const userFilePath = path.join(dbDirPath, 'users.xlsx');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const workbook = xlsx.read(fileContent, { type: 'base64' });
      const sheetName = workbook.SheetNames[0];
      expect(sheetName).toBeDefined();
      const worksheet = workbook.Sheets[sheetName!];
      expect(worksheet).toBeDefined();
      const data = xlsx.utils.sheet_to_json(worksheet!);
      expect(data).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 1, name: 'XLSX User 1', email: 'xlsx1@test.com', age: 20, isActive: true }),
          expect.objectContaining({ id: 2, name: 'XLSX User 2', email: 'xlsx2@test.com', age: 21, isActive: true }),
        ])
      );
    });

    it('should determine lastId from existing CSV files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'csv_db_read');
      const userFilePath = path.join(dbDirPath, 'users.csv');

      // Manually create a CSV with existing data
      await fs.mkdir(dbDirPath, { recursive: true });
      const initialCsv = papaparse.unparse([{ id: 5, name: 'Existing User', email: 'existing@test.com', age: 50, isActive: true }]);
      await fs.writeFile(userFilePath, initialCsv);

      const adapter = konro.createFileAdapter({ format: 'csv', mode: 'on-demand', multi: { dir: dbDirPath } });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const newUser = await db.insert('users', { name: 'New CSV User', email: 'newcsv@test.com', age: 25 });
      expect(newUser.id).toBe(6);
    });

    it('should determine lastId from existing XLSX files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'xlsx_db_read');
      const userFilePath = path.join(dbDirPath, 'users.xlsx');

      // Manually create an XLSX with existing data
      await fs.mkdir(dbDirPath, { recursive: true });
      const initialData = [{ id: 10, name: 'Existing XLSX User', email: 'existing_xlsx@test.com', age: 60, isActive: false }];
      const worksheet = xlsx.utils.json_to_sheet(initialData);
      const workbook = xlsx.utils.book_new();
      xlsx.utils.book_append_sheet(workbook, worksheet, 'data');
      const fileContent = xlsx.write(workbook, { bookType: 'xlsx', type: 'base64' });
      await fs.writeFile(userFilePath, fileContent, 'utf-8');

      const adapter = konro.createFileAdapter({ format: 'xlsx', mode: 'on-demand', multi: { dir: dbDirPath } });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const newUser = await db.insert('users', { name: 'New XLSX User', email: 'newxlsx@test.com', age: 35 });
      expect(newUser.id).toBe(11);
    });

    it('should generate UUIDs for id column', async () => {
      const dbDirPath = path.join(TEST_DIR, 'uuid_db');
      const adapter = konro.createFileAdapter({
        format: 'yaml',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({ schema: uuidTestSchema, adapter });

      const user = await db.insert('uuid_users', { name: 'UUID User' });
      expect(typeof user.id).toBe('string');
      expect(user.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);

      const fileContent = await fs.readFile(path.join(dbDirPath, 'uuid_users.yaml'), 'utf-8');
      const parsed = yaml.load(fileContent) as any;
      expect(parsed.records[0].id).toBe(user.id);
    });
  });
});
```

## File: test/integration/Adapters/Read.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';
import { KonroStorageError } from '../../../src/utils/error.util';

describe('Integration > Adapters > Read', () => {

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  describe('SingleFileJson', () => {
    const dbFilePath = path.join(TEST_DIR, 'read_test.json');
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: dbFilePath },
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    it('should correctly read and parse a single JSON file', async () => {
      const state = db.createEmptyState();
      state.users.records.push({ id: 1, name: 'Reader', email: 'reader@test.com', age: 30, isActive: true });
      state.users.meta.lastId = 1;
      await fs.writeFile(dbFilePath, JSON.stringify(state, null, 2));

      const readState = await db.read();
      expect(readState.users.records.length).toBe(1);
      expect(readState.users.records[0]?.name).toBe('Reader');
      expect(readState.users.meta.lastId).toBe(1);
    });

    it('should return an empty state if the file does not exist', async () => {
      const readState = await db.read();
      expect(readState).toEqual(db.createEmptyState());
    });

    it('should throw KonroStorageError for a corrupt JSON file', async () => {
      await fs.writeFile(dbFilePath, '{ "users": { "records": ['); // Invalid JSON
      await expect(db.read()).rejects.toThrow(KonroStorageError);
    });
  });

  describe('MultiFileYaml', () => {
    const dbDirPath = path.join(TEST_DIR, 'read_yaml_test');
    const adapter = konro.createFileAdapter({
      format: 'yaml',
      multi: { dir: dbDirPath },
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    it('should correctly read and parse multiple YAML files', async () => {
      const state = db.createEmptyState();
      state.users.records.push({ id: 1, name: 'YamlReader', email: 'yaml@test.com', age: 31, isActive: true });
      state.users.meta.lastId = 1;
      state.posts.records.push({ id: 1, title: 'Yaml Post', content: '...', authorId: 1, publishedAt: new Date() });
      state.posts.meta.lastId = 1;

      await fs.mkdir(dbDirPath, { recursive: true });
      await fs.writeFile(path.join(dbDirPath, 'users.yaml'), yaml.dump({ records: state.users.records, meta: state.users.meta }));
      await fs.writeFile(path.join(dbDirPath, 'posts.yaml'), yaml.dump({ records: state.posts.records, meta: state.posts.meta }));
      
      const readState = await db.read();
      expect(readState.users.records.length).toBe(1);
      expect(readState.users.records[0]?.name).toBe('YamlReader');
      expect(readState.posts.records.length).toBe(1);
      expect(readState.posts.records[0]?.title).toBe('Yaml Post');
      expect(readState.tags.records.length).toBe(0); // Ensure non-existent files are handled
    });

    it('should return an empty state if the directory does not exist', async () => {
      const readState = await db.read();
      expect(readState).toEqual(db.createEmptyState());
    });
  });
});
```

## File: test/integration/Adapters/SingleFileJson.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';

describe('Integration > Adapters > SingleFileJson', () => {
  const dbFilePath = path.join(TEST_DIR, 'db.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  it('should correctly write the DatabaseState to a single JSON file', async () => {
    let state = db.createEmptyState();
    [state] = db.insert(state, 'users', {
      name: 'JSON User',
      email: 'json@test.com',
      age: 33,
    });

    await db.write(state);

    const fileExists = await fs.access(dbFilePath).then(() => true).catch(() => false);
    expect(fileExists).toBe(true);

    const fileContent = await fs.readFile(dbFilePath, 'utf-8');
    const parsedContent = JSON.parse(fileContent);

    expect(parsedContent.users.records.length).toBe(1);
    expect(parsedContent.users.records[0].name).toBe('JSON User');
    expect(parsedContent.users.meta.lastId).toBe(1);
    expect(parsedContent.posts.records.length).toBe(0);
  });

  it('should correctly serialize complex data types like dates', async () => {
    let state = db.createEmptyState();
    const testDate = new Date('2023-10-27T10:00:00.000Z');

    [state] = db.insert(state, 'posts', {
      title: 'Dated Post',
      content: '...',
      authorId: 1,
      // override default
      publishedAt: testDate,
    });

    await db.write(state);

    const fileContent = await fs.readFile(dbFilePath, 'utf-8');
    const parsedContent = JSON.parse(fileContent);

    expect(parsedContent.posts.records[0].publishedAt).toBe(testDate.toISOString());
  });
});
```

## File: test/integration/DBContext/Initialization.test.ts
```typescript
import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema } from '../../util';
import path from 'path';

describe('Integration > DBContext > Initialization', () => {
  it('should successfully create a db context with a valid schema and adapter', () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });

    const db = konro.createDatabase({
      schema: testSchema,
      adapter: adapter,
    });

    expect(db).toBeDefined();
    expect(db.schema).toEqual(testSchema);
    expect(db.adapter).toBe(adapter);
    expect(typeof db.read).toBe('function');
    expect(typeof db.write).toBe('function');
    expect(typeof db.insert).toBe('function');
    expect(typeof db.update).toBe('function');
    expect(typeof db.delete).toBe('function');
    expect(typeof db.query).toBe('function');
  });

  it('should correctly generate a pristine, empty DatabaseState object via db.createEmptyState()', () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });
    const db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });

    const emptyState = db.createEmptyState();

    expect(emptyState).toEqual({
      users: { records: [], meta: { lastId: 0 } },
      posts: { records: [], meta: { lastId: 0 } },
      profiles: { records: [], meta: { lastId: 0 } },
      tags: { records: [], meta: { lastId: 0 } },
      posts_tags: { records: [], meta: { lastId: 0 } },
    });
  });

  it('should have the full schema definition available at db.schema for direct reference in queries', () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });
    const db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });

    // Example of using db.schema to reference a column definition
    const userEmailColumn = db.schema.tables.users.email;
    expect(userEmailColumn).toEqual(testSchema.tables.users.email);
    expect(userEmailColumn.dataType).toBe('string');
  });
});
```

## File: test/integration/InMemoryFlow/CrudCycle.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema } from '../../util';
import path from 'path';
import type { InMemoryDbContext } from '../../../src/db';
import type { DatabaseState } from '../../../src/types';

describe('Integration > InMemoryFlow > CrudCycle', () => {
  let db: InMemoryDbContext<typeof testSchema>;
  let state: DatabaseState<typeof testSchema>;

  beforeEach(() => {
    // Adapter is needed for context creation, but we won't use its I/O
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });
    db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });
    state = db.createEmptyState();
  });

  it('should allow inserting a record and then immediately querying for it', () => {
    const [newState, insertedUser] = db.insert(state, 'users', {
      name: 'InMemory Alice',
      email: 'alice@inmemory.com',
      age: 30,
      isActive: true,
    });
    expect(insertedUser.id).toBe(1);

    const users = db.query(newState).from('users').all();
    expect(users.length).toBe(1);
    expect(users[0]).toEqual(insertedUser);
  });

  it('should correctly chain mutation operations by passing the newState', () => {
    // Insert user
    const [stateAfterUserInsert, user] = db.insert(state, 'users', {
      name: 'Chain User',
      email: 'chain@test.com',
      age: 40,
      isActive: true,
    });

    // Insert post using the new state
    const [stateAfterPostInsert, post] = db.insert(stateAfterUserInsert, 'posts', {
      title: 'Chained Post',
      content: '...',
      authorId: user.id,
      publishedAt: new Date(),
    });

    expect(stateAfterPostInsert.users.records.length).toBe(1);
    expect(stateAfterPostInsert.posts.records.length).toBe(1);
    expect(post.authorId).toBe(user.id);
  });

  it('should update a record and verify the change in the returned newState', () => {
    const [stateAfterInsert, user] = db.insert(state, 'users', {
      name: 'Update Me',
      email: 'update@test.com',
      age: 50,
      isActive: true,
    });

    const [stateAfterUpdate, updatedUsers] = db.update(stateAfterInsert, 'users')
      .set({ name: 'Updated Name' })
      .where({ id: user.id });

    expect(updatedUsers.length).toBe(1);
    expect(updatedUsers[0]?.name).toBe('Updated Name');

    const queriedUser = db.query(stateAfterUpdate).from('users').where({ id: user.id }).first();
    expect(queriedUser?.name).toBe('Updated Name');
    expect(stateAfterInsert.users.records[0]?.name).toBe('Update Me'); // Original state is untouched
  });

  it('should delete a record and verify its absence in the returned newState', () => {
    const [stateAfterInsert, user] = db.insert(state, 'users', {
      name: 'Delete Me',
      email: 'delete@test.com',
      age: 60,
      isActive: true,
    });

    const [stateAfterDelete, deletedUsers] = db.delete(stateAfterInsert, 'users')
      .where({ id: user.id });

    expect(deletedUsers.length).toBe(1);
    expect(deletedUsers[0]?.name).toBe('Delete Me');

    const users = db.query(stateAfterDelete).from('users').all();
    expect(users.length).toBe(0);
  });

  it('should correctly execute a query with a .with() clause on an in-memory state', () => {
    const [s1, user] = db.insert(state, 'users', {
      name: 'Relation User',
      email: 'relation@test.com',
      age: 35,
      isActive: true,
    });
    const [s2, ] = db.insert(s1, 'posts', [
        { title: 'Relational Post 1', content: '...', authorId: user.id, publishedAt: new Date() },
        { title: 'Relational Post 2', content: '...', authorId: user.id, publishedAt: new Date() },
    ]);

    const userWithPosts = db.query(s2).from('users').where({ id: user.id }).with({ posts: true }).first();

    expect(userWithPosts).toBeDefined();
    expect(userWithPosts?.name).toBe('Relation User');
    expect(userWithPosts?.posts).toBeInstanceOf(Array);
    expect(userWithPosts?.posts?.length).toBe(2);
    expect(userWithPosts?.posts?.[0]?.title).toBe('Relational Post 1');
  });
});
```

## File: test/integration/Types/InferredTypes.test-d.ts
```typescript
import { describe, it } from 'bun:test';
import { konro } from '../../konro-test-import';
import { schemaDef } from '../../util';

/**
 * NOTE: This is a type definition test file.
 * It is not meant to be run, but to be checked by `tsc`.
 * The presence of `// @ts-expect-error` comments indicates
 * that a TypeScript compilation error is expected on the next line.
 * If the error does not occur, `tsc` will fail, which is the desired behavior for this test.
 */
describe('Integration > Types > InferredTypes', () => {
  it('should pass type checks', () => {
    const testSchema = konro.createSchema(schemaDef);
    type User = typeof testSchema.types.users;

    // Test 1: Inferred User type should have correct primitive and relational fields.
    const user: User = {
      id: 1,
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
      isActive: true,
      posts: [{
        id: 1,
        title: 'Post 1',
        content: '...',
        authorId: 1,
        publishedAt: new Date(),
      }],
      profile: null,
    };

    // This should be valid
    user.name; // Accessing for type check
    const inMemoryAdapter = konro.createFileAdapter({ format: 'json', single: { filepath: 'dummy.json' }});
    const db = konro.createDatabase({ schema: testSchema, adapter: inMemoryAdapter });
    const state = db.createEmptyState(); // For in-memory db

    // Test 2: Should cause a TS error if a non-existent field is used in a where clause.
    // @ts-expect-error - 'nonExistentField' does not exist on type 'User'.
    db.query(state).from('users').where({ nonExistentField: 'value' });

    // This should be valid
    db.query(state).from('users').where({ name: 'Alice' });

    // Test 3: Should cause a TS error if a wrong type is passed to db.insert().
    // @ts-expect-error - 'age' should be a number, not a string.
    db.insert(state, 'users', { name: 'Bob', email: 'bob@test.com', age: 'twenty-five' });

    // This should be valid - using type assertion for test-only code
    // @ts-ignore - This is a type test only, not runtime code
    db.insert(state, 'users', { name: 'Bob', email: 'bob@test.com', age: 25 });

    // Test 4: Nested .with clause on in-memory db should be typed correctly
    db.query(state).from('users').with({
      posts: {
        where: (post) => post.title.startsWith('A') // post is typed as Post
      }
    }).first();

    // @ts-expect-error - 'nonExistentRelation' is not a valid relation on 'users'
    db.query(state).from('users').with({ nonExistentRelation: true });

    // Test 5: A query without .with() should return the base type, without relations.
    const baseUser = db.query(state).from('users').where({ id: 1 }).first();
    // This should be valid
    baseUser?.name;
    // @ts-expect-error - 'posts' does not exist on base user type, as .with() was not used.
    baseUser?.posts;

    // Test 6: A query with .with() should return the relations, which are now accessible.
    const userWithPosts = db.query(state).from('users').where({ id: 1 }).with({ posts: true }).first();
    userWithPosts?.posts; // This should be valid and typed as Post[] | undefined
    
    // userWithPosts?.posts?.[0]?.author; 

    // --- On-Demand DB Type Tests ---
    const onDemandAdapter = konro.createFileAdapter({ format: 'yaml', mode: 'on-demand', multi: { dir: 'dummy-dir' }});
    const onDemandDb = konro.createDatabase({ schema: testSchema, adapter: onDemandAdapter });

    // Test 7: On-demand query should not require state.
    onDemandDb.query().from('users').where({ name: 'Alice' }).first(); // Should be valid

    // Test 8: On-demand query with .with() should be typed correctly without state.
    onDemandDb.query().from('users').with({
      posts: {
        where: (post) => post.title.startsWith('A')
      }
    }).first();

    // @ts-expect-error - 'nonExistentRelation' is not a valid relation on 'users'
    onDemandDb.query().from('users').with({ nonExistentRelation: true });

    // Test 9: On-demand insert should be awaitable and return the correct type.
    const insertedUserPromise = onDemandDb.insert('users', { name: 'OnDemand', email: 'od@test.com', age: 22 });
    // @ts-expect-error - 'posts' should not exist on the base inserted type
    insertedUserPromise.then(u => u.posts);
  });
});
```

## File: test/unit/Core/Aggregate.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _aggregateImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';
import { konro } from '../../konro-test-import';

describe('Unit > Core > Aggregate', () => {
  let testState: DatabaseState;

  beforeEach(() => {
    testState = {
      users: {
        records: [
          { id: 1, name: 'Alice', age: 30, isActive: true },
          { id: 2, name: 'Bob', age: 25, isActive: true },
          { id: 3, name: 'Charlie', age: 42, isActive: false },
          { id: 4, name: 'Denise', age: 30, isActive: true },
          { id: 5, name: 'Edward', age: null, isActive: true }, // age can be null
        ],
        meta: { lastId: 5 },
      },
      posts: { records: [], meta: { lastId: 0 } },
      profiles: { records: [], meta: { lastId: 0 } },
      tags: { records: [], meta: { lastId: 0 } },
      posts_tags: { records: [], meta: { lastId: 0 } },
    };
  });

  it('should correctly count all records in a table', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      aggregations: { total: konro.count() }
    });
    expect(result.total).toBe(5);
  });

  it('should correctly count records matching a where clause', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      where: (r) => r.isActive === true,
      aggregations: { activeUsers: konro.count() }
    });
    expect(result.activeUsers).toBe(4);
  });

  it('should correctly sum a numeric column', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      aggregations: { totalAge: konro.sum('age') }
    });
    // 30 + 25 + 42 + 30 = 127
    expect(result.totalAge).toBe(127);
  });

  it('should correctly calculate the average of a numeric column', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      aggregations: { averageAge: konro.avg('age') }
    });
    // 127 / 4 = 31.75
    expect(result.averageAge).toBe(31.75);
  });

  it('should find the minimum value in a numeric column', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      aggregations: { minAge: konro.min('age') }
    });
    expect(result.minAge).toBe(25);
  });

  it('should find the maximum value in a numeric column', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      aggregations: { maxAge: konro.max('age') }
    });
    expect(result.maxAge).toBe(42);
  });

  it('should handle multiple aggregations in one call', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      where: (r) => r.isActive === true,
      aggregations: {
        count: konro.count(),
        avgAge: konro.avg('age'), // Alice(30), Bob(25), Denise(30) -> 85 / 3
      }
    });
    expect(result.count).toBe(4); // Includes Edward with null age
    expect(result.avgAge).toBeCloseTo(85 / 3);
  });

  it('should return 0 for count on an empty/filtered-out set', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      where: (r) => (r.age as number) > 100,
      aggregations: { count: konro.count() }
    });
    expect(result.count).toBe(0);
  });

  it('should return 0 for sum on an empty set', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      where: (r) => (r.age as number) > 100,
      aggregations: { sumAge: konro.sum('age') }
    });
    expect(result.sumAge).toBe(0);
  });

  it('should return null for avg, min, and max on an empty set', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      where: (r) => (r.age as number) > 100,
      aggregations: {
        avgAge: konro.avg('age'),
        minAge: konro.min('age'),
        maxAge: konro.max('age'),
      }
    });
    expect(result.avgAge).toBeNull();
    expect(result.minAge).toBeNull();
    expect(result.maxAge).toBeNull();
  });

  it('should ignore non-numeric and null values in calculations', () => {
    const result = _aggregateImpl(testState, testSchema, {
      tableName: 'users',
      aggregations: {
        count: konro.count(),
        sum: konro.sum('age'),
        avg: konro.avg('age'),
        min: konro.min('age'),
        max: konro.max('age'),
      }
    });
    // There are 5 users, but only 4 have numeric ages.
    // The implementation of avg/sum/min/max filters for numbers.
    // The count is for all records matching where.
    expect(result.count).toBe(5);
    expect(result.sum).toBe(127);
    expect(result.avg).toBe(31.75);
    expect(result.min).toBe(25);
    expect(result.max).toBe(42);
  });
});
```

## File: test/unit/Core/Delete.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { _deleteImpl } from '../../../src/operations';
import { DatabaseState, KRecord } from '../../../src/types';
import { konro } from '../../konro-test-import';

describe('Unit > Core > Delete', () => {
    let testState: DatabaseState;

    const hardDeleteSchema = konro.createSchema({
        tables: {
            users: {
                id: konro.id(),
                name: konro.string(),
                email: konro.string(),
                age: konro.number(),
            },
            posts: {
                id: konro.id(),
                title: konro.string(),
                userId: konro.number()
            },
            profiles: { id: konro.id(), bio: konro.string(), userId: konro.number() },
            tags: { id: konro.id(), name: konro.string() },
            posts_tags: { id: konro.id(), postId: konro.number(), tagId: konro.number() },
        },
        relations: () => ({
            users: {
                posts: konro.many('posts', { on: 'id', references: 'userId', onDelete: 'CASCADE' })
            }
        })
    });
    
    const softDeleteSchema = konro.createSchema({
        tables: {
            users: {
                id: konro.id(),
                name: konro.string(),
                email: konro.string(),
                age: konro.number(),
                deletedAt: konro.deletedAt()
            },
            posts: {
                id: konro.id(),
                title: konro.string(),
                userId: konro.number()
            },
            profiles: { id: konro.id(), bio: konro.string(), userId: konro.number() },
            tags: { id: konro.id(), name: konro.string() },
            posts_tags: { id: konro.id(), postId: konro.number(), tagId: konro.number() },
        },
        relations: () => ({
            users: {
                posts: konro.many('posts', { on: 'id', references: 'userId', onDelete: 'CASCADE' })
            }
        })
    });

    beforeEach(() => {
        testState = {
            users: {
                records: [
                    { id: 1, name: 'Alice', email: 'a@a.com', age: 30, deletedAt: null },
                    { id: 2, name: 'Bob', email: 'b@b.com', age: 25, deletedAt: null },
                    { id: 3, name: 'Charlie', email: 'c@c.com', age: 42, deletedAt: null },
                ],
                meta: { lastId: 3 },
            },
            posts: { 
                records: [
                    { id: 101, title: 'Post A', userId: 1 },
                    { id: 102, title: 'Post B', userId: 2 },
                    { id: 103, title: 'Post C', userId: 1 },
                ], 
                meta: { lastId: 103 } 
            },
            profiles: { records: [], meta: { lastId: 0 } },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    it('should return a new state object, not mutate the original state, on hard delete', () => {
        const originalState = structuredClone(testState);
        const [newState] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => r.id === 1);
        
        expect(newState).not.toBe(originalState);
        expect(originalState.users!.records.length).toBe(3);
        expect(newState.users!.records.length).toBe(2);
    });

    it('should only hard delete records that match the predicate function', () => {
        const [newState, deleted] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => typeof r.age === 'number' && r.age > 35);
        
        expect(deleted.length).toBe(1);
        expect(deleted[0]!.id).toBe(3);
        expect(newState.users!.records.length).toBe(2);
        expect(newState.users!.records.find(u => u.id === 3)).toBeUndefined();
    });

    it('should return both the new state and an array of the full, hard-deleted records in the result tuple', () => {
        const [newState, deleted] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => r.id === 2);

        expect(newState).toBeDefined();
        expect(deleted).toBeInstanceOf(Array);
        expect(deleted.length).toBe(1);
        expect(deleted[0]!).toEqual({ id: 2, name: 'Bob', email: 'b@b.com', age: 25, deletedAt: null });
    });

    it('should not modify the table meta lastId on delete', () => {
        const [newState] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => r.id === 3);
        expect(newState.users!.meta.lastId).toBe(3);
    });

    it('should soft delete a record by setting deletedAt if the column exists in schema', () => {
        const [newState, deleted] = _deleteImpl(testState, softDeleteSchema, 'users', (r) => r.id === 2);

        expect(newState.users!.records.length).toBe(3); // Record is not removed
        const deletedUser = newState.users!.records.find(u => u.id === 2);
        expect(deletedUser?.deletedAt).toBeInstanceOf(Date);
        
        expect(deleted.length).toBe(1);
        expect(deleted[0]!.id).toBe(2);
        expect(deleted[0]!.deletedAt).toEqual(deletedUser?.deletedAt);
    });

    it('should not soft delete an already soft-deleted record', () => {
        (testState.users!.records[1] as KRecord).deletedAt = new Date('2024-01-01');
        const [newState, deleted] = _deleteImpl(testState, softDeleteSchema, 'users', (r) => r.id === 2);

        expect(newState).toBe(testState); // Should return original state as nothing changed
        expect(deleted.length).toBe(0);
        expect((newState.users!.records[1] as KRecord).deletedAt).toEqual(new Date('2024-01-01'));
    });

    it('should perform a cascading delete on related records', () => {
        const [newState, deletedUsers] = _deleteImpl(testState, softDeleteSchema, 'users', (r) => r.id === 1);
        
        expect(deletedUsers.length).toBe(1);
        expect(newState.users!.records.find(u => u.id === 1)?.deletedAt).toBeInstanceOf(Date);
        
        // Check that posts by user 1 are also gone (hard delete, as posts have no deletedAt)
        const postsForUser1 = newState.posts!.records.filter(p => p.userId === 1);
        expect(postsForUser1.length).toBe(0);

        // Check that other posts are unaffected
        expect(newState.posts!.records.length).toBe(1);
        expect(newState.posts!.records[0]!.id).toBe(102);
    });
});
```

## File: test/unit/Core/Insert.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _insertImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';

describe('Unit > Core > Insert', () => {
    let emptyState: DatabaseState;

    beforeEach(() => {
        emptyState = {
            users: { records: [], meta: { lastId: 0 } },
            posts: { records: [], meta: { lastId: 10 } },
            profiles: { records: [], meta: { lastId: 0 } },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    it('should return a new state object, not mutate the original state, on insert', () => {
        const originalState = structuredClone(emptyState);
        const [newState] = _insertImpl(emptyState, testSchema, 'users', [{ name: 'Test', email: 'test@test.com', age: 25 }]);
        
        expect(newState).not.toBe(originalState);
        expect(originalState.users!.records.length).toBe(0);
        expect(newState.users!.records.length).toBe(1);
    });

    it('should correctly increment the lastId in the table meta', () => {
        const [newState] = _insertImpl(emptyState, testSchema, 'users', [{ name: 'Test', email: 'test@test.com', age: 25 }]);
        expect(newState.users!.meta.lastId).toBe(1);

        const [finalState] = _insertImpl(newState, testSchema, 'users', [{ name: 'Test2', email: 'test2@test.com', age: 30 }]);
        expect(finalState.users!.meta.lastId).toBe(2);
    });

    it('should assign the new id to the inserted record', () => {
        const [newState, inserted] = _insertImpl(emptyState, testSchema, 'posts', [{ title: 'My Post', content: '...', authorId: 1 }]);
        expect(newState.posts!.meta.lastId).toBe(11);
        expect(inserted[0]!.id).toBe(11);
        expect(newState.posts!.records[0]!.id).toBe(11);
    });

    it('should apply default values for fields that are not provided', () => {
        const [newState, inserted] = _insertImpl(emptyState, testSchema, 'users', [{ name: 'Default User', email: 'default@test.com', age: 30 }]);
        expect(inserted[0]!.isActive).toBe(true);
        expect(newState.users!.records[0]!.isActive).toBe(true);
    });

    it('should apply default values from a function call, like for dates', () => {
        const before = new Date();
        const [newState, inserted] = _insertImpl(emptyState, testSchema, 'posts', [{ title: 'Dated Post', content: '...', authorId: 1 }]);
        const after = new Date();

        const publishedAt = inserted[0]!.publishedAt as Date;
        expect(publishedAt).toBeInstanceOf(Date);
        expect(publishedAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
        expect(publishedAt.getTime()).toBeLessThanOrEqual(after.getTime());
        expect(newState.posts!.records[0]!.publishedAt).toEqual(publishedAt);
    });

    it('should successfully insert multiple records in a single call', () => {
        const usersToInsert = [
            { name: 'User A', email: 'a@test.com', age: 21 },
            { name: 'User B', email: 'b@test.com', age: 22 },
        ];
        const [newState, inserted] = _insertImpl(emptyState, testSchema, 'users', usersToInsert);

        expect(newState.users!.records.length).toBe(2);
        expect(inserted.length).toBe(2);
        expect(newState.users!.meta.lastId).toBe(2);
        expect(inserted[0]!.id).toBe(1);
        expect(inserted[1]!.id).toBe(2);
        expect(inserted[0]!.name).toBe('User A');
        expect(inserted[1]!.name).toBe('User B');
    });

    it('should return both the new state and the newly created record(s) in the result tuple', () => {
        const userToInsert = { name: 'Single', email: 'single@test.com', age: 40 };
        const [newState, inserted] = _insertImpl(emptyState, testSchema, 'users', [userToInsert]);
        
        expect(newState).toBeDefined();
        expect(inserted).toBeInstanceOf(Array);
        expect(inserted.length).toBe(1);
        expect(inserted[0]!.name).toBe('Single');
        expect(inserted[0]!.id).toBe(1);
    });
});
```

## File: test/unit/Core/Query-With.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _queryImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';

describe('Unit > Core > Query-With', () => {
    let testState: DatabaseState;

    beforeEach(() => {
        testState = {
            users: {
                records: [
                    { id: 1, name: 'Alice' },
                    { id: 2, name: 'Bob' },
                ],
                meta: { lastId: 2 },
            },
            posts: {
                records: [
                    { id: 10, title: 'Alice Post 1', authorId: 1 },
                    { id: 11, title: 'Bob Post 1', authorId: 2 },
                    { id: 12, title: 'Alice Post 2', authorId: 1 },
                ],
                meta: { lastId: 12 },
            },
            profiles: {
                records: [
                    { id: 100, bio: 'Bio for Alice', userId: 1 },
                ],
                meta: { lastId: 100 },
            },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    it('should resolve a `one` relationship and attach it to the parent record', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'posts',
            where: r => r.id === 10,
            with: { author: true }
        });

        expect(results.length).toBe(1);
        const post = results[0]!;
        expect(post).toBeDefined();
        const author = post.author as {id: unknown, name: unknown};
        expect(author).toBeDefined();
        expect(author.id).toBe(1);
        expect(author.name).toBe('Alice');
    });

    it('should resolve a `many` relationship and attach it as an array', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: { posts: true }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        expect(user).toBeDefined();
        const posts = user.posts as {title: unknown}[];
        expect(posts).toBeInstanceOf(Array);
        expect(posts.length).toBe(2);
        expect(posts[0]!.title).toBe('Alice Post 1');
        expect(posts[1]!.title).toBe('Alice Post 2');
    });

    it('should filter nested records within a .with() clause', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: {
                posts: {
                    where: (post) => typeof post.title === 'string' && post.title.includes('Post 2')
                }
            }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        const posts = user.posts as {id: unknown}[];
        expect(posts).toBeDefined();
        expect(posts.length).toBe(1);
        expect(posts[0]!.id).toBe(12);
    });

    it('should select nested fields within a .with() clause', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: {
                posts: {
                    select: {
                        postTitle: testSchema.tables.posts.title
                    }
                }
            }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        const posts = user.posts as {postTitle: unknown}[];
        expect(posts).toBeDefined();
        expect(posts.length).toBe(2);
        expect(posts[0]!).toEqual({ postTitle: 'Alice Post 1' });
    });

    it('should handle multiple relations at once', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: {
                posts: true,
                profile: true
            }
        });
        
        expect(results.length).toBe(1);
        const user = results[0]!;
        const posts = user.posts as unknown[];
        const profile = user.profile as { bio: unknown };
        expect(posts).toBeInstanceOf(Array);
        expect(posts.length).toBe(2);
        expect(profile).toBeDefined();
        expect(profile.bio).toBe('Bio for Alice');
    });

    it('should return null for a `one` relation if no related record is found', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 2, // Bob has no profile
            with: { profile: true }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        expect(user.profile).toBeNull();
    });

    it('should return an empty array for a `many` relation if no related records are found', () => {
        // Add a user with no posts
        testState.users!.records.push({ id: 3, name: 'Charlie' });
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 3,
            with: { posts: true }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        expect(user.posts).toBeInstanceOf(Array);
        expect((user.posts as unknown[]).length).toBe(0);
    });

    it('should handle nested `with` clauses for deep relations', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'posts',
            where: r => r.id === 10, // Alice Post 1
            with: {
                author: { // author is a user
                    with: {
                        posts: { // author's other posts
                            where: p => p.id === 12 // Filter to Alice Post 2
                        }
                    }
                }
            }
        });

        expect(results.length).toBe(1);
        const post = results[0]!;
        expect(post.id).toBe(10);

        const author = post.author as { id: unknown, name: unknown, posts: { id: unknown }[] };
        expect(author).toBeDefined();
        expect(author.id).toBe(1);
        expect(author.name).toBe('Alice');

        const authorPosts = author.posts;
        expect(authorPosts).toBeInstanceOf(Array);
        expect(authorPosts.length).toBe(1);
        expect(authorPosts[0]!.id).toBe(12);
    });
});
```

## File: test/unit/Core/Query.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _queryImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';

describe('Unit > Core > Query', () => {
    let testState: DatabaseState;

    beforeEach(() => {
        testState = {
            users: {
                records: [
                    { id: 1, name: 'Alice', age: 30, isActive: true },
                    { id: 2, name: 'Bob', age: 25, isActive: true },
                    { id: 3, name: 'Charlie', age: 42, isActive: false },
                    { id: 4, name: 'Denise', age: 30, isActive: true },
                ],
                meta: { lastId: 4 },
            },
            posts: { records: [], meta: { lastId: 0 } },
            profiles: { records: [], meta: { lastId: 0 } },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    it('should select all fields from a table when .select() is omitted', () => {
        const results = _queryImpl(testState, testSchema, { tableName: 'users' });
        expect(results.length).toBe(4);
        expect(results[0]!).toEqual({ id: 1, name: 'Alice', age: 30, isActive: true });
        expect(Object.keys(results[0]!).length).toBe(4);
    });

    it('should select only the specified fields when using .select()', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            select: {
                name: testSchema.tables.users.name,
                age: testSchema.tables.users.age
            }
        });
        expect(results.length).toBe(4);
        expect(results[0]!).toEqual({ name: 'Alice', age: 30 });
        expect(Object.keys(results[0]!).length).toBe(2);
    });

    it('should filter records correctly using a where function', () => {
        const results = _queryImpl(testState, testSchema, { tableName: 'users', where: (r) => r.age === 30 });
        expect(results.length).toBe(2);
        expect(results[0]!.name).toBe('Alice');
        expect(results[1]!.name).toBe('Denise');
    });

    it('should limit the number of returned records correctly using .limit()', () => {
        const results = _queryImpl(testState, testSchema, { tableName: 'users', limit: 2 });
        expect(results.length).toBe(2);
        expect(results[0]!.id).toBe(1);
        expect(results[1]!.id).toBe(2);
    });

    it('should skip the correct number of records using .offset()', () => {
        const results = _queryImpl(testState, testSchema, { tableName: 'users', offset: 2 });
        expect(results.length).toBe(2);
        expect(results[0]!.id).toBe(3);
        expect(results[1]!.id).toBe(4);
    });

    it('should correctly handle limit and offset together for pagination', () => {
        const results = _queryImpl(testState, testSchema, { tableName: 'users', offset: 1, limit: 2 });
        expect(results.length).toBe(2);
        expect(results[0]!.id).toBe(2);
        expect(results[1]!.id).toBe(3);
    });

    it('should return an array of all matching records when using .all()', () => {
        // This is implicit in _queryImpl, the test just verifies the base case
        const results = _queryImpl(testState, testSchema, { tableName: 'users', where: r => r.isActive === true });
        expect(results).toBeInstanceOf(Array);
        expect(results.length).toBe(3);
    });

    it('should return the first matching record when using .first()', () => {
        // This is simulated by adding limit: 1
        const results = _queryImpl(testState, testSchema, { tableName: 'users', where: r => typeof r.age === 'number' && r.age > 28, limit: 1 });
        expect(results.length).toBe(1);
        expect(results[0]!.id).toBe(1);
    });

    it('should return null when .first() finds no matching record', () => {
        // This is simulated by _queryImpl returning [] and the caller handling it
        const results = _queryImpl(testState, testSchema, { tableName: 'users', where: r => typeof r.age === 'number' && r.age > 50, limit: 1 });
        expect(results.length).toBe(0);
    });
});
```

## File: test/unit/Core/Update.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _updateImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';

describe('Unit > Core > Update', () => {
    let testState: DatabaseState;

    beforeEach(() => {
        testState = {
            users: {
                records: [
                    { id: 1, name: 'Alice', email: 'a@a.com', age: 30, isActive: true },
                    { id: 2, name: 'Bob', email: 'b@b.com', age: 25, isActive: true },
                    { id: 3, name: 'Charlie', email: 'c@c.com', age: 42, isActive: false },
                ],
                meta: { lastId: 3 },
            },
            posts: { records: [], meta: { lastId: 0 } },
            profiles: { records: [], meta: { lastId: 0 } },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    it('should return a new state object, not mutate the original state, on update', () => {
        const originalState = structuredClone(testState);
        const [newState] = _updateImpl(testState, testSchema, 'users', { age: 31 }, (r) => r.id === 1);
        
        expect(newState).not.toBe(originalState);
        expect(originalState.users!.records[0]!.age).toBe(30);
        expect(newState.users!.records.find(u => u.id === 1)?.age).toBe(31);
    });

    it('should only update records that match the predicate function', () => {
        const [newState, updated] = _updateImpl(testState, testSchema, 'users', { isActive: true }, (r) => r.name === 'Charlie');
        
        expect(updated.length).toBe(1);
        expect(updated[0]!.id).toBe(3);
        expect(updated[0]!.isActive).toBe(true);
        expect(newState.users!.records.find(u => u.id === 3)?.isActive).toBe(true);
        expect(newState.users!.records.find(u => u.id === 1)?.isActive).toBe(true); // Unchanged
    });

    it('should correctly modify the fields specified in the set payload', () => {
        const [newState, updated] = _updateImpl(testState, testSchema, 'users', { age: 26, name: 'Robert' }, (r) => r.id === 2);

        expect(updated.length).toBe(1);
        const updatedUser = newState.users!.records.find(u => u.id === 2);
        expect(updatedUser?.name).toBe('Robert');
        expect(updatedUser?.age).toBe(26);
    });

    it('should not allow changing the id of an updated record', () => {
        const payload = { id: 99, age: 50 };
        const [newState, updated] = _updateImpl(testState, testSchema, 'users', payload, (r) => r.id === 1);
        
        expect(updated.length).toBe(1);
        expect(updated[0]!.id).toBe(1); // The id should remain 1
        expect(updated[0]!.age).toBe(50);
        
        const userInNewState = newState.users!.records.find(u => u.age === 50);
        expect(userInNewState?.id).toBe(1);

        const userWithOldId = newState.users!.records.find(u => u.id === 1);
        expect(userWithOldId).toBeDefined();
        expect(userWithOldId?.age).toBe(50);
        
        const userWithNewId = newState.users!.records.find(u => u.id === 99);
        expect(userWithNewId).toBeUndefined();
    });

    it('should return an empty array of updated records if the predicate matches nothing', () => {
        const [newState, updated] = _updateImpl(testState, testSchema, 'users', { age: 99 }, (r) => r.id === 999);
        expect(updated.length).toBe(0);
        expect(newState.users!.records).toEqual(testState.users!.records);
        // For a no-op, the original state object should be returned for performance.
        expect(newState).toBe(testState);
    });

    it('should return both the new state and an array of the full, updated records in the result tuple', () => {
        const [newState, updated] = _updateImpl(testState, testSchema, 'users', { isActive: false }, (r) => r.id === 1);
        expect(newState).toBeDefined();
        expect(updated).toBeInstanceOf(Array);
        expect(updated.length).toBe(1);
        expect(updated[0]!).toEqual({
            id: 1,
            name: 'Alice',
            email: 'a@a.com',
            age: 30,
            isActive: false,
        });
    });
});
```

## File: test/unit/Schema/ColumnHelpers.test.ts
```typescript
import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';

describe('Unit > Schema > ColumnHelpers', () => {
  it('should create a valid ID column definition object when calling konro.id()', () => {
    const idCol = konro.id();
    expect(idCol).toEqual({
      _type: 'column',
      dataType: 'id',
      options: { unique: true, _pk_strategy: 'auto-increment' },
      _tsType: 0,
    });
  });

  it('should create a valid UUID column definition object when calling konro.uuid()', () => {
    const uuidCol = konro.uuid();
    expect(uuidCol).toEqual({
      _type: 'column',
      dataType: 'id',
      options: { unique: true, _pk_strategy: 'uuid' },
      _tsType: '',
    });
  });

  it('should create a valid string column definition with no options', () => {
    const stringCol = konro.string();
    expect(stringCol).toEqual({
      _type: 'column',
      dataType: 'string',
      options: undefined,
      _tsType: '',
    });
  });

  it('should create a valid string column definition with all specified options', () => {
    const defaultFn = () => 'default';
    const stringCol = konro.string({
      unique: true,
      default: defaultFn,
      min: 5,
      max: 100,
      format: 'email',
    });
    expect(stringCol).toEqual({
      _type: 'column',
      dataType: 'string',
      options: {
        unique: true,
        default: defaultFn,
        min: 5,
        max: 100,
        format: 'email',
      },
      _tsType: '',
    });
  });

  it('should create a valid number column definition with no options', () => {
    const numberCol = konro.number();
    expect(numberCol).toEqual({
      _type: 'column',
      dataType: 'number',
      options: undefined,
      _tsType: 0,
    });
  });

  it('should create a valid number column definition with all specified options', () => {
    const numberCol = konro.number({
      unique: false,
      default: 0,
      min: 0,
      max: 1000,
      type: 'integer',
    });
    expect(numberCol).toEqual({
      _type: 'column',
      dataType: 'number',
      options: {
        unique: false,
        default: 0,
        min: 0,
        max: 1000,
        type: 'integer',
      },
      _tsType: 0,
    });
  });

  it('should create a valid boolean column with no options', () => {
    const boolCol = konro.boolean();
    expect(boolCol).toEqual({
      _type: 'column',
      dataType: 'boolean',
      options: undefined,
      _tsType: false,
    });
  });

  it('should create a valid boolean column definition with a default value', () => {
    const boolCol = konro.boolean({ default: false });
    expect(boolCol).toEqual({
      _type: 'column',
      dataType: 'boolean',
      options: { default: false },
      _tsType: false,
    });
  });

  it('should create a valid date column definition with no options', () => {
    const dateCol = konro.date();
    expect(dateCol).toEqual({
      _type: 'column',
      dataType: 'date',
      options: undefined,
      _tsType: expect.any(Date),
    });
  });

  it('should create a valid date column definition with a default function', () => {
    const defaultDateFn = () => new Date();
    const dateCol = konro.date({ default: defaultDateFn });
    expect(dateCol).toEqual({
      _type: 'column',
      dataType: 'date',
      options: { default: defaultDateFn },
      _tsType: expect.any(Date),
    });
    expect(dateCol.options?.default).toBe(defaultDateFn);
  });

  it('should create a valid string column with a literal default', () => {
    const stringCol = konro.string({ default: 'hello' });
    expect(stringCol).toEqual({
      _type: 'column',
      dataType: 'string',
      options: { default: 'hello' },
      _tsType: '',
    });
  });

  it('should create a valid number column with a function default', () => {
    const defaultFn = () => 42;
    const numberCol = konro.number({ default: defaultFn });
    expect(numberCol).toEqual({
      _type: 'column',
      dataType: 'number',
      options: {
        default: defaultFn,
      },
      _tsType: 0,
    });
    expect(numberCol.options?.default).toBe(defaultFn);
  });

  it('should create a valid boolean column with a function default', () => {
    const defaultFn = () => true;
    const boolCol = konro.boolean({ default: defaultFn });
    expect(boolCol).toEqual({
      _type: 'column',
      dataType: 'boolean',
      options: {
        default: defaultFn,
      },
      _tsType: false,
    });
    expect(boolCol.options?.default).toBe(defaultFn);
  });

  it('should create a valid object column definition', () => {
    const objCol = konro.object<{ meta: string }>();
    expect(objCol).toMatchObject({
      _type: 'column',
      dataType: 'object',
      options: undefined,
    });
  });
});
```

## File: test/unit/Schema/CreateSchema.test.ts
```typescript
import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';

describe('Unit > Schema > CreateSchema', () => {
  it('should correctly assemble a full schema object from tables and relations definitions', () => {
    const tableDefs = {
      users: {
        id: konro.id(),
        name: konro.string(),
      },
      posts: {
        id: konro.id(),
        title: konro.string(),
        authorId: konro.number(),
      },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
      relations: () => ({
        users: {
          posts: konro.many('posts', { on: 'id', references: 'authorId' }),
        },
        posts: {
          author: konro.one('users', { on: 'authorId', references: 'id' }),
        },
      }),
    });

    expect(schema.tables).toBe(tableDefs);
    expect(schema.relations).toBeDefined();
    expect(schema.relations.users.posts).toBeDefined();
    expect(schema.relations.posts.author).toBeDefined();
    expect(schema.types).toBeNull(); // Runtime placeholder
  });

  it('should handle schemas with no relations defined', () => {
    const tableDefs = {
      logs: {
        id: konro.id(),
        message: konro.string(),
      },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
    });

    expect(schema.tables).toBe(tableDefs);
    expect(schema.relations).toEqual({});
  });

  it('should handle schemas where relations function returns an empty object', () => {
    const tableDefs = {
      users: {
        id: konro.id(),
        name: konro.string(),
      },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
      relations: () => ({}),
    });

    expect(schema.tables).toBe(tableDefs);
    expect(schema.relations).toEqual({});
  });

  it('should handle schemas with multiple relations on one table', () => {
    const tableDefs = {
      users: { id: konro.id(), name: konro.string() },
      posts: { id: konro.id(), title: konro.string(), authorId: konro.number(), editorId: konro.number() },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
      relations: () => ({
        posts: {
          author: konro.one('users', { on: 'authorId', references: 'id' }),
          editor: konro.one('users', { on: 'editorId', references: 'id' }),
        },
      }),
    });

    expect(schema.relations.posts.author).toBeDefined();
    expect(schema.relations.posts.editor).toBeDefined();
    expect(schema.relations.posts.author.targetTable).toBe('users');
    expect(schema.relations.posts.editor.targetTable).toBe('users');
  });
});
```

## File: test/unit/Schema/RelationHelpers.test.ts
```typescript
import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';

describe('Unit > Schema > RelationHelpers', () => {
  it('should create a valid one-to-many relationship definition object when calling konro.many()', () => {
    const manyRel = konro.many('posts', { on: 'id', references: 'authorId' });
    expect(manyRel).toEqual({
      _type: 'relation',
      relationType: 'many',
      targetTable: 'posts',
      on: 'id',
      references: 'authorId',
    });
  });

  it('should create a valid one-to-one/many-to-one relationship definition object when calling konro.one()', () => {
    const oneRel = konro.one('users', { on: 'authorId', references: 'id' });
    expect(oneRel).toEqual({
      _type: 'relation',
      relationType: 'one',
      targetTable: 'users',
      on: 'authorId',
      references: 'id',
    });
  });
});
```

## File: test/unit/Validation/Constraints.test.ts
```typescript
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _insertImpl, _updateImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('Unit > Validation > Constraints', () => {
    let testState: DatabaseState;

    beforeEach(() => {
        testState = {
            users: {
                records: [{ id: 1, name: 'Alice', email: 'alice@example.com', age: 30, isActive: true }],
                meta: { lastId: 1 },
            },
            posts: { records: [], meta: { lastId: 0 } },
            profiles: { records: [], meta: { lastId: 0 } },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    // NOTE: These tests are expected to fail until validation is implemented in core operations.
    // This is intentional to highlight the missing functionality as per the test plan.
    
    it('should throw a KonroValidationError when inserting a record with a non-unique value', () => {
        const user = { name: 'Bob', email: 'alice@example.com', age: 25 };
        // This should throw because 'alice@example.com' is already used and `email` is unique.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });

    it('should throw a KonroValidationError for a string that violates a format: email constraint', () => {
        const user = { name: 'Bob', email: 'bob@invalid', age: 25 };
        // This should throw because the email format is invalid.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });

    it('should throw a KonroValidationError for a number smaller than the specified min', () => {
        const user = { name: 'Bob', email: 'bob@example.com', age: 17 }; // age.min is 18
        // This should throw because age is below min.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });

    it('should throw a KonroValidationError for a string shorter than the specified min', () => {
        const user = { name: 'B', email: 'bob@example.com', age: 25 }; // name.min is 2
        // This should throw because name is too short.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });
    
    it('should throw a KonroValidationError on update for a non-unique value', () => {
        // Add another user to create conflict
        testState.users!.records.push({ id: 2, name: 'Charlie', email: 'charlie@example.com', age: 40, isActive: true });
        testState.users!.meta.lastId = 2;

        const predicate = (r: any) => r.id === 2;
        const data = { email: 'alice@example.com' }; // Try to update charlie's email to alice's

        expect(() => _updateImpl(testState, testSchema, 'users', data, predicate)).toThrow(KonroValidationError);
    });
});
```

## File: test/konro-test-import.ts
```typescript
// This file is used to easily switch the import source for 'konro' during testing.
// A script can replace the export line below to target 'src', 'dist', or the 'konro' package.
//
// For example:
// To test against src:       export * from '../src/index';
// To test against dist (mjs): export * from '../dist/index.mjs';
// To test against dist (js):  export * from '../dist/index.js';
// To test against npm package: export * from 'konro';
export * from '../src/index';
// export * from '../dist/index.mjs';
// export * from '../dist/index.js';
```

## File: test/util.ts
```typescript
import { konro } from './konro-test-import';
import { promises as fs } from 'fs';
import path from 'path';

export const TEST_DIR = path.join(__dirname, 'test_run_data');

// --- Schema Definition ---

const tables = {
  users: {
    id: konro.id(),
    name: konro.string({ min: 2 }),
    email: konro.string({ unique: true, format: 'email' }),
    age: konro.number({ min: 18, type: 'integer' }),
    isActive: konro.boolean({ default: true }),
  },
  posts: {
    id: konro.id(),
    title: konro.string(),
    content: konro.string(),
    authorId: konro.number(),
    publishedAt: konro.date({ default: () => new Date() }),
  },
  profiles: {
    id: konro.id(),
    bio: konro.string(),
    userId: konro.number({ unique: true }),
  },
  tags: {
    id: konro.id(),
    name: konro.string({ unique: true }),
  },
  posts_tags: {
    id: konro.id(),
    postId: konro.number(),
    tagId: konro.number(),
  },
};

export const schemaDef = {
  tables,
  relations: (_tables: typeof tables) => ({
    users: {
      posts: konro.many('posts', { on: 'id', references: 'authorId' }),
      profile: konro.one('profiles', { on: 'id', references: 'userId' }),
    },
    posts: {
      author: konro.one('users', { on: 'authorId', references: 'id' }),
      tags: konro.many('posts_tags', { on: 'id', references: 'postId' }),
    },
    profiles: {
      user: konro.one('users', { on: 'userId', references: 'id' }),
    },
    posts_tags: {
      post: konro.one('posts', { on: 'postId', references: 'id' }),
      tag: konro.one('tags', { on: 'tagId', references: 'id' }),
    }
  }),
};

export const testSchema = konro.createSchema(schemaDef);

export type UserCreate = typeof testSchema.create.users;

export const uuidTestSchema = konro.createSchema({
  tables: {
    uuid_users: {
      id: konro.uuid(),
      name: konro.string(),
    },
  },
});

// --- Test Utilities ---

export const cleanup = async () => {
  try {
    await fs.rm(TEST_DIR, { recursive: true, force: true });
  } catch (error: any) {
    if (error.code !== 'ENOENT') {
      console.error('Error during cleanup:', error);
    }
  }
};

export const ensureTestDir = async () => {
  await fs.mkdir(TEST_DIR, { recursive: true });
}
```

## File: tsconfig.build.json
```json
{
  "extends": "./tsconfig.json",
  "compilerOptions": {
    "rootDir": "./src"
  },
  "include": ["src/**/*"],
  "exclude": ["dist/**/*", "test/**/*"]
}
```

## File: tsconfig.json
```json
{
  "compilerOptions": {
    // Environment setup & latest features
    "lib": ["ESNext"],
    "target": "ESNext",
    "module": "ESNext",
    "moduleDetection": "force",
    "allowJs": true,
    "allowSyntheticDefaultImports": true,

    // Output configuration
    "moduleResolution": "node",
    "outDir": "./dist",
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,

    // Best practices
    "strict": true,
    "skipLibCheck": true,
    "noFallthroughCasesInSwitch": true,
    "noUncheckedIndexedAccess": true,
    "noImplicitOverride": true,

    // Some stricter flags
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitAny": true,
    "noPropertyAccessFromIndexSignature": false
  },
  "include": ["src/**/*", "test/**/*"],
  "exclude": ["dist/**/*"]
}
```

## File: src/operations.ts
```typescript
import { randomUUID } from 'crypto';
import type {
  DatabaseState,
  KRecord,
  KonroSchema,
  RelationDefinition,
  WithClause,
  QueryDescriptor,
  AggregationDescriptor,
} from './types';
import { KonroError, KonroValidationError } from './utils/error.util';

// --- HELPERS ---


/** Creates a pristine, empty database state from a schema. */
export const createEmptyState = <S extends KonroSchema<any, any>>(schema: S): DatabaseState<S> => {
  const state = {} as DatabaseState<S>;
  for (const tableName in schema.tables) {
    // This is a controlled cast, safe because we are iterating over the schema's tables.
    (state as any)[tableName] = { records: [], meta: { lastId: 0 } };
  }
  return state;
};

// --- QUERY ---

const _processWith = <S extends KonroSchema<any, any>>(
  recordsToProcess: KRecord[],
  currentTableName: string,
  withClause: WithClause,
  schema: S,
  state: DatabaseState
): KRecord[] => {
  // structuredClone is important to avoid mutating the records from the previous recursion level or the main state.
  const resultsWithRelations = structuredClone(recordsToProcess);

  for (const record of resultsWithRelations) {
    for (const relationName in withClause) {
      const relationDef = schema.relations[currentTableName]?.[relationName];
      if (!relationDef) continue;

      const withOpts = withClause[relationName];
      // Skip if the value is `false` or something not truthy (though types should prevent this)
      if (!withOpts) continue;

      const relatedRecords = findRelatedRecords(state, record, relationDef);

      const nestedWhere = typeof withOpts === 'object' ? withOpts.where : undefined;
      const nestedSelect = typeof withOpts === 'object' ? withOpts.select : undefined;
      const nestedWith = typeof withOpts === 'object' ? withOpts.with : undefined;

      let processedRelatedRecords = nestedWhere ? relatedRecords.filter(nestedWhere) : [...relatedRecords];

      // Recursively process deeper relations first
      if (nestedWith && processedRelatedRecords.length > 0) {
        processedRelatedRecords = _processWith(
          processedRelatedRecords,
          relationDef.targetTable,
          nestedWith,
          schema,
          state
        );
      }

      // Then, apply select on the (potentially already processed) related records
      if (nestedSelect) {
        const targetTableSchema = schema.tables[relationDef.targetTable];
        if (!targetTableSchema) throw KonroError({ code: 'E201', tableName: relationDef.targetTable });

        processedRelatedRecords = processedRelatedRecords.map(rec => {
          const newRec: KRecord = {};
          for (const outputKey in nestedSelect) {
            const def = nestedSelect[outputKey];
            if (!def) continue;
            // nested with() does not support selecting relations, only columns, as per spec.
            if (def._type === 'column') {
              const colName = Object.keys(targetTableSchema).find(key => targetTableSchema[key] === def);
              if (colName && rec.hasOwnProperty(colName)) {
                newRec[outputKey] = rec[colName];
              }
            }
          }
          return newRec;
        });
      }

      // Finally, attach the results to the parent record
      if (relationDef.relationType === 'one') {
        record[relationName] = processedRelatedRecords[0] ?? null;
      } else {
        record[relationName] = processedRelatedRecords;
      }
    }
  }

  return resultsWithRelations;
};

export const _queryImpl = <S extends KonroSchema<any, any>>(state: DatabaseState, schema: S, descriptor: QueryDescriptor): KRecord[] => {
  const tableState = state[descriptor.tableName];
  if (!tableState) return [];

  const tableSchema = schema.tables[descriptor.tableName];
  if (!tableSchema) throw KonroError({ code: 'E201', tableName: descriptor.tableName });
  const deletedAtColumn = Object.keys(tableSchema).find(key => tableSchema[key]?.options?._konro_sub_type === 'deletedAt');

  // 1. Filter
  let results: KRecord[];

  // Auto-filter soft-deleted records unless opted-out
  if (deletedAtColumn && !descriptor.withDeleted) {
    results = tableState.records.filter(r => r[deletedAtColumn] === null || r[deletedAtColumn] === undefined);
  } else {
    results = [...tableState.records];
  }
  
  results = descriptor.where ? results.filter(descriptor.where) : results;

  // 2. Eager load relations (`with`) - must happen after filtering
  if (descriptor.with) {
    results = 
_processWith(results, descriptor.tableName, descriptor.with, schema, state);
  }

  // 3. Paginate
  const offset = descriptor.offset ?? 0;
  const limit = descriptor.limit ?? results.length;
  let paginatedResults = results.slice(offset, offset + limit);

  // 4. Select Fields
  if (descriptor.select) {
    const relationsSchema = schema.relations[descriptor.tableName] ?? {};

    paginatedResults = paginatedResults.map(rec => {
      const newRec: KRecord = {};
      for (const outputKey in descriptor.select!) {
        const def = descriptor.select![outputKey];
        if (!def) continue;
        if (def._type === 'column') {
          const colName = Object.keys(tableSchema).find(key => tableSchema[key] === def);
          if (colName && rec.hasOwnProperty(colName)) {
            newRec[outputKey] = rec[colName];
          }
        } else if (def._type === 'relation') {
          const relName = Object.keys(relationsSchema).find(key => relationsSchema[key] === def);
          if (relName && rec.hasOwnProperty(relName)) {
            newRec[outputKey] = rec[relName];
          }
        }
      }
      return newRec;
    });
  }

  return paginatedResults;
};

const findRelatedRecords = (state: DatabaseState, record: KRecord, relationDef: RelationDefinition) => {
  const foreignKey = record[relationDef.on];
  const targetTable = state[relationDef.targetTable];

  if (foreignKey === undefined || !targetTable) return [];

  // one-to-many: 'on' is PK on current table, 'references' is FK on target
  if (relationDef.relationType === 'many') {
    return targetTable.records.filter(r => r[relationDef.references] === foreignKey);
  }

  // many-to-one: 'on' is FK on current table, 'references' is PK on target
  if (relationDef.relationType === 'one') {
    return targetTable.records.filter(r => r[relationDef.references] === foreignKey);
  }

  return [];
};

// --- AGGREGATION ---

export const _aggregateImpl = <S extends KonroSchema<any, any>>(
  state: DatabaseState,
  _schema: S, // Not used but keep for API consistency
  descriptor: AggregationDescriptor
): Record<string, number | null> => {
  const tableState = state[descriptor.tableName];
  if (!tableState) return {};

  const filteredRecords = descriptor.where ? tableState.records.filter(descriptor.where) : [...tableState.records];
  const results: Record<string, number | null> = {};

  for (const resultKey in descriptor.aggregations) {
    const aggDef = descriptor.aggregations[resultKey];
    if (!aggDef) continue;

    if (aggDef.aggType === 'count') {
      results[resultKey] = filteredRecords.length;
      continue;
    }

    if (!aggDef.column) {
      throw KonroError({ code: 'E203', aggType: aggDef.aggType });
    }
    const column = aggDef.column;

    const values = filteredRecords.map(r => r[column]).filter(v => typeof v === 'number') as number[];

    if (values.length === 0) {
      if (aggDef.aggType === 'sum') {
        results[resultKey] = 0; // sum of empty set is 0
      } else {
        results[resultKey] = null; // avg, min, max of empty set is null
      }
      continue;
    }

    switch (aggDef.aggType) {
      case 'sum':
        results[resultKey] = values.reduce((sum, val) => sum + val, 0);
        break;
      case 'avg':
        results[resultKey] = values.reduce((sum, val) => sum + val, 0) / values.length;
        break;
      case 'min':
        results[resultKey] = Math.min(...values);
        break;
      case 'max':
        results[resultKey] = Math.max(...values);
        break;
    }
  }
  return results;
};

// --- INSERT ---

export const _insertImpl = <S extends KonroSchema<any, any>>(state: DatabaseState, schema: S, tableName: string, values: KRecord[]): [DatabaseState, KRecord[]] => {
  const oldTableState = state[tableName];
  if (!oldTableState) throw KonroError({ code: 'E200', tableName });

  // To maintain immutability, we deep-clone only the table being modified.
  const tableState = structuredClone(oldTableState);
  const tableSchema = schema.tables[tableName];
  if (!tableSchema) throw KonroError({ code: 'E201', tableName });
  const insertedRecords: KRecord[] = [];

  for (const value of values) {
    const newRecord: KRecord = { ...value };
    // Handle IDs and defaults
    for (const colName in tableSchema) {
      const colDef = tableSchema[colName];
      if (colDef.dataType === 'id') {
        if (newRecord[colName] === undefined) {
          // Generate new PK if not provided
          if (colDef.options?._pk_strategy === 'uuid') {
            newRecord[colName] = randomUUID();
          } else { // 'auto-increment' or legacy undefined strategy
            tableState.meta.lastId++;
            newRecord[colName] = tableState.meta.lastId;
          }
        } else {
          // If user provided an ID for an auto-increment table, update lastId to avoid future collisions.
          if (colDef.options?._pk_strategy !== 'uuid' && typeof newRecord[colName] === 'number') {
            tableState.meta.lastId = Math.max(tableState.meta.lastId, newRecord[colName] as number);
          }
        }
      }
      if (newRecord[colName] === undefined && colDef.options?.default !== undefined) {
        newRecord[colName] = typeof colDef.options.default === 'function' ? colDef.options.default() : colDef.options.default;
      }
    }

    // Validate the record before inserting
    validateRecord(newRecord, tableSchema, tableState.records);

    tableState.records.push(newRecord);
    insertedRecords.push(newRecord);
  }

  const newState = { ...state, [tableName]: tableState };
  return [newState, insertedRecords];
};

// --- UPDATE ---

export const _updateImpl = <S extends KonroSchema<any, any>>(state: DatabaseState, schema: S, tableName: string, data: Partial<KRecord>, predicate: (record: KRecord) => boolean): [DatabaseState, KRecord[]] => {
  const oldTableState = state[tableName];
  if (!oldTableState) throw KonroError({ code: 'E200', tableName });

  const tableSchema = schema.tables[tableName];
  if (!tableSchema) {
    throw KonroError({ code: 'E201', tableName });
  }

  const updatedRecords: KRecord[] = [];

  // Auto-update 'updatedAt' timestamp
  for (const colName of Object.keys(tableSchema)) {
      if (tableSchema[colName]?.options?._konro_sub_type === 'updatedAt') {
          (data as KRecord)[colName] = new Date();
      }
  }

  const updateData = { ...data };
  // Find the ID column from the schema and prevent it from being updated.
  const idColumn = Object.entries(tableSchema).find(([, colDef]) => {
    return colDef && typeof colDef === 'object' && '_type' in colDef && colDef._type === 'column' && 'dataType' in colDef && colDef.dataType === 'id';
  })?.[0];
  if (idColumn && updateData[idColumn] !== undefined) {
    delete updateData[idColumn];
  }

  const newRecords = oldTableState.records.map(record => {
    if (predicate(record)) {
      const updatedRecord = { ...record, ...updateData };

      // Validate the updated record, excluding current record from unique checks
      const otherRecords = oldTableState.records.filter(r => r !== record);
      validateRecord(updatedRecord, tableSchema, otherRecords);

      updatedRecords.push(updatedRecord);
      return updatedRecord;
    }
    return record;
  });

  if (updatedRecords.length === 0) {
    return [state, []];
  }

  const tableState = { ...oldTableState, records: newRecords };
  const newState = { ...state, [tableName]: tableState };

  return [newState, updatedRecords];
};


// --- DELETE ---

function applyCascades<S extends KonroSchema<any, any>>(
  state: DatabaseState<S>,
  schema: S,
  tableName: string, // table with deleted records, e.g. 'posts'
  deletedRecords: KRecord[]
): DatabaseState<S> {
  let nextState = state;
  if (!schema.relations) return nextState;

  const pk = Object.keys(schema.tables[tableName]).find(k => schema.tables[tableName][k]?.dataType === 'id') ?? 'id';
  const deletedKeys = new Set(deletedRecords.map(r => r[pk]));
  if (deletedKeys.size === 0) return nextState;

  // Check relations defined on the deleted table (e.g., users -> posts)
  const relationsOnDeletedTable = schema.relations[tableName] ?? {};
  for (const relationName in relationsOnDeletedTable) {
    const relation = relationsOnDeletedTable[relationName];
    const relatedTableName = relation.targetTable;
    
    if (relation.onDelete) {
      const foreignKey = relation.references; // The FK on the related table
      const predicate = (record: KRecord) => deletedKeys.has(record[foreignKey] as any);
      

      if (relation.onDelete === 'CASCADE') {
        const [cascadedState, _] = _deleteImpl(nextState, schema, relatedTableName, predicate);
        nextState = cascadedState as DatabaseState<S>;
      } else if (relation.onDelete === 'SET NULL') {
        const updateData = { [foreignKey]: null };
        const [cascadedState, _] = _updateImpl(nextState, schema, relatedTableName, updateData, predicate);
        nextState = cascadedState as DatabaseState<S>;
      }
    }
  }

  // Also iterate over all tables to find ones that have a FK to `tableName`
  for (const relatedTableName in schema.relations) {
    if (relatedTableName === tableName) continue;
    
    const relationsOnRelatedTable = schema.relations[relatedTableName];
    for (const relationName in relationsOnRelatedTable) {
      const inboundRelation = relationsOnRelatedTable[relationName];

      // Found a relation pointing to our deleted table
      if (inboundRelation.targetTable === tableName) {
        
        // Check for the onDelete rule. Prioritize the rule on the table with the FK.
        // If not found, check the inverse relation (for one-to-many cases).
        let onDelete = inboundRelation.onDelete;
        if (!onDelete) {
            const relationsOnOriginalTable = schema.relations[tableName] ?? {};
            for (const outboundRelationName in relationsOnOriginalTable) {
                const outboundRelation = relationsOnOriginalTable[outboundRelationName];
                if (outboundRelation.targetTable === relatedTableName &&
                    outboundRelation.on === inboundRelation.references &&
                    outboundRelation.references === inboundRelation.on) {
                    onDelete = outboundRelation.onDelete;
                    break;
                }
            }
        }
        
        // Also check if the relation is defined on the original table pointing to the related table
        if (!onDelete) {
            const relationsOnOriginalTable = schema.relations[tableName] ?? {};
            for (const outboundRelationName in relationsOnOriginalTable) {
                const outboundRelation = relationsOnOriginalTable[outboundRelationName];
                if (outboundRelation.targetTable === relatedTableName) {
                    onDelete = outboundRelation.onDelete;
                    break;
                }
            }
        }

        if (!onDelete) continue;

        const foreignKey = inboundRelation.on; // The FK on the related table
        const predicate = (record: KRecord) => deletedKeys.has(record[foreignKey] as any);
        

        if (onDelete === 'CASCADE') {
          const [cascadedState, _] = _deleteImpl(nextState, schema, relatedTableName, predicate);
          nextState = cascadedState as DatabaseState<S>;
        } else if (onDelete === 'SET NULL') {
          const updateData = { [foreignKey]: null };
          const [cascadedState, _] = _updateImpl(nextState, schema, relatedTableName, updateData, predicate);
          nextState = cascadedState as DatabaseState<S>;
        }
      }
    }
  }
  return nextState;
}

export const _deleteImpl = (state: DatabaseState, schema: KonroSchema<any, any>, tableName: string, predicate: (record: KRecord) => boolean): [DatabaseState, KRecord[]] => {
  const tableState = state[tableName];
  if (!tableState) throw KonroError({ code: 'E200', tableName });
  const tableSchema = schema.tables[tableName];
  if (!tableSchema) throw KonroError({ code: 'E201', tableName });

  const deletedAtColumn = Object.keys(tableSchema).find(key => tableSchema[key]?.options?._konro_sub_type === 'deletedAt');

  // Soft delete path
  if (deletedAtColumn) {
    // Use update implementation for soft-delete. It will also handle `updatedAt`.
    const [baseState, recordsToUpdate] = _updateImpl(
      state,
      schema,
      tableName,
      { [deletedAtColumn]: new Date() },
      (record) => !record[deletedAtColumn] && predicate(record)
    );

    if (recordsToUpdate.length === 0) return [state, []];
    const finalState = applyCascades(baseState, schema, tableName, recordsToUpdate);
    // The returned records are the ones that were just soft-deleted from this table.
    return [finalState, recordsToUpdate];
  } 
  
  // Hard delete path
  const deletedRecords = tableState.records.filter(predicate);
  const remainingRecords = tableState.records.filter(r => !predicate(r));

  if (deletedRecords.length === 0) {
    return [state, []];
  }

  const newState = {
    ...state,
    [tableName]: {
      ...tableState,
      records: remainingRecords,
    },
  };

  const finalState = applyCascades(newState, schema, tableName, deletedRecords);

  return [finalState, deletedRecords];
};

// --- VALIDATION ---

const validateRecord = (record: KRecord, tableSchema: Record<string, any>, existingRecords: KRecord[]): void => {
  for (const [columnName, colDef] of Object.entries(tableSchema)) {
    if (!colDef || typeof colDef !== 'object' || !('dataType' in colDef)) continue;

    const value = record[columnName];
    const options = colDef.options || {};

    // Skip validation for undefined values (they should have defaults applied already)
    if (value === undefined) continue;

    // Validate unique constraint, allowing multiple nulls
    if (options.unique && value !== null && existingRecords.some(r => r[columnName] === value)) {
      throw KonroValidationError({ code: 'E300', value: String(value), columnName });
    }

    // Validate string constraints
    if (colDef.dataType === 'string' && typeof value === 'string') {
      // Min length
      if (options.min !== undefined && value.length < options.min) {
        throw KonroValidationError({ code: 'E301', value, columnName, min: options.min });
      }

      // Max length
      if (options.max !== undefined && value.length > options.max) {
        throw KonroValidationError({ code: 'E302', value, columnName, max: options.max });
      }

      // Format validation
      if (options.format === 'email' && !isValidEmail(value)) {
        throw KonroValidationError({ code: 'E303', value, columnName });
      }
    }

    // Validate number constraints
    if (colDef.dataType === 'number' && typeof value === 'number') {
      // Min value
      if (options.min !== undefined && value < options.min) {
        throw KonroValidationError({ code: 'E304', value, columnName, min: options.min });
      }

      // Max value
      if (options.max !== undefined && value > options.max) {
        throw KonroValidationError({ code: 'E305', value, columnName, max: options.max });
      }
    }
  }
};

const isValidEmail = (email: string): boolean => {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
};
```

## File: test/integration/Adapters/PerRecord.test.ts
```typescript
import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir, uuidTestSchema } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';
import { KonroError, KonroStorageError } from '../../../src/utils/error.util';
import type { InMemoryDbContext, OnDemandDbContext } from '../../../src/db';

describe('Integration > Adapters > PerRecord', () => {
  const dbDirPath = path.join(TEST_DIR, 'per_record_db');

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  describe('Initialization', () => {
    it('should successfully create a per-record adapter', () => {
      const adapter = konro.createFileAdapter({
        format: 'json',
        perRecord: { dir: dbDirPath },
      });
      expect(adapter).toBeDefined();
      expect(adapter.options.perRecord).toEqual({ dir: dbDirPath });
    });

    it('should throw an error for unsupported formats like "csv"', () => {
      expect(() => {
        konro.createFileAdapter({
          format: 'csv',
          perRecord: { dir: dbDirPath },
        });
      }).toThrow(KonroError({ code: 'E105' }));
    });
  });

  describe('In-Memory Mode (JSON)', () => {
    let db: InMemoryDbContext<typeof testSchema>;
    const adapter = konro.createFileAdapter({
      format: 'json',
      perRecord: { dir: dbDirPath },
    });

    beforeEach(() => {
      db = konro.createDatabase({ schema: testSchema, adapter });
    });

    it('should write each record to a separate file and a meta file', async () => {
      let state = db.createEmptyState();
      [state] = db.insert(state, 'users', { name: 'Record User', email: 'rec@test.com', age: 33 });
      [state] = db.insert(state, 'posts', { title: 'Record Post', content: '...', authorId: 1 });

      await db.write(state);

      const usersDir = path.join(dbDirPath, 'users');
      const postsDir = path.join(dbDirPath, 'posts');
      
      const userRecordPath = path.join(usersDir, '1.json');
      const userMetaPath = path.join(usersDir, '_meta.json');
      const postRecordPath = path.join(postsDir, '1.json');
      const postMetaPath = path.join(postsDir, '_meta.json');

      const userRecordContent = JSON.parse(await fs.readFile(userRecordPath, 'utf-8'));
      const userMetaContent = JSON.parse(await fs.readFile(userMetaPath, 'utf-8'));
      const postRecordContent = JSON.parse(await fs.readFile(postRecordPath, 'utf-8'));
      const postMetaContent = JSON.parse(await fs.readFile(postMetaPath, 'utf-8'));

      expect(userRecordContent.name).toBe('Record User');
      expect(userMetaContent.lastId).toBe(1);
      expect(postRecordContent.title).toBe('Record Post');
      expect(postMetaContent.lastId).toBe(1);
    });

    it('should delete record files that are no longer in the state', async () => {
      let state = db.createEmptyState();
      [state] = db.insert(state, 'users', { name: 'To Be Deleted', email: 'del@test.com', age: 40 });
      await db.write(state);
      
      const userRecordPath = path.join(dbDirPath, 'users', '1.json');
      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(true);

      [state] = db.delete(state, 'users').where({ id: 1 });
      await db.write(state);

      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(false);
    });

    it('should read records from individual files to build the state', async () => {
      // Manually create files
      const usersDir = path.join(dbDirPath, 'users');
      await fs.mkdir(usersDir, { recursive: true });
      await fs.writeFile(path.join(usersDir, '1.json'), JSON.stringify({ id: 1, name: 'Manual User', email: 'man@test.com', age: 50, isActive: true }));
      await fs.writeFile(path.join(usersDir, '_meta.json'), JSON.stringify({ lastId: 1 }));
      
      const state = await db.read();
      
      expect(state.users.records.length).toBe(1);
      expect(state.users.records[0]?.name).toBe('Manual User');
      expect(state.users.meta.lastId).toBe(1);
      expect(state.posts.records.length).toBe(0);
    });
    
    it('should derive lastId from record files if meta file is missing', async () => {
        const usersDir = path.join(dbDirPath, 'users');
        await fs.mkdir(usersDir, { recursive: true });
        await fs.writeFile(path.join(usersDir, '2.json'), JSON.stringify({ id: 2, name: 'User 2', email: 'u2@test.com', age: 50, isActive: true }));
        await fs.writeFile(path.join(usersDir, '5.json'), JSON.stringify({ id: 5, name: 'User 5', email: 'u5@test.com', age: 50, isActive: true }));

        const state = await db.read();
        expect(state.users.meta.lastId).toBe(5);
    });

    it('should throw KonroStorageError for a corrupt record file', async () => {
      const usersDir = path.join(dbDirPath, 'users');
      await fs.mkdir(usersDir, { recursive: true });
      await fs.writeFile(path.join(usersDir, '1.json'), '{ "id": 1, "name": "Corrupt"'); // Invalid JSON
      
      await expect(db.read()).rejects.toThrow(KonroStorageError);
    });
  });

  describe('In-Memory Mode (YAML)', () => {
    let db: InMemoryDbContext<typeof testSchema>;
    const adapter = konro.createFileAdapter({
      format: 'yaml',
      perRecord: { dir: dbDirPath },
    });

    beforeEach(() => {
      db = konro.createDatabase({ schema: testSchema, adapter });
    });

    it('should write each record to a separate YAML file and a meta file', async () => {
      let state = db.createEmptyState();
      [state] = db.insert(state, 'users', { name: 'YAML Record User', email: 'yrec@test.com', age: 33 });
      [state] = db.insert(state, 'posts', { title: 'YAML Record Post', content: '...', authorId: 1 });

      await db.write(state);

      const usersDir = path.join(dbDirPath, 'users');
      const postsDir = path.join(dbDirPath, 'posts');

      const userRecordPath = path.join(usersDir, '1.yaml');
      const userMetaPath = path.join(usersDir, '_meta.json');
      const postRecordPath = path.join(postsDir, '1.yaml');
      const postMetaPath = path.join(postsDir, '_meta.json');

      const userRecordContent = yaml.load(await fs.readFile(userRecordPath, 'utf-8')) as any;
      const userMetaContent = JSON.parse(await fs.readFile(userMetaPath, 'utf-8'));
      const postRecordContent = yaml.load(await fs.readFile(postRecordPath, 'utf-8')) as any;
      const postMetaContent = JSON.parse(await fs.readFile(postMetaPath, 'utf-8'));

      expect(userRecordContent.name).toBe('YAML Record User');
      expect(userMetaContent.lastId).toBe(1);
      expect(postRecordContent.title).toBe('YAML Record Post');
      expect(postMetaContent.lastId).toBe(1);
    });

    it('should read records from individual YAML files to build the state', async () => {
      // Manually create files
      const usersDir = path.join(dbDirPath, 'users');
      await fs.mkdir(usersDir, { recursive: true });
      await fs.writeFile(path.join(usersDir, '1.yaml'), yaml.dump({ id: 1, name: 'Manual YAML User', email: 'yman@test.com', age: 50, isActive: true }));
      await fs.writeFile(path.join(usersDir, '_meta.json'), JSON.stringify({ lastId: 1 }));

      const state = await db.read();

      expect(state.users.records.length).toBe(1);
      expect(state.users.records[0]?.name).toBe('Manual YAML User');
      expect(state.users.meta.lastId).toBe(1);
      expect(state.posts.records.length).toBe(0);
    });

    it('should throw KonroStorageError for a corrupt record YAML file', async () => {
      const usersDir = path.join(dbDirPath, 'users');
      await fs.mkdir(usersDir, { recursive: true });
      await fs.writeFile(path.join(usersDir, '1.yaml'), 'name: Corrupt\n  bad-indent: true'); // Invalid YAML

      await expect(db.read()).rejects.toThrow(KonroStorageError);
    });
  });

  describe('On-Demand Mode (YAML)', () => {
    let db: OnDemandDbContext<typeof testSchema>;
    
    beforeEach(() => {
        const adapter = konro.createFileAdapter({
            format: 'yaml',
            mode: 'on-demand',
            perRecord: { dir: dbDirPath },
        });
        db = konro.createDatabase({ schema: testSchema, adapter });
    });

    it('should insert a record and create its file and update meta', async () => {
      const user = await db.insert('users', { name: 'OnDemand Record', email: 'odr@test.com', age: 25 });
      
      const userRecordPath = path.join(dbDirPath, 'users', `${user.id}.yaml`);
      const userMetaPath = path.join(dbDirPath, 'users', '_meta.json');

      const recordContent = yaml.load(await fs.readFile(userRecordPath, 'utf-8')) as any;
      const metaContent = JSON.parse(await fs.readFile(userMetaPath, 'utf-8'));

      expect(recordContent.name).toBe('OnDemand Record');
      expect(metaContent.lastId).toBe(1);
    });

    it('should update a record file', async () => {
      const user = await db.insert('users', { name: 'Update Me', email: 'upd@test.com', age: 35 });
      await db.update('users').set({ name: 'Updated Name' }).where({ id: user.id });

      const userRecordPath = path.join(dbDirPath, 'users', `${user.id}.yaml`);
      const recordContent = yaml.load(await fs.readFile(userRecordPath, 'utf-8')) as any;
      
      expect(recordContent.name).toBe('Updated Name');
    });

    it('should delete a record file', async () => {
      const user = await db.insert('users', { name: 'Delete Me', email: 'del@test.com', age: 45 });
      const userRecordPath = path.join(dbDirPath, 'users', `${user.id}.yaml`);
      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(true);

      await db.delete('users').where({ id: user.id });
      expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(false);
    });

    it('should query with relations by reading multiple tables', async () => {
        const user = await db.insert('users', { name: 'Author', email: 'author@test.com', age: 35 });
        await db.insert('posts', { title: 'Post by Author', content: '...', authorId: user.id });
        
        const userWithPosts = await db.query().from('users').where({ id: user.id }).with({ posts: true }).first();
        
        expect(userWithPosts).toBeDefined();
        expect(userWithPosts?.posts?.length).toBe(1);
        expect(userWithPosts?.posts?.[0]?.title).toBe('Post by Author');
    });
  });

  describe('ID Handling', () => {
    it('should generate UUIDs for filenames and record IDs', async () => {
        const adapter = konro.createFileAdapter({
            format: 'json',
            mode: 'on-demand',
            perRecord: { dir: dbDirPath },
        });
        const db = konro.createDatabase({ schema: uuidTestSchema, adapter });

        const user = await db.insert('uuid_users', { name: 'UUID User' });
        
        expect(typeof user.id).toBe('string');
        const userRecordPath = path.join(dbDirPath, 'uuid_users', `${user.id}.json`);
        expect(await fs.access(userRecordPath).then(() => true).catch(() => false)).toBe(true);
        
        const recordContent = JSON.parse(await fs.readFile(userRecordPath, 'utf-8'));
        expect(recordContent.id).toBe(user.id);
        expect(recordContent.name).toBe('UUID User');
    });

    it('on-demand insert should not derive lastId from existing files', async () => {
        // Manually create a file with ID 5, but no meta file
        const usersDir = path.join(dbDirPath, 'users');
        await fs.mkdir(usersDir, { recursive: true });
        await fs.writeFile(path.join(usersDir, '5.json'), JSON.stringify({ id: 5, name: 'Existing User', email: 'ex@test.com', age: 55, isActive: true }));
        
        const adapter = konro.createFileAdapter({ format: 'json', mode: 'on-demand', perRecord: { dir: dbDirPath } });
        const db = konro.createDatabase({ schema: testSchema, adapter });
        
        // Inserting should start from ID 1 because _meta.json doesn't exist
        const newUser = await db.insert('users', { name: 'New User', email: 'new@test.com', age: 22 });
        expect(newUser.id).toBe(1);
        
        const metaContent = JSON.parse(await fs.readFile(path.join(usersDir, '_meta.json'), 'utf-8'));
        expect(metaContent.lastId).toBe(1);
    });
  });
});
```

## File: package.json
```json
{
  "name": "konro",
  "version": "0.1.17",
  "description": "A type-safe, functional-inspired ORM for local JSON/YAML file-based data sources.",
  "type": "module",
  "main": "./dist/index.cjs",
  "module": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js",
      "require": "./dist/index.cjs"
    }
  },
  "files": [
    "dist"
  ],
  "homepage": "https://github.com/nocapro/konro",
  "repository": {
    "type": "git",
    "url": "git+https://github.com/nocapro/konro.git"
  },
  "keywords": [
    "orm",
    "json",
    "yaml",
    "csv",
    "xlsx",
    "database",
    "typescript",
    "local-first",
    "immutable",
    "functional"
  ],
  "author": "nocapro",
  "license": "MIT",
  "devDependencies": {
    "@types/bun": "latest",
    "@types/js-yaml": "^4.0.9",
    "@types/papaparse": "^5.3.14",
    "@typescript-eslint/eslint-plugin": "^8.36.0",
    "@typescript-eslint/parser": "^8.36.0",
    "eslint": "^9.30.1",
    "js-yaml": "^4.1.0",
    "papaparse": "^5.4.1",
    "typescript": "^5.5.4",
    "xlsx": "^0.18.5",
    "tsup": "^8.5.0"
  },
  "peerDependencies": {
    "js-yaml": "^4.1.0",
    "papaparse": "^5.4.1",
    "typescript": "^5.0.0",
    "xlsx": "^0.18.5"
  },
  "peerDependenciesMeta": {
    "js-yaml": {
      "optional": true
    },
    "papaparse": {
      "optional": true
    },
    "xlsx": {
      "optional": true
    }
  },
  "scripts": {
    "lint": "eslint .",
    "build": "tsup",
    "dev": "tsup --watch",
    "test": "bun test",
    "test:restore-importer": "git checkout -- test/konro-test-import.ts",
    "test:src": "npm run test:restore-importer && bun test",
    "test:dist": "npm run build && echo \"export * from '../dist/index.js';\" > test/konro-test-import.ts && bun test && npm run test:restore-importer",
    "prepublishOnly": "npm run build"
  }
}
```
