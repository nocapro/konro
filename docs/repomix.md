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
  integration/
    Adapters/
      MultiFileYaml.test.ts
      OnDemand.test.ts
      PerRecord.test.ts
      Read.test.ts
      SingleFileJson.test.ts
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

        const currentFiles = new Set(tableState.records.map((r: KRecord) => `${r[idColumn]}${fileExtension}`));
        const existingFiles = (await fs.readdir(tableDir)).filter(f => !f.startsWith('_meta') && !f.endsWith(TEMP_FILE_SUFFIX));

        const recordWrites = tableState.records.map((r) => writeAtomic(path.join(tableDir, `${r[idColumn]}${fileExtension}`), serializer.stringify(r), fs));
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
import { TEMP_FILE_SUFFIX } from './utils/constants';

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

    const writeTableState = async (tableName: string, tableState: TableState, idColumn: string): Promise<void> => {
      const tableDir = getTableDir(tableName);
      await fs.mkdir(tableDir, { recursive: true });
      await writeAtomic(getMetaPath(tableName), JSON.stringify(tableState.meta, null, 2), fs);

      const currentFiles = new Set(tableState.records.map((r) => `${(r as KRecord)[idColumn]}${fileExtension}`));
      const existingFiles = (await fs.readdir(tableDir)).filter(f => !f.startsWith('_meta') && !f.endsWith(TEMP_FILE_SUFFIX));

      const recordWrites = tableState.records.map((r) =>
        writeAtomic(getRecordPath(tableName, (r as KRecord)[idColumn]), serializer.stringify(r), fs)
      );
      const recordDeletes = existingFiles.filter(f => !currentFiles.has(f)).map(f =>
        fs.unlink(path.join(tableDir, f as string))
      );
      await Promise.all([...recordWrites, ...recordDeletes]);
    };

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
        await writeTableState(tableName, newState[tableName]!, idColumn);
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

  const pk = Object.keys(schema.tables[tableName]).find(k => schema.tables[tableName][k]?.options?._pk_strategy) ?? 'id';
  const deletedKeys = new Set(deletedRecords.map(r => r[pk]));
  if (deletedKeys.size === 0) return nextState;

  // Iterate over all tables to find ones that have a FK to `tableName`
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
export function object<T extends object>(
options?: { optional?: boolean; default?: unknown }
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
    "noPropertyAccessFromIndexSignature": false
  },
  "include": ["src/**/*", "test/**/*"],
  "exclude": ["dist/**/*"]
}
```

## File: package.json
```json
{
  "name": "konro",
  "version": "0.1.16",
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
