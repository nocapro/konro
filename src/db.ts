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