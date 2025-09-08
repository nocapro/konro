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