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