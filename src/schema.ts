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