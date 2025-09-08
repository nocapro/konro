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