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
