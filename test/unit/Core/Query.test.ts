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