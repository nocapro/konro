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