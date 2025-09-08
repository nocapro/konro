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