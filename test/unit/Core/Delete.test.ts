import { describe, it, expect, beforeEach } from 'bun:test';
import { _deleteImpl } from '../../../src/operations';
import { DatabaseState, KRecord } from '../../../src/types';
import { konro } from '../../konro-test-import';

describe('Unit > Core > Delete', () => {
    let testState: DatabaseState;

    const hardDeleteSchema = konro.createSchema({
        tables: {
            users: {
                id: konro.id(),
                name: konro.string(),
                email: konro.string(),
                age: konro.number(),
            },
            posts: {
                id: konro.id(),
                title: konro.string(),
                userId: konro.number()
            },
            profiles: { id: konro.id(), bio: konro.string(), userId: konro.number() },
            tags: { id: konro.id(), name: konro.string() },
            posts_tags: { id: konro.id(), postId: konro.number(), tagId: konro.number() },
        },
        relations: () => ({
            users: {
                posts: konro.many('posts', { on: 'id', references: 'userId', onDelete: 'CASCADE' })
            }
        })
    });
    
    const softDeleteSchema = konro.createSchema({
        tables: {
            users: {
                id: konro.id(),
                name: konro.string(),
                email: konro.string(),
                age: konro.number(),
                deletedAt: konro.deletedAt()
            },
            posts: {
                id: konro.id(),
                title: konro.string(),
                userId: konro.number()
            },
            profiles: { id: konro.id(), bio: konro.string(), userId: konro.number() },
            tags: { id: konro.id(), name: konro.string() },
            posts_tags: { id: konro.id(), postId: konro.number(), tagId: konro.number() },
        },
        relations: () => ({
            users: {
                posts: konro.many('posts', { on: 'id', references: 'userId', onDelete: 'CASCADE' })
            }
        })
    });

    beforeEach(() => {
        testState = {
            users: {
                records: [
                    { id: 1, name: 'Alice', email: 'a@a.com', age: 30, deletedAt: null },
                    { id: 2, name: 'Bob', email: 'b@b.com', age: 25, deletedAt: null },
                    { id: 3, name: 'Charlie', email: 'c@c.com', age: 42, deletedAt: null },
                ],
                meta: { lastId: 3 },
            },
            posts: { 
                records: [
                    { id: 101, title: 'Post A', userId: 1 },
                    { id: 102, title: 'Post B', userId: 2 },
                    { id: 103, title: 'Post C', userId: 1 },
                ], 
                meta: { lastId: 103 } 
            },
            profiles: { records: [], meta: { lastId: 0 } },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    it('should return a new state object, not mutate the original state, on hard delete', () => {
        const originalState = structuredClone(testState);
        const [newState] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => r.id === 1);
        
        expect(newState).not.toBe(originalState);
        expect(originalState.users!.records.length).toBe(3);
        expect(newState.users!.records.length).toBe(2);
    });

    it('should only hard delete records that match the predicate function', () => {
        const [newState, deleted] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => typeof r.age === 'number' && r.age > 35);
        
        expect(deleted.length).toBe(1);
        expect(deleted[0]!.id).toBe(3);
        expect(newState.users!.records.length).toBe(2);
        expect(newState.users!.records.find(u => u.id === 3)).toBeUndefined();
    });

    it('should return both the new state and an array of the full, hard-deleted records in the result tuple', () => {
        const [newState, deleted] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => r.id === 2);

        expect(newState).toBeDefined();
        expect(deleted).toBeInstanceOf(Array);
        expect(deleted.length).toBe(1);
        expect(deleted[0]!).toEqual({ id: 2, name: 'Bob', email: 'b@b.com', age: 25, deletedAt: null });
    });

    it('should not modify the table meta lastId on delete', () => {
        const [newState] = _deleteImpl(testState, hardDeleteSchema, 'users', (r) => r.id === 3);
        expect(newState.users!.meta.lastId).toBe(3);
    });

    it('should soft delete a record by setting deletedAt if the column exists in schema', () => {
        const [newState, deleted] = _deleteImpl(testState, softDeleteSchema, 'users', (r) => r.id === 2);

        expect(newState.users!.records.length).toBe(3); // Record is not removed
        const deletedUser = newState.users!.records.find(u => u.id === 2);
        expect(deletedUser?.deletedAt).toBeInstanceOf(Date);
        
        expect(deleted.length).toBe(1);
        expect(deleted[0]!.id).toBe(2);
        expect(deleted[0]!.deletedAt).toEqual(deletedUser?.deletedAt);
    });

    it('should not soft delete an already soft-deleted record', () => {
        (testState.users!.records[1] as KRecord).deletedAt = new Date('2024-01-01');
        const [newState, deleted] = _deleteImpl(testState, softDeleteSchema, 'users', (r) => r.id === 2);

        expect(newState).toBe(testState); // Should return original state as nothing changed
        expect(deleted.length).toBe(0);
        expect((newState.users!.records[1] as KRecord).deletedAt).toEqual(new Date('2024-01-01'));
    });

    it('should perform a cascading delete on related records', () => {
        const [newState, deletedUsers] = _deleteImpl(testState, softDeleteSchema, 'users', (r) => r.id === 1);
        
        expect(deletedUsers.length).toBe(1);
        expect(newState.users!.records.find(u => u.id === 1)?.deletedAt).toBeInstanceOf(Date);
        
        // Check that posts by user 1 are also gone (hard delete, as posts have no deletedAt)
        const postsForUser1 = newState.posts!.records.filter(p => p.userId === 1);
        expect(postsForUser1.length).toBe(0);

        // Check that other posts are unaffected
        expect(newState.posts!.records.length).toBe(1);
        expect(newState.posts!.records[0]!.id).toBe(102);
    });
});