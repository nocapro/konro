import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _queryImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';

describe('Unit > Core > Query-With', () => {
    let testState: DatabaseState;

    beforeEach(() => {
        testState = {
            users: {
                records: [
                    { id: 1, name: 'Alice' },
                    { id: 2, name: 'Bob' },
                ],
                meta: { lastId: 2 },
            },
            posts: {
                records: [
                    { id: 10, title: 'Alice Post 1', authorId: 1 },
                    { id: 11, title: 'Bob Post 1', authorId: 2 },
                    { id: 12, title: 'Alice Post 2', authorId: 1 },
                ],
                meta: { lastId: 12 },
            },
            profiles: {
                records: [
                    { id: 100, bio: 'Bio for Alice', userId: 1 },
                ],
                meta: { lastId: 100 },
            },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    it('should resolve a `one` relationship and attach it to the parent record', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'posts',
            where: r => r.id === 10,
            with: { author: true }
        });

        expect(results.length).toBe(1);
        const post = results[0]!;
        expect(post).toBeDefined();
        const author = post.author as {id: unknown, name: unknown};
        expect(author).toBeDefined();
        expect(author.id).toBe(1);
        expect(author.name).toBe('Alice');
    });

    it('should resolve a `many` relationship and attach it as an array', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: { posts: true }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        expect(user).toBeDefined();
        const posts = user.posts as {title: unknown}[];
        expect(posts).toBeInstanceOf(Array);
        expect(posts.length).toBe(2);
        expect(posts[0]!.title).toBe('Alice Post 1');
        expect(posts[1]!.title).toBe('Alice Post 2');
    });

    it('should filter nested records within a .with() clause', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: {
                posts: {
                    where: (post) => typeof post.title === 'string' && post.title.includes('Post 2')
                }
            }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        const posts = user.posts as {id: unknown}[];
        expect(posts).toBeDefined();
        expect(posts.length).toBe(1);
        expect(posts[0]!.id).toBe(12);
    });

    it('should select nested fields within a .with() clause', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: {
                posts: {
                    select: {
                        postTitle: testSchema.tables.posts.title
                    }
                }
            }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        const posts = user.posts as {postTitle: unknown}[];
        expect(posts).toBeDefined();
        expect(posts.length).toBe(2);
        expect(posts[0]!).toEqual({ postTitle: 'Alice Post 1' });
    });

    it('should handle multiple relations at once', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 1,
            with: {
                posts: true,
                profile: true
            }
        });
        
        expect(results.length).toBe(1);
        const user = results[0]!;
        const posts = user.posts as unknown[];
        const profile = user.profile as { bio: unknown };
        expect(posts).toBeInstanceOf(Array);
        expect(posts.length).toBe(2);
        expect(profile).toBeDefined();
        expect(profile.bio).toBe('Bio for Alice');
    });

    it('should return null for a `one` relation if no related record is found', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 2, // Bob has no profile
            with: { profile: true }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        expect(user.profile).toBeNull();
    });

    it('should return an empty array for a `many` relation if no related records are found', () => {
        // Add a user with no posts
        testState.users!.records.push({ id: 3, name: 'Charlie' });
        const results = _queryImpl(testState, testSchema, {
            tableName: 'users',
            where: r => r.id === 3,
            with: { posts: true }
        });

        expect(results.length).toBe(1);
        const user = results[0]!;
        expect(user.posts).toBeInstanceOf(Array);
        expect((user.posts as unknown[]).length).toBe(0);
    });

    it('should handle nested `with` clauses for deep relations', () => {
        const results = _queryImpl(testState, testSchema, {
            tableName: 'posts',
            where: r => r.id === 10, // Alice Post 1
            with: {
                author: { // author is a user
                    with: {
                        posts: { // author's other posts
                            where: p => p.id === 12 // Filter to Alice Post 2
                        }
                    }
                }
            }
        });

        expect(results.length).toBe(1);
        const post = results[0]!;
        expect(post.id).toBe(10);

        const author = post.author as { id: unknown, name: unknown, posts: { id: unknown }[] };
        expect(author).toBeDefined();
        expect(author.id).toBe(1);
        expect(author.name).toBe('Alice');

        const authorPosts = author.posts;
        expect(authorPosts).toBeInstanceOf(Array);
        expect(authorPosts.length).toBe(1);
        expect(authorPosts[0]!.id).toBe(12);
    });
});