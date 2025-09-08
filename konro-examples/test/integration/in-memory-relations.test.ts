import { konro } from 'konro';
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../test.util';
import type { DatabaseState } from 'konro';

describe('In-Memory Relations', () => {
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: 'dummy.json' },
    mode: 'in-memory',
  });

  const db = konro.createDatabase({ schema: testSchema, adapter });
  let state: DatabaseState<typeof testSchema>;

  beforeEach(() => {
    state = db.createEmptyState();
    // Seed data
    const [s1] = db.insert(state, 'users', [
      { name: 'Alice', email: 'alice@example.com', age: 30 },
      { name: 'Bob', email: 'bob@example.com', age: 40 },
    ]);
    const [s2] = db.insert(s1, 'posts', [
      { title: 'Alices Post', authorId: 1, views: 100 },
      { title: 'Bobs Post', authorId: 2, views: 150 },
      { title: 'Alices Second Post', authorId: 1, views: 200 },
    ]);
    const [s3] = db.insert(s2, 'comments', [
      { text: 'Great post!', authorId: 2, postId: 1 },
      { text: 'Thanks!', authorId: 1, postId: 1 },
    ]);
    state = s3;
  });

  it('should eager load a one-to-many relationship', () => {
    const userWithPosts = db.query(state).from('users').where({ id: 1 }).with({ posts: true }).first();
    
    expect(userWithPosts).not.toBeNull();
    expect(userWithPosts?.posts).toBeDefined();
    expect(userWithPosts?.posts).toHaveLength(2);
    expect(userWithPosts?.posts?.[0].title).toBe('Alices Post');
  });

  it('should eager load a many-to-one relationship', () => {
    const postWithAuthor = db.query(state).from('posts').where({ id: 1 }).with({ author: true }).first();

    expect(postWithAuthor).not.toBeNull();
    expect(postWithAuthor?.author).toBeDefined();
    expect(postWithAuthor?.author?.name).toBe('Alice');
  });

  it('should handle nested eager loading', () => {
    const userWithPostsAndComments = db.query(state).from('users').where({ id: 1 }).with({
        posts: {
            with: { comments: true }
        }
    }).first();

    expect(userWithPostsAndComments?.posts).toHaveLength(2);
    expect(userWithPostsAndComments?.posts?.[0].comments).toHaveLength(2);
    expect(userWithPostsAndComments?.posts?.[0].comments?.[0].text).toBe('Great post!');
  });

  it('should apply onDelete: CASCADE', () => {
    // Delete post with ID 1
    const [nextState, deletedPosts] = db.delete(state, 'posts').where({ id: 1 });

    expect(deletedPosts).toHaveLength(1);
    expect(nextState.posts).toBeDefined();
    if (nextState.posts) {
      expect(nextState.posts.records.find(p => p.id === 1)).toBeUndefined();
    }

    // Comments related to post 1 should also be deleted
    const comments = db.query(nextState).from('comments').where({ postId: 1 }).all();
    expect(comments).toHaveLength(0);
    expect(nextState.comments).toBeDefined();
    if(nextState.comments) {
      expect(nextState.comments.records).toHaveLength(0);
    }
  });

  it('should apply onDelete: SET NULL', () => {
    // Delete user with ID 1 (Alice)
    const [nextState, deletedUsers] = db.delete(state, 'users').where({ id: 1 });

    expect(deletedUsers).toHaveLength(1);
    
    // Alice's posts should have their authorId set to null
    const alicesPosts = db.query(nextState).from('posts').where(p => p.title.includes('Alices')).all();
    expect(alicesPosts).toHaveLength(2);
    const post1 = alicesPosts[0];
    expect(post1).toBeDefined();
    if(post1) expect(post1.authorId).toBeNull();
    
    const post2 = alicesPosts[1];
    expect(post2).toBeDefined();
    if(post2) expect(post2.authorId).toBeNull();

    // Bob's post should be unaffected
    const bobsPost = db.query(nextState).from('posts').where({ id: 2 }).first();
    expect(bobsPost?.authorId).toBe(2);
  });

  it('should not set null if not the related record', () => {
    // Delete Bob (id 2)
    const [nextState, _] = db.delete(state, 'users').where({ id: 2 });
    
    // Alice's posts should be unaffected
    const alicesPosts = db.query(nextState).from('posts').where({ authorId: 1 }).all();
    expect(alicesPosts).toHaveLength(2);

    // Bob's post should now have a null authorId
    const bobsPost = db.query(nextState).from('posts').where({ id: 2 }).first();
    expect(bobsPost).toBeDefined();
    expect(bobsPost?.authorId).toBeNull();
  });
});