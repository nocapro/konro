import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Query with Relations', () => {
  const dbFilePath = path.join(TEST_DIR, 'query_with_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  let userId1: number, userId2: number;
  let postId1: number;

  beforeEach(async () => {
    await ensureTestDir();
    let state = db.createEmptyState();
    
    // Insert users
    let u1, u2;
    [state, u1] = db.insert(state, 'users', { name: 'Alice', email: 'alice@test.com', age: 30 });
    [state, u2] = db.insert(state, 'users', { name: 'Bob', email: 'bob@test.com', age: 35 });
    userId1 = u1.id; 
    userId2 = u2.id;
    
    // Insert posts
    let p1;
    [state, p1] = db.insert(state, 'posts', { title: 'Alice Post 1', content: '...', authorId: userId1 });
    [state] = db.insert(state, 'posts', { title: 'Bob Post 1', content: '...', authorId: userId2 });
    [state] = db.insert(state, 'posts', { title: 'Alice Post 2', content: '...', authorId: userId1 });
    postId1 = p1.id;

    // Insert profiles
    [state] = db.insert(state, 'profiles', { bio: 'Bio for Alice', userId: userId1 });

    await db.write(state);
  });
  afterEach(cleanup);

  it('should eager-load a one-to-many relationship', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ id: userId1 }).with({ posts: true }).first();

    expect(user).toBeDefined();
    expect(user?.posts).toBeDefined();
    expect(user?.posts?.length).toBe(2);
    expect(user?.posts?.map(p => p.title).sort()).toEqual(['Alice Post 1', 'Alice Post 2']);
  });

  it('should eager-load a many-to-one relationship', async () => {
    const state = await db.read();
    const post = db.query(state).from('posts').where({ id: postId1 }).with({ author: true }).first();

    expect(post).toBeDefined();
    expect(post?.author).toBeDefined();
    expect(post?.author?.name).toBe('Alice');
  });

  it('should eager-load a one-to-one relationship', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ id: userId1 }).with({ profile: true }).first();
    
    expect(user).toBeDefined();
    expect(user?.profile).toBeDefined();
    expect(user?.profile?.bio).toBe('Bio for Alice');
  });

  it('should return null for a one-relation if no related record exists', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ id: userId2 }).with({ profile: true }).first();
    
    expect(user).toBeDefined();
    expect(user?.profile).toBeNull();
  });

  it('should return an empty array for a many-relation if no related records exist', async () => {
    let state = await db.read();
    let newUser;
    [state, newUser] = db.insert(state, 'users', { name: 'Charlie', email: 'charlie@test.com', age: 40 });
    
    const user = db.query(state).from('users').where({ id: newUser.id }).with({ posts: true }).first();
    expect(user).toBeDefined();
    expect(user?.posts).toEqual([]);
  });

  it('should handle nested eager-loading', async () => {
    const state = await db.read();
    const post = db.query(state)
      .from('posts')
      .where({ id: postId1 })
      .with({
        author: {
          with: {
            posts: true,
            profile: true,
          },
        },
      })
      .first();

    expect(post?.author?.name).toBe('Alice');
    expect(post?.author?.profile?.bio).toBe('Bio for Alice');
    expect(post?.author?.posts?.length).toBe(2);
  });

  it('should filter related records with a `where` clause', async () => {
    const state = await db.read();
    const user = db.query(state)
      .from('users')
      .where({ id: userId1 })
      .with({
        posts: {
          where: (post) => post.title.includes('Post 2'),
        }
      })
      .first();

    expect(user?.posts?.length).toBe(1);
    expect(user?.posts?.[0]?.title).toBe('Alice Post 2');
  });

  it('should select specific fields from related records', async () => {
    const state = await db.read();
    const user = db.query(state)
        .from('users')
        .where({ id: userId1 })
        .with({
            posts: {
                select: {
                    postTitle: testSchema.tables.posts.title,
                }
            }
        })
        .first();

    expect(user?.posts?.length).toBe(2);
    expect(user?.posts?.[0]).toEqual({ postTitle: 'Alice Post 1' });
    expect(user?.posts?.[1]).toEqual({ postTitle: 'Alice Post 2' });
    // @ts-expect-error - content should not exist on the selected type
    expect(user?.posts?.[0]?.content).toBeUndefined();
  });
});