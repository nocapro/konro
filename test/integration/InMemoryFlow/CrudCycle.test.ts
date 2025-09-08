import { describe, it, expect, beforeEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema } from '../../util';
import path from 'path';
import type { InMemoryDbContext } from '../../../src/db';
import type { DatabaseState } from '../../../src/types';

describe('Integration > InMemoryFlow > CrudCycle', () => {
  let db: InMemoryDbContext<typeof testSchema>;
  let state: DatabaseState<typeof testSchema>;

  beforeEach(() => {
    // Adapter is needed for context creation, but we won't use its I/O
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });
    db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });
    state = db.createEmptyState();
  });

  it('should allow inserting a record and then immediately querying for it', () => {
    const [newState, insertedUser] = db.insert(state, 'users', {
      name: 'InMemory Alice',
      email: 'alice@inmemory.com',
      age: 30,
      isActive: true,
    });
    expect(insertedUser.id).toBe(1);

    const users = db.query(newState).from('users').all();
    expect(users.length).toBe(1);
    expect(users[0]).toEqual(insertedUser);
  });

  it('should correctly chain mutation operations by passing the newState', () => {
    // Insert user
    const [stateAfterUserInsert, user] = db.insert(state, 'users', {
      name: 'Chain User',
      email: 'chain@test.com',
      age: 40,
      isActive: true,
    });

    // Insert post using the new state
    const [stateAfterPostInsert, post] = db.insert(stateAfterUserInsert, 'posts', {
      title: 'Chained Post',
      content: '...',
      authorId: user.id,
      publishedAt: new Date(),
    });

    expect(stateAfterPostInsert.users.records.length).toBe(1);
    expect(stateAfterPostInsert.posts.records.length).toBe(1);
    expect(post.authorId).toBe(user.id);
  });

  it('should update a record and verify the change in the returned newState', () => {
    const [stateAfterInsert, user] = db.insert(state, 'users', {
      name: 'Update Me',
      email: 'update@test.com',
      age: 50,
      isActive: true,
    });

    const [stateAfterUpdate, updatedUsers] = db.update(stateAfterInsert, 'users')
      .set({ name: 'Updated Name' })
      .where({ id: user.id });

    expect(updatedUsers.length).toBe(1);
    expect(updatedUsers[0]?.name).toBe('Updated Name');

    const queriedUser = db.query(stateAfterUpdate).from('users').where({ id: user.id }).first();
    expect(queriedUser?.name).toBe('Updated Name');
    expect(stateAfterInsert.users.records[0]?.name).toBe('Update Me'); // Original state is untouched
  });

  it('should delete a record and verify its absence in the returned newState', () => {
    const [stateAfterInsert, user] = db.insert(state, 'users', {
      name: 'Delete Me',
      email: 'delete@test.com',
      age: 60,
      isActive: true,
    });

    const [stateAfterDelete, deletedUsers] = db.delete(stateAfterInsert, 'users')
      .where({ id: user.id });

    expect(deletedUsers.length).toBe(1);
    expect(deletedUsers[0]?.name).toBe('Delete Me');

    const users = db.query(stateAfterDelete).from('users').all();
    expect(users.length).toBe(0);
  });

  it('should correctly execute a query with a .with() clause on an in-memory state', () => {
    const [s1, user] = db.insert(state, 'users', {
      name: 'Relation User',
      email: 'relation@test.com',
      age: 35,
      isActive: true,
    });
    const [s2, ] = db.insert(s1, 'posts', [
        { title: 'Relational Post 1', content: '...', authorId: user.id, publishedAt: new Date() },
        { title: 'Relational Post 2', content: '...', authorId: user.id, publishedAt: new Date() },
    ]);

    const userWithPosts = db.query(s2).from('users').where({ id: user.id }).with({ posts: true }).first();

    expect(userWithPosts).toBeDefined();
    expect(userWithPosts?.name).toBe('Relation User');
    expect(userWithPosts?.posts).toBeInstanceOf(Array);
    expect(userWithPosts?.posts?.length).toBe(2);
    expect(userWithPosts?.posts?.[0]?.title).toBe('Relational Post 1');
  });
});
