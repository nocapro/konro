import { konro } from 'konro';
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../test.util';
import type { DatabaseState } from 'konro';

describe('In-Memory CRUD Operations', () => {
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: 'dummy.json' }, // Path is not used by in-memory mode
    mode: 'in-memory',
  });

  const db = konro.createDatabase({ schema: testSchema, adapter });
  let state: DatabaseState<typeof testSchema>;

  beforeEach(() => {
    state = db.createEmptyState();
  });

  it('should insert a single record and retrieve it', () => {
    const [nextState, insertedUser] = db.insert(state, 'users', {
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
    });

    expect(insertedUser.id).toBe(1);
    expect(insertedUser.name).toBe('Alice');
    expect(nextState.users).toBeDefined();
    if (nextState.users) {
      expect(nextState.users.records).toHaveLength(1);
      expect(nextState.users.meta.lastId).toBe(1);
    }

    const user = db.query(nextState).from('users').where({ id: 1 }).first();
    expect(user).not.toBeNull();
    expect(user?.name).toBe('Alice');
    expect(user?.age).toBe(30);
  });

  it('should insert multiple records at once', () => {
    const [nextState, inserted] = db.insert(state, 'users', [
      { name: 'Bob', email: 'bob@example.com' },
      { name: 'Charlie', email: 'charlie@example.com' },
    ]);

    expect(inserted).toHaveLength(2);
    expect(inserted[0].id).toBe(1);
    expect(inserted[1].id).toBe(2);
    expect(nextState.users).toBeDefined();
    if (nextState.users) {
      expect(nextState.users.records).toHaveLength(2);
      expect(nextState.users.meta.lastId).toBe(2);
    }
  });

  it('should throw an error for duplicate unique fields', () => {
    const [nextState] = db.insert(state, 'users', { name: 'Alice', email: 'alice@example.com' });
    
    expect(() => {
      db.insert(nextState, 'users', { name: 'Alicia', email: 'alice@example.com' });
    }).toThrow('Validation Error: Value \'alice@example.com\' for column \'email\' must be unique.');
  });

  it('should update a record and auto-update `updatedAt` field', async () => {
    const [s1, user] = db.insert(state, 'users', { name: 'Old Name', email: 'test@test.com' });
    const originalUpdatedAt = user.updatedAt;

    // Ensure a small delay for timestamp comparison
    await new Promise(resolve => setTimeout(resolve, 10));

    const [, updatedUsers] = db.update(s1, 'users').set({ name: 'New Name' }).where({ id: user.id });

    expect(updatedUsers).toHaveLength(1);
    const updatedUser = updatedUsers[0];
    expect(updatedUser).toBeDefined();
    if (updatedUser) {
      expect(updatedUser.name).toBe('New Name');
      expect(updatedUser.id).toBe(user.id);
      expect(updatedUser.updatedAt.getTime()).toBeGreaterThan(originalUpdatedAt.getTime());
    }
  });
  
  it('should perform a soft delete', () => {
    const [s1] = db.insert(state, 'users', { name: 'ToDelete', email: 'delete@me.com' });
    const [s2, deletedUsers] = db.delete(s1, 'users').where({ email: 'delete@me.com' });
    
    expect(deletedUsers).toHaveLength(1);
    const deletedUser = deletedUsers[0];
    expect(deletedUser).toBeDefined();
    if (deletedUser) expect(deletedUser.deletedAt).toBeInstanceOf(Date);
    
    // Should not be found by default query
    const user = db.query(s2).from('users').where({ email: 'delete@me.com' }).first();
    expect(user).toBeNull();

    // Should be found with withDeleted()
    const softDeletedUser = db.query(s2).from('users').withDeleted().where({ email: 'delete@me.com' }).first();
    expect(softDeletedUser).not.toBeNull();
    expect(softDeletedUser?.deletedAt).not.toBeNull();
  });

  it('should perform a hard delete', () => {
    const hardDeleteSchema = konro.createSchema({
        tables: {
            items: { id: konro.id(), name: konro.string() }
        }
    });
    const dbHard = konro.createDatabase({ schema: hardDeleteSchema, adapter });
    let localState = dbHard.createEmptyState();

    const [s1] = dbHard.insert(localState, 'items', { name: 'Temp' });
    const [s2, deletedItems] = dbHard.delete(s1, 'items').where({ name: 'Temp' });

    expect(deletedItems).toHaveLength(1);
    expect(s2.items).toBeDefined();
    if (s2.items) {
      expect(s2.items.records).toHaveLength(0);
    }
  });

  it('should paginate results with limit and offset', () => {
    const [s1] = db.insert(state, 'users', [
        { name: 'User 1', email: 'user1@test.com' },
        { name: 'User 2', email: 'user2@test.com' },
        { name: 'User 3', email: 'user3@test.com' },
        { name: 'User 4', email: 'user4@test.com' },
    ]);

    const page1 = db.query(s1).from('users').limit(2).all();
    expect(page1).toHaveLength(2);
    const user1 = page1[0];
    expect(user1).toBeDefined();
    if (user1) {
      expect(user1.name).toBe('User 1');
    }

    const page2 = db.query(s1).from('users').limit(2).offset(2).all();
    expect(page2).toHaveLength(2);
    const user3 = page2[0];
    expect(user3).toBeDefined();
    if (user3) {
      expect(user3.name).toBe('User 3');
    }
  });
});