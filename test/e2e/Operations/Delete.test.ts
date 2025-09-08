import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Delete', () => {
  const dbFilePath = path.join(TEST_DIR, 'delete_test.json');
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: dbFilePath },
  });
  const db = konro.createDatabase({
    schema: testSchema,
    adapter,
  });

  beforeEach(async () => {
    await ensureTestDir();
    let state = db.createEmptyState();
    const usersToInsert = [
      { name: 'User A', email: 'a@test.com', age: 20 },
      { name: 'User B', email: 'b@test.com', age: 30 },
      { name: 'User C', email: 'c@test.com', age: 40 },
    ];
    [state] = db.insert(state, 'users', usersToInsert);
    await db.write(state);
  });
  afterEach(cleanup);

  it('should delete a single record matching a predicate object', async () => {
    let state = await db.read();
    expect(db.query(state).from('users').all().length).toBe(3);

    let deletedUsers;
    [state, deletedUsers] = db.delete(state, 'users').where({ email: 'b@test.com' });

    expect(deletedUsers.length).toBe(1);
    expect(deletedUsers[0]?.name).toBe('User B');

    const remainingUsers = db.query(state).from('users').all();
    expect(remainingUsers.length).toBe(2);
    expect(remainingUsers.find(u => u.email === 'b@test.com')).toBeUndefined();
  });

  it('should delete multiple records matching a predicate function', async () => {
    let state = await db.read();
    expect(db.query(state).from('users').all().length).toBe(3);

    let deletedUsers;
    [state, deletedUsers] = db.delete(state, 'users').where(user => user.age < 35);

    expect(deletedUsers.length).toBe(2);
    expect(deletedUsers.map(u => u.name).sort()).toEqual(['User A', 'User B']);

    const remainingUsers = db.query(state).from('users').all();
    expect(remainingUsers.length).toBe(1);
    expect(remainingUsers[0]?.name).toBe('User C');
  });

  it('should return an empty array and unchanged state if no records match', async () => {
    const initialState = await db.read();
    
    const [newState, deletedUsers] = db.delete(initialState, 'users').where({ name: 'Nonexistent' });

    expect(deletedUsers.length).toBe(0);
    expect(newState).toBe(initialState); // Should be the exact same object reference
  });

  it('should persist deletions to disk', async () => {
    let state = await db.read();
    [state] = db.delete(state, 'users').where({ id: 1 });
    await db.write(state);

    const stateAfterWrite = await db.read();
    const users = db.query(stateAfterWrite).from('users').all();
    expect(users.length).toBe(2);
    expect(users.find(u => u.id === 1)).toBeUndefined();
  });
});