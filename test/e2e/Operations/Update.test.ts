import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('E2E > Operations > Update', () => {
  const dbFilePath = path.join(TEST_DIR, 'update_test.json');
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

  it('should update a single record and return it', async () => {
    let state = await db.read();
    let updatedUsers;
    [state, updatedUsers] = db.update(state, 'users')
      .set({ name: 'User A Updated', age: 21 })
      .where({ email: 'a@test.com' });

    expect(updatedUsers.length).toBe(1);
    const updatedUser = updatedUsers[0];
    expect(updatedUser?.name).toBe('User A Updated');
    expect(updatedUser?.age).toBe(21);
    expect(updatedUser?.id).toBe(1); // ID should be unchanged

    const userInState = db.query(state).from('users').where({ id: 1 }).first();
    expect(userInState?.name).toBe('User A Updated');
  });

  it('should update multiple records and return them', async () => {
    let state = await db.read();
    let updatedUsers;
    [state, updatedUsers] = db.update(state, 'users')
      .set({ isActive: false })
      .where(user => user.age < 35);

    expect(updatedUsers.length).toBe(2);
    updatedUsers.forEach(u => expect(u.isActive).toBe(false));

    const usersInState = db.query(state).from('users').all();
    expect(usersInState.filter(u => !u.isActive).length).toBe(2);
  });

  it('should not allow updating the primary key', async () => {
    let state = await db.read();
    let updatedUsers;

    [state, updatedUsers] = db.update(state, 'users')
      // @ts-expect-error - ID is not a valid key in the update type
      .set({ id: 99, name: 'ID Test' })
      .where({ id: 1 });
    
    expect(updatedUsers.length).toBe(1);
    expect(updatedUsers[0]?.id).toBe(1); // ID unchanged
    expect(updatedUsers[0]?.name).toBe('ID Test');
  });
  
  it('should throw validation error on update', async () => {
    let state = await db.read();
    
    // Make 'c@test.com' unavailable
    const failUpdate = () => {
      db.update(state, 'users')
        .set({ email: 'c@test.com' }) // Try to use an existing unique email
        .where({ id: 1 });
    };

    expect(failUpdate).toThrow(KonroValidationError);
  });

  it('should return an empty array if no records match the update predicate', async () => {
    let state = await db.read();
    let updatedUsers;
    [state, updatedUsers] = db.update(state, 'users')
      .set({ name: 'Should not be set' })
      .where({ id: 999 });

    expect(updatedUsers.length).toBe(0);
  });
});