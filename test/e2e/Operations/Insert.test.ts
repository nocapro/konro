import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('E2E > Operations > Insert', () => {
  const dbFilePath = path.join(TEST_DIR, 'insert_test.json');
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
    await db.write(db.createEmptyState());
  });
  afterEach(cleanup);

  it('should insert a single record and return it', async () => {
    const state = await db.read();
    const [newState, newUser] = db.insert(state, 'users', {
      name: 'John Doe',
      email: 'john@test.com',
      age: 30,
    });

    expect(newUser.id).toBe(1);
    expect(newUser.name).toBe('John Doe');
    expect(newUser.isActive).toBe(true); // default value

    const usersInState = db.query(newState).from('users').all();
    expect(usersInState.length).toBe(1);
    expect(usersInState[0]).toEqual(newUser);
  });

  it('should insert multiple records and return them', async () => {
    const state = await db.read();
    const usersToInsert = [
      { name: 'Jane Doe', email: 'jane@test.com', age: 28 },
      { name: 'Peter Pan', email: 'peter@test.com', age: 100, isActive: false },
    ];
    const [newState, newUsers] = db.insert(state, 'users', usersToInsert);

    expect(newUsers.length).toBe(2);
    expect(newUsers[0]?.id).toBe(1);
    expect(newUsers[1]?.id).toBe(2);
    expect(newUsers[0]?.name).toBe('Jane Doe');
    expect(newUsers[1]?.isActive).toBe(false);

    const usersInState = db.query(newState).from('users').all();
    expect(usersInState.length).toBe(2);
  });

  it('should auto-increment IDs correctly across multiple inserts', async () => {
    let state = await db.read();
    let newUser;

    [state, newUser] = db.insert(state, 'users', { name: 'First', email: 'first@test.com', age: 20 });
    expect(newUser.id).toBe(1);

    [state, newUser] = db.insert(state, 'users', { name: 'Second', email: 'second@test.com', age: 21 });
    expect(newUser.id).toBe(2);
  });

  it('should throw validation error for duplicate unique fields', async () => {
    let state = await db.read();
    [state] = db.insert(state, 'users', { name: 'Unique User', email: 'unique@test.com', age: 40 });

    const insertDuplicate = () => {
      db.insert(state, 'users', { name: 'Another User', email: 'unique@test.com', age: 41 });
    };

    expect(insertDuplicate).toThrow("Value 'unique@test.com' for column 'email' must be unique");
  });

  it('should throw validation error for constraint violations', async () => {
    const state = await db.read();
    const insertInvalid = () => {
      db.insert(state, 'users', { name: 'A', email: 'bademail.com', age: 17 });
    };
    // It should throw on the first failure it finds. Order not guaranteed.
    // In this case, 'name' length < 2
    expect(insertInvalid).toThrow(KonroValidationError);
  });
});