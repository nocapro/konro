import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';

describe('E2E > Operations > Query', () => {
  const dbFilePath = path.join(TEST_DIR, 'query_test.json');
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
      { name: 'Alice', email: 'alice@test.com', age: 29, isActive: true },
      { name: 'Bob', email: 'bob@test.com', age: 35, isActive: false },
      { name: 'Charlie', email: 'charlie@test.com', age: 30, isActive: true },
    ];
    [state] = db.insert(state, 'users', usersToInsert);
    await db.write(state);
  });
  afterEach(cleanup);

  it('should return all records from a table', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').all();
    expect(users.length).toBe(3);
  });

  it('should filter records using a `where` object predicate', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').where({ age: 30, isActive: true }).all();
    expect(users.length).toBe(1);
    expect(users[0]?.name).toBe('Charlie');
  });

  it('should filter records using a `where` function predicate', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').where(u => u.name.startsWith('A') || u.name.startsWith('B')).all();
    expect(users.length).toBe(2);
    expect(users.map(u => u.name).sort()).toEqual(['Alice', 'Bob']);
  });

  it('should return a single record using `first()`', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ email: 'bob@test.com' }).first();
    expect(user).toBeDefined();
    expect(user?.name).toBe('Bob');
  });

  it('should return null from `first()` if no record matches', async () => {
    const state = await db.read();
    const user = db.query(state).from('users').where({ name: 'Zelda' }).first();
    expect(user).toBeNull();
  });

  it('should limit the number of results', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').limit(2).all();
    expect(users.length).toBe(2);
  });

  it('should offset the results for pagination', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').offset(1).all();
    expect(users.length).toBe(2);
    expect(users[0]?.name).toBe('Bob');
  });

  it('should combine limit and offset', async () => {
    const state = await db.read();
    const users = db.query(state).from('users').limit(1).offset(1).all();
    expect(users.length).toBe(1);
    expect(users[0]?.name).toBe('Bob');
  });

  it('should select and rename specific fields', async () => {
    const state = await db.read();
    const partialUsers = db.query(state)
      .from('users')
      .where({ name: 'Alice' })
      .select({
        userName: testSchema.tables.users.name,
        userEmail: testSchema.tables.users.email,
      })
      .all();

    expect(partialUsers.length).toBe(1);
    const user = partialUsers[0];
    expect(user as any).toEqual({ userName: 'Alice', userEmail: 'alice@test.com' });
    expect((user as any).age).toBeUndefined();
  });
});