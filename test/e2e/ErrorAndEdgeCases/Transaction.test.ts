import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('E2E > ErrorAndEdgeCases > Transaction', () => {
  const dbFilePath = path.join(TEST_DIR, 'transaction_test.json');
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
    // Start with a clean slate for each test
    await db.write(db.createEmptyState());
  });
  afterEach(cleanup);

    it('should not write to disk if an operation fails mid-transaction', async () => {
        // 1. Get initial state with one user
        let state = await db.read();
        [state] = db.insert(state, 'users', { name: 'Good User', email: 'good@test.com', age: 30, isActive: true });
        await db.write(state);

    const contentBefore = await fs.readFile(dbFilePath, 'utf-8');

    // 2. Start a "transaction": read, then perform multiple operations
    let transactionState = await db.read();

        // This one is fine
        [transactionState] = db.insert(transactionState, 'users', { name: 'Another User', email: 'another@test.com', age: 31, isActive: true });

        // This one will fail due to unique constraint
        const failingOperation = () => {
            db.insert(transactionState, 'users', { name: 'Bad User', email: 'good@test.com', age: 32, isActive: true });
        };
        expect(failingOperation).toThrow(KonroValidationError);

    // Even if the error is caught, the developer should not write the tainted `transactionState`.
    // The file on disk should remain untouched from before the transaction started.
    const contentAfter = await fs.readFile(dbFilePath, 'utf-8');
    expect(contentAfter).toEqual(contentBefore);
  });

    it('should not change the database file if an update matches no records', async () => {
        let state = await db.read();
        [state] = db.insert(state, 'users', { name: 'Initial User', email: 'initial@test.com', age: 50, isActive: true });
        await db.write(state);

    const contentBefore = await fs.readFile(dbFilePath, 'utf-8');

    // Read the state to perform an update
    let currentState = await db.read();
    const [newState] = await db.update(currentState, 'users')
      .set({ name: 'This Should Not Be Set' })
      .where({ id: 999 }); // This matches no records

    await db.write(newState);

    const contentAfter = await fs.readFile(dbFilePath, 'utf-8');

    // The content should be identical because the state object itself shouldn't have changed meaningfully.
    expect(contentAfter).toEqual(contentBefore);
  });
});
