import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema } from '../../util';
import path from 'path';

describe('Integration > DBContext > Initialization', () => {
  it('should successfully create a db context with a valid schema and adapter', () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });

    const db = konro.createDatabase({
      schema: testSchema,
      adapter: adapter,
    });

    expect(db).toBeDefined();
    expect(db.schema).toEqual(testSchema);
    expect(db.adapter).toBe(adapter);
    expect(typeof db.read).toBe('function');
    expect(typeof db.write).toBe('function');
    expect(typeof db.insert).toBe('function');
    expect(typeof db.update).toBe('function');
    expect(typeof db.delete).toBe('function');
    expect(typeof db.query).toBe('function');
  });

  it('should correctly generate a pristine, empty DatabaseState object via db.createEmptyState()', () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });
    const db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });

    const emptyState = db.createEmptyState();

    expect(emptyState).toEqual({
      users: { records: [], meta: { lastId: 0 } },
      posts: { records: [], meta: { lastId: 0 } },
      profiles: { records: [], meta: { lastId: 0 } },
      tags: { records: [], meta: { lastId: 0 } },
      posts_tags: { records: [], meta: { lastId: 0 } },
    });
  });

  it('should have the full schema definition available at db.schema for direct reference in queries', () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      single: { filepath: path.join(__dirname, 'test.db.json') },
    });
    const db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });

    // Example of using db.schema to reference a column definition
    const userEmailColumn = db.schema.tables.users.email;
    expect(userEmailColumn).toEqual(testSchema.tables.users.email);
    expect(userEmailColumn.dataType).toBe('string');
  });
});