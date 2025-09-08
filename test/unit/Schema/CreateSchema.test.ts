import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';

describe('Unit > Schema > CreateSchema', () => {
  it('should correctly assemble a full schema object from tables and relations definitions', () => {
    const tableDefs = {
      users: {
        id: konro.id(),
        name: konro.string(),
      },
      posts: {
        id: konro.id(),
        title: konro.string(),
        authorId: konro.number(),
      },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
      relations: () => ({
        users: {
          posts: konro.many('posts', { on: 'id', references: 'authorId' }),
        },
        posts: {
          author: konro.one('users', { on: 'authorId', references: 'id' }),
        },
      }),
    });

    expect(schema.tables).toBe(tableDefs);
    expect(schema.relations).toBeDefined();
    expect(schema.relations.users.posts).toBeDefined();
    expect(schema.relations.posts.author).toBeDefined();
    expect(schema.types).toBeNull(); // Runtime placeholder
  });

  it('should handle schemas with no relations defined', () => {
    const tableDefs = {
      logs: {
        id: konro.id(),
        message: konro.string(),
      },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
    });

    expect(schema.tables).toBe(tableDefs);
    expect(schema.relations).toEqual({});
  });

  it('should handle schemas where relations function returns an empty object', () => {
    const tableDefs = {
      users: {
        id: konro.id(),
        name: konro.string(),
      },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
      relations: () => ({}),
    });

    expect(schema.tables).toBe(tableDefs);
    expect(schema.relations).toEqual({});
  });

  it('should handle schemas with multiple relations on one table', () => {
    const tableDefs = {
      users: { id: konro.id(), name: konro.string() },
      posts: { id: konro.id(), title: konro.string(), authorId: konro.number(), editorId: konro.number() },
    };

    const schema = konro.createSchema({
      tables: tableDefs,
      relations: () => ({
        posts: {
          author: konro.one('users', { on: 'authorId', references: 'id' }),
          editor: konro.one('users', { on: 'editorId', references: 'id' }),
        },
      }),
    });

    expect(schema.relations.posts.author).toBeDefined();
    expect(schema.relations.posts.editor).toBeDefined();
    expect(schema.relations.posts.author.targetTable).toBe('users');
    expect(schema.relations.posts.editor.targetTable).toBe('users');
  });
});