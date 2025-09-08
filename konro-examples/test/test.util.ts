import { konro } from 'konro';
import { rm, mkdir } from 'fs/promises';
import path from 'path';

export const TEST_TMP_DIR = path.resolve(process.cwd(), 'test', '.tmp');

/**
 * Cleans up the temporary directory.
 */
export const cleanup = async () => {
  await rm(TEST_TMP_DIR, { recursive: true, force: true });
};

/**
 * Sets up a clean temporary directory for a test run.
 */
export const setup = async () => {
  await cleanup();
  await mkdir(TEST_TMP_DIR, { recursive: true });
};

/**
 * A comprehensive schema for use in tests, covering various column types,
 * validations, and relationships.
 */
export const testSchema = konro.createSchema({
  tables: {
    users: {
      id: konro.id(),
      name: konro.string(),
      email: konro.string({ unique: true, format: 'email' }),
      age: konro.number({ optional: true, min: 0 }),
      createdAt: konro.createdAt(),
      updatedAt: konro.updatedAt(),
      deletedAt: konro.deletedAt(),
    },
    posts: {
      id: konro.id(),
      title: konro.string({ min: 3 }),
      authorId: konro.number({ optional: true }), // Optional to test SET NULL
      published: konro.boolean({ default: false }),
      views: konro.number({ default: 0 }),
    },
    comments: {
        id: konro.id(),
        text: konro.string(),
        authorId: konro.number(),
        postId: konro.number(),
    }
  },
  relations: (_tables: unknown) => ({
    users: {
      posts: konro.many('posts', { on: 'id', references: 'authorId' }),
    },
    posts: {
      author: konro.one('users', { on: 'authorId', references: 'id', onDelete: 'SET NULL' }),
      comments: konro.many('comments', { on: 'id', references: 'postId', onDelete: 'CASCADE' }),
    },
    comments: {
        author: konro.one('users', { on: 'authorId', references: 'id' }),
        post: konro.one('posts', { on: 'postId', references: 'id' }),
    }
  }),
});

// Export the inferred schema type for type-safe usage in tests
export type TestSchema = typeof testSchema;