# Directory Structure
```
package.json
test/e2e/multi-file.test.ts
test/e2e/per-record.test.ts
test/e2e/single-file.test.ts
test/integration/in-memory-aggregations.test.ts
test/integration/in-memory-crud.test.ts
test/integration/in-memory-relations.test.ts
test/test.util.ts
tsconfig.json
```

# Files

## File: package.json
```json
{
  "name": "konro-testing",
  "module": "index.ts",
  "type": "module",
  "scripts": {
    "test": "bun test"
  },
  "devDependencies": {
    "@types/bun": "latest",
    "js-yaml": "^4.1.0",
    "konro": "^0.1.7",
    "papaparse": "^5.4.1",
    "xlsx": "https://cdn.sheetjs.com/xlsx-0.20.2/xlsx-0.20.2.tgz",
    "@types/js-yaml": "^4.0.9",
    "@types/papaparse": "^5.3.14"
  },
  "peerDependencies": {
    "typescript": "^5.0.0"
  }
}
```

## File: test/e2e/multi-file.test.ts
```typescript
import { konro } from 'konro';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { testSchema, setup, cleanup, TEST_TMP_DIR } from '../test.util';
import { readdir } from 'fs/promises';
import path from 'path';

describe('E2E: Multi-File Strategy (On-Demand)', () => {
  const dir = path.join(TEST_TMP_DIR, 'multi-file-db');

  beforeAll(setup);
  afterAll(cleanup);
  beforeEach(setup);

  it('should perform CRUD operations on demand', async () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      multi: { dir },
      mode: 'on-demand',
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    // Insert user - should create a users.json file
    const user = await db.insert('users', { name: 'On-Demand User', email: 'ondemand@test.com' });
    expect(user.id).toBe(1);

    let files = await readdir(dir);
    expect(files).toContain('users.json');

    // Insert post - should create a posts.json file
    await db.insert('posts', { title: 'On-Demand Post', authorId: user.id });
    files = await readdir(dir);
    expect(files).toContain('posts.json');

    // Query with relation
    const postWithAuthor = await db.query().from('posts').where({ id: 1 }).with({ author: true }).first();
    expect(postWithAuthor).not.toBeNull();
    expect(postWithAuthor?.author?.name).toBe('On-Demand User');

    // Update
    const updatedUsers = await db.update('users').set({ name: 'Updated Name' }).where({ id: 1 });
    expect(updatedUsers).toHaveLength(1);
    const updatedUser = updatedUsers[0];
    expect(updatedUser).toBeDefined();
    if (updatedUser) {
      expect(updatedUser.name).toBe('Updated Name');
    }

    // Verify update with a fresh query
    const freshUser = await db.query().from('users').where({ id: 1 }).first();
    expect(freshUser?.name).toBe('Updated Name');

    // Delete
    const deletedUsers = await db.delete('users').where({ id: 1 });
    expect(deletedUsers).toHaveLength(1);

    const noUser = await db.query().from('users').where({ id: 1 }).first();
    expect(noUser).toBeNull();
  });
});
```

## File: test/e2e/per-record.test.ts
```typescript
import { konro } from 'konro';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { testSchema, setup, cleanup, TEST_TMP_DIR } from '../test.util';
import { readdir } from 'fs/promises';
import path from 'path';

describe('E2E: Per-Record Strategy (On-Demand)', () => {
  const dir = path.join(TEST_TMP_DIR, 'per-record-db');
  
  beforeAll(setup);
  afterAll(cleanup);
  beforeEach(setup);

  it('should perform CRUD operations on demand', async () => {
    const adapter = konro.createFileAdapter({
      format: 'json',
      perRecord: { dir },
      mode: 'on-demand',
    });
    const db = konro.createDatabase({ schema: testSchema, adapter });

    // Insert user - should create a users directory and record file
    const user = await db.insert('users', { name: 'Record User', email: 'record@test.com' });
    expect(user.id).toBe(1);

    let rootFiles = await readdir(dir);
    expect(rootFiles).toContain('users');
    let userFiles = await readdir(path.join(dir, 'users'));
    expect(userFiles).toContain('1.json');
    expect(userFiles).toContain('__meta.json');

    // Query user
    const foundUser = await db.query().from('users').where({ id: 1 }).first();
    expect(foundUser?.name).toBe('Record User');
    
    // Insert another user
    await db.insert('users', { name: 'Record User 2', email: 'record2@test.com' });
    userFiles = await readdir(path.join(dir, 'users'));
    expect(userFiles).toContain('2.json');

    // Query all users
    const allUsers = await db.query().from('users').all();
    expect(allUsers).toHaveLength(2);

    // Hard delete
    await db.delete('users').where({ id: 1 });
    userFiles = await readdir(path.join(dir, 'users'));
    expect(userFiles).not.toContain('1.json');
    
    const remainingUsers = await db.query().from('users').all();
    expect(remainingUsers).toHaveLength(1);
    const remainingUser = remainingUsers[0];
    expect(remainingUser).toBeDefined();
    if (remainingUser) {
      expect(remainingUser.id).toBe(2);
    }
  });
});
```

## File: test/e2e/single-file.test.ts
```typescript
import { konro } from 'konro';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { testSchema, setup, cleanup, TEST_TMP_DIR } from '../test.util';
import { readFile } from 'fs/promises';
import path from 'path';
import jsyaml from 'js-yaml';

describe('E2E: Single-File Strategy', () => {
  beforeAll(setup);
  afterAll(cleanup);
  beforeEach(setup); // Clean tmp dir before each test

  const formats = ['json', 'yaml'] as const;

  for (const format of formats) {
    it(`should write and read a database using ${format} format`, async () => {
      const filepath = path.join(TEST_TMP_DIR, `db.${format}`);
      const adapter = konro.createFileAdapter({
        format,
        single: { filepath },
        mode: 'in-memory',
      });
      const db = konro.createDatabase({ schema: testSchema, adapter });
      
      let state = db.createEmptyState();
      const [nextState] = db.insert(state, 'users', { name: 'E2E User', email: 'e2e@test.com' });

      await db.write(nextState);

      // Verify file content
      const fileContent = await readFile(filepath, 'utf-8');
      expect(fileContent).not.toBeNull();
      const parsedContent = format === 'json' ? JSON.parse(fileContent) : jsyaml.load(fileContent);
      expect(parsedContent.users.records[0].name).toBe('E2E User');

      // Read the state back
      const readState = await db.read();
      expect(readState.users).toBeDefined();
      if (readState.users) {
        expect(readState.users.records).toHaveLength(1);
        const user = readState.users.records[0];
        expect(user).toBeDefined();
        if (user) {
          expect(user.name).toBe('E2E User');
        }
        expect(readState.users.meta.lastId).toBe(1);
      }
    });
  }
});
```

## File: test/integration/in-memory-aggregations.test.ts
```typescript
import { konro } from 'konro';
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../test.util';
import type { DatabaseState } from 'konro';

describe('In-Memory Aggregations', () => {
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: 'dummy.json' },
    mode: 'in-memory',
  });

  const db = konro.createDatabase({ schema: testSchema, adapter });
  let state: DatabaseState<typeof testSchema>;

  beforeEach(() => {
    state = db.createEmptyState();
    const [s1] = db.insert(state, 'users', [
      { name: 'Alice', email: 'alice@example.com', age: 30 },
      { name: 'Bob', email: 'bob@example.com', age: 40 },
      { name: 'Charlie', email: 'charlie@example.com', age: 50 },
    ]);
    const [s2] = db.insert(s1, 'posts', [
      { title: 'Post 1', authorId: 1, views: 100 },
      { title: 'Post 2', authorId: 2, views: 150 },
      { title: 'Post 3', authorId: 1, views: 200 },
    ]);
    state = s2;
  });

  it('should count records', () => {
    const result = db.query(state).from('users').aggregate({ total: konro.count() });
    expect(result.total).toBe(3);
  });

  it('should sum a column', () => {
    const result = db.query(state).from('posts').aggregate({ totalViews: konro.sum('views') });
    expect(result.totalViews).toBe(450); // 100 + 150 + 200
  });

  it('should average a column', () => {
    const result = db.query(state).from('users').aggregate({ avgAge: konro.avg('age') });
    expect(result.avgAge).toBe(40); // (30 + 40 + 50) / 3
  });

  it('should find the minimum and maximum of a column', () => {
    const result = db.query(state).from('users').aggregate({
      minAge: konro.min('age'),
      maxAge: konro.max('age'),
    });
    expect(result.minAge).toBe(30);
    expect(result.maxAge).toBe(50);
  });

  it('should handle aggregations on empty sets', () => {
    const result = db.query(state).from('users').where({ name: 'Nonexistent' }).aggregate({
      total: konro.count(),
      avgAge: konro.avg('age'),
      sumViews: konro.sum('views'),
    });
    expect(result.total).toBe(0);
    expect(result.avgAge).toBeNull();
    expect(result.sumViews).toBe(0); // Sum of empty set is 0
  });

  it('should combine aggregations with a where clause', () => {
    // Sum of views for posts by author 1
    const result = db.query(state).from('posts').where({ authorId: 1 }).aggregate({
      totalViews: konro.sum('views'),
      postCount: konro.count(),
    });
    expect(result.totalViews).toBe(300); // 100 + 200
    expect(result.postCount).toBe(2);
  });
});
```

## File: test/integration/in-memory-crud.test.ts
```typescript
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
```

## File: test/integration/in-memory-relations.test.ts
```typescript
import { konro } from 'konro';
import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../test.util';
import type { DatabaseState } from 'konro';

describe('In-Memory Relations', () => {
  const adapter = konro.createFileAdapter({
    format: 'json',
    single: { filepath: 'dummy.json' },
    mode: 'in-memory',
  });

  const db = konro.createDatabase({ schema: testSchema, adapter });
  let state: DatabaseState<typeof testSchema>;

  beforeEach(() => {
    state = db.createEmptyState();
    // Seed data
    const [s1] = db.insert(state, 'users', [
      { name: 'Alice', email: 'alice@example.com', age: 30 },
      { name: 'Bob', email: 'bob@example.com', age: 40 },
    ]);
    const [s2] = db.insert(s1, 'posts', [
      { title: 'Alices Post', authorId: 1, views: 100 },
      { title: 'Bobs Post', authorId: 2, views: 150 },
      { title: 'Alices Second Post', authorId: 1, views: 200 },
    ]);
    const [s3] = db.insert(s2, 'comments', [
      { text: 'Great post!', authorId: 2, postId: 1 },
      { text: 'Thanks!', authorId: 1, postId: 1 },
    ]);
    state = s3;
  });

  it('should eager load a one-to-many relationship', () => {
    const userWithPosts = db.query(state).from('users').where({ id: 1 }).with({ posts: true }).first();
    
    expect(userWithPosts).not.toBeNull();
    expect(userWithPosts?.posts).toBeDefined();
    expect(userWithPosts?.posts).toHaveLength(2);
    expect(userWithPosts?.posts?.[0].title).toBe('Alices Post');
  });

  it('should eager load a many-to-one relationship', () => {
    const postWithAuthor = db.query(state).from('posts').where({ id: 1 }).with({ author: true }).first();

    expect(postWithAuthor).not.toBeNull();
    expect(postWithAuthor?.author).toBeDefined();
    expect(postWithAuthor?.author?.name).toBe('Alice');
  });

  it('should handle nested eager loading', () => {
    const userWithPostsAndComments = db.query(state).from('users').where({ id: 1 }).with({
        posts: {
            with: { comments: true }
        }
    }).first();

    expect(userWithPostsAndComments?.posts).toHaveLength(2);
    expect(userWithPostsAndComments?.posts?.[0].comments).toHaveLength(2);
    expect(userWithPostsAndComments?.posts?.[0].comments?.[0].text).toBe('Great post!');
  });

  it('should apply onDelete: CASCADE', () => {
    // Delete post with ID 1
    const [nextState, deletedPosts] = db.delete(state, 'posts').where({ id: 1 });

    expect(deletedPosts).toHaveLength(1);
    expect(nextState.posts).toBeDefined();
    if (nextState.posts) {
      expect(nextState.posts.records.find(p => p.id === 1)).toBeUndefined();
    }

    // Comments related to post 1 should also be deleted
    const comments = db.query(nextState).from('comments').where({ postId: 1 }).all();
    expect(comments).toHaveLength(0);
    expect(nextState.comments).toBeDefined();
    if(nextState.comments) {
      expect(nextState.comments.records).toHaveLength(0);
    }
  });

  it('should apply onDelete: SET NULL', () => {
    // Delete user with ID 1 (Alice)
    const [nextState, deletedUsers] = db.delete(state, 'users').where({ id: 1 });

    expect(deletedUsers).toHaveLength(1);
    
    // Alice's posts should have their authorId set to null
    const alicesPosts = db.query(nextState).from('posts').where(p => p.title.includes('Alices')).all();
    expect(alicesPosts).toHaveLength(2);
    const post1 = alicesPosts[0];
    expect(post1).toBeDefined();
    if(post1) expect(post1.authorId).toBeNull();
    
    const post2 = alicesPosts[1];
    expect(post2).toBeDefined();
    if(post2) expect(post2.authorId).toBeNull();

    // Bob's post should be unaffected
    const bobsPost = db.query(nextState).from('posts').where({ id: 2 }).first();
    expect(bobsPost?.authorId).toBe(2);
  });
});
```

## File: test/test.util.ts
```typescript
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
```

## File: tsconfig.json
```json
{
  "compilerOptions": {
    // Environment setup & latest features
    "lib": ["ESNext"],
    "target": "ESNext", 
    "module": "Preserve",
    "moduleDetection": "force",
    "jsx": "react-jsx",
    "allowJs": true,

    // Bundler mode
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "verbatimModuleSyntax": true,
    "noEmit": true,

    // Best practices
    "strict": true,
    "skipLibCheck": true,
    "noFallthroughCasesInSwitch": true,
    "noUncheckedIndexedAccess": false,
    "noImplicitOverride": true,

    // Some stricter flags (disabled by default)
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noPropertyAccessFromIndexSignature": false
  },
  "include": ["test/**/*.ts"],
  "exclude": ["dist"]
}
```
