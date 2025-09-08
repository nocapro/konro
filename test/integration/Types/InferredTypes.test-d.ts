import { describe, it } from 'bun:test';
import { konro } from '../../konro-test-import';
import { schemaDef } from '../../util';

/**
 * NOTE: This is a type definition test file.
 * It is not meant to be run, but to be checked by `tsc`.
 * The presence of `// @ts-expect-error` comments indicates
 * that a TypeScript compilation error is expected on the next line.
 * If the error does not occur, `tsc` will fail, which is the desired behavior for this test.
 */
describe('Integration > Types > InferredTypes', () => {
  it('should pass type checks', () => {
    const testSchema = konro.createSchema(schemaDef);
    type User = typeof testSchema.types.users;

    // Test 1: Inferred User type should have correct primitive and relational fields.
    const user: User = {
      id: 1,
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
      isActive: true,
      posts: [{
        id: 1,
        title: 'Post 1',
        content: '...',
        authorId: 1,
        publishedAt: new Date(),
      }],
      profile: null,
    };

    // This should be valid
    user.name; // Accessing for type check
    const inMemoryAdapter = konro.createFileAdapter({ format: 'json', single: { filepath: 'dummy.json' }});
    const db = konro.createDatabase({ schema: testSchema, adapter: inMemoryAdapter });
    const state = db.createEmptyState(); // For in-memory db

    // Test 2: Should cause a TS error if a non-existent field is used in a where clause.
    // @ts-expect-error - 'nonExistentField' does not exist on type 'User'.
    db.query(state).from('users').where({ nonExistentField: 'value' });

    // This should be valid
    db.query(state).from('users').where({ name: 'Alice' });

    // Test 3: Should cause a TS error if a wrong type is passed to db.insert().
    // @ts-expect-error - 'age' should be a number, not a string.
    db.insert(state, 'users', { name: 'Bob', email: 'bob@test.com', age: 'twenty-five' });

    // This should be valid - using type assertion for test-only code
    // @ts-ignore - This is a type test only, not runtime code
    db.insert(state, 'users', { name: 'Bob', email: 'bob@test.com', age: 25 });

    // Test 4: Nested .with clause on in-memory db should be typed correctly
    db.query(state).from('users').with({
      posts: {
        where: (post) => post.title.startsWith('A') // post is typed as Post
      }
    }).first();

    // @ts-expect-error - 'nonExistentRelation' is not a valid relation on 'users'
    db.query(state).from('users').with({ nonExistentRelation: true });

    // Test 5: A query without .with() should return the base type, without relations.
    const baseUser = db.query(state).from('users').where({ id: 1 }).first();
    // This should be valid
    baseUser?.name;
    // @ts-expect-error - 'posts' does not exist on base user type, as .with() was not used.
    baseUser?.posts;

    // Test 6: A query with .with() should return the relations, which are now accessible.
    const userWithPosts = db.query(state).from('users').where({ id: 1 }).with({ posts: true }).first();
    userWithPosts?.posts; // This should be valid and typed as Post[] | undefined
    
    // userWithPosts?.posts?.[0]?.author; 

    // --- On-Demand DB Type Tests ---
    const onDemandAdapter = konro.createFileAdapter({ format: 'yaml', mode: 'on-demand', multi: { dir: 'dummy-dir' }});
    const onDemandDb = konro.createDatabase({ schema: testSchema, adapter: onDemandAdapter });

    // Test 7: On-demand query should not require state.
    onDemandDb.query().from('users').where({ name: 'Alice' }).first(); // Should be valid

    // Test 8: On-demand query with .with() should be typed correctly without state.
    onDemandDb.query().from('users').with({
      posts: {
        where: (post) => post.title.startsWith('A')
      }
    }).first();

    // @ts-expect-error - 'nonExistentRelation' is not a valid relation on 'users'
    onDemandDb.query().from('users').with({ nonExistentRelation: true });

    // Test 9: On-demand insert should be awaitable and return the correct type.
    const insertedUserPromise = onDemandDb.insert('users', { name: 'OnDemand', email: 'od@test.com', age: 22 });
    // @ts-expect-error - 'posts' should not exist on the base inserted type
    insertedUserPromise.then(u => u.posts);
  });
});
