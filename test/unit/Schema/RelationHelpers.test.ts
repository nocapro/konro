import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';

describe('Unit > Schema > RelationHelpers', () => {
  it('should create a valid one-to-many relationship definition object when calling konro.many()', () => {
    const manyRel = konro.many('posts', { on: 'id', references: 'authorId' });
    expect(manyRel).toEqual({
      _type: 'relation',
      relationType: 'many',
      targetTable: 'posts',
      on: 'id',
      references: 'authorId',
    });
  });

  it('should create a valid one-to-one/many-to-one relationship definition object when calling konro.one()', () => {
    const oneRel = konro.one('users', { on: 'authorId', references: 'id' });
    expect(oneRel).toEqual({
      _type: 'relation',
      relationType: 'one',
      targetTable: 'users',
      on: 'authorId',
      references: 'id',
    });
  });
});