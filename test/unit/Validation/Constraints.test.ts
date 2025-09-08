import { describe, it, expect, beforeEach } from 'bun:test';
import { testSchema } from '../../util';
import { _insertImpl, _updateImpl } from '../../../src/operations';
import { DatabaseState } from '../../../src/types';
import { KonroValidationError } from '../../../src/utils/error.util';

describe('Unit > Validation > Constraints', () => {
    let testState: DatabaseState;

    beforeEach(() => {
        testState = {
            users: {
                records: [{ id: 1, name: 'Alice', email: 'alice@example.com', age: 30, isActive: true }],
                meta: { lastId: 1 },
            },
            posts: { records: [], meta: { lastId: 0 } },
            profiles: { records: [], meta: { lastId: 0 } },
            tags: { records: [], meta: { lastId: 0 } },
            posts_tags: { records: [], meta: { lastId: 0 } },
        };
    });

    // NOTE: These tests are expected to fail until validation is implemented in core operations.
    // This is intentional to highlight the missing functionality as per the test plan.
    
    it('should throw a KonroValidationError when inserting a record with a non-unique value', () => {
        const user = { name: 'Bob', email: 'alice@example.com', age: 25 };
        // This should throw because 'alice@example.com' is already used and `email` is unique.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });

    it('should throw a KonroValidationError for a string that violates a format: email constraint', () => {
        const user = { name: 'Bob', email: 'bob@invalid', age: 25 };
        // This should throw because the email format is invalid.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });

    it('should throw a KonroValidationError for a number smaller than the specified min', () => {
        const user = { name: 'Bob', email: 'bob@example.com', age: 17 }; // age.min is 18
        // This should throw because age is below min.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });

    it('should throw a KonroValidationError for a string shorter than the specified min', () => {
        const user = { name: 'B', email: 'bob@example.com', age: 25 }; // name.min is 2
        // This should throw because name is too short.
        expect(() => _insertImpl(testState, testSchema, 'users', [user])).toThrow(KonroValidationError);
    });
    
    it('should throw a KonroValidationError on update for a non-unique value', () => {
        // Add another user to create conflict
        testState.users!.records.push({ id: 2, name: 'Charlie', email: 'charlie@example.com', age: 40, isActive: true });
        testState.users!.meta.lastId = 2;

        const predicate = (r: any) => r.id === 2;
        const data = { email: 'alice@example.com' }; // Try to update charlie's email to alice's

        expect(() => _updateImpl(testState, testSchema, 'users', data, predicate)).toThrow(KonroValidationError);
    });
});