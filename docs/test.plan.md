
### **Directory Structure Plan**

```
test/util.test.ts
test/
├── unit/
│   ├── Schema/
│   │   ├── ColumnHelpers.test.ts
│   │   ├── RelationHelpers.test.ts
│   │   └── CreateSchema.test.ts
│   ├── Core/
│   │   ├── Insert.test.ts
│   │   ├── Update.test.ts
│   │   ├── Delete.test.ts
│   │   ├── Query.test.ts
│   │   └── Query-With.test.ts
│   └── Validation/
│       └── Constraints.test.ts
├── integration/
│   ├── DBContext/
│   │   └── Initialization.test.ts
│   ├── Adapters/
│   │   ├── SingleFileJson.test.ts
│   │   ├── MultiFileYaml.test.ts
│   │   └── Read.test.ts
│   ├── InMemoryFlow/
│   │   └── CrudCycle.test.ts
│   └── Types/
│       └── InferredTypes.test-d.ts
└── e2e/
    ├── SingleFileJson/
    │   └── FullLifecycle.test.ts
    ├── MultiFileYaml/
    │   └── FullLifecycle.test.ts
    └── ErrorAndEdgeCases/
        ├── Transaction.test.ts
        └── Pagination.test.ts
```

---

### **Unit Tests (`test/unit/`)**

*   **`test/unit/Schema/ColumnHelpers.test.ts`**
    *   it should create a valid ID column definition object when calling `konro.id()`
    *   it should create a valid string column definition with all specified options (unique, default, min, max, format)
    *   it should create a valid number column definition with all specified options (unique, default, min, max, type)
    *   it should create a valid boolean column definition with a default value
    *   it should create a valid date column definition with a default function

*   **`test/unit/Schema/RelationHelpers.test.ts`**
    *   it should create a valid one-to-many relationship definition object when calling `konro.many()`
    *   it should create a valid one-to-one/many-to-one relationship definition object when calling `konro.one()`

*   **`test/unit/Schema/CreateSchema.test.ts`**
    *   it should correctly assemble a full schema object from tables and relations definitions

*   **`test/unit/Core/Insert.test.ts`**
    *   it should return a new state object, not mutate the original state, on insert
    *   it should correctly increment the `lastId` in the table's metadata
    *   it should assign the new `id` to the inserted record
    *   it should apply default values for fields that are not provided
    *   it should apply default values from a function call, like for dates
    *   it should successfully insert multiple records in a single call
    *   it should return both the new state and the newly created record(s) in the result tuple

*   **`test/unit/Core/Update.test.ts`**
    *   it should return a new state object, not mutate the original state, on update
    *   it should only update records that match the `where` predicate object
    *   it should only update records that match the `where` predicate function
    *   it should correctly modify the fields specified in the `set` payload
    *   it should not change the `id` of an updated record
    *   it should return both the new state and an array of the full, updated records in the result tuple
    *   it should return an empty array of updated records if the predicate matches nothing

*   **`test/unit/Core/Delete.test.ts`**
    *   it should return a new state object, not mutate the original state, on delete
    *   it should only delete records that match the `where` predicate object
    *   it should only delete records that match the `where` predicate function
    *   it should return both the new state and an array of the full, deleted records in the result tuple
    *   it should not modify the table's `lastId` metadata on delete

*   **`test/unit/Core/Query.test.ts`**
    *   it should select all fields from a table when `.select()` is omitted
    *   it should select only the specified fields when using `.select()`
    *   it should filter records correctly using a `where` object for equality checks
    *   it should filter records correctly using a complex `where` function
    *   it should limit the number of returned records correctly using `.limit()`
    *   it should skip the correct number of records using `.offset()`
    *   it should correctly handle `limit` and `offset` together for pagination
    *   it should return an array of all matching records when using `.all()`
    *   it should return the first matching record when using `.first()`
    *   it should return null when `.first()` finds no matching record

*   **`test/unit/Core/Query-With.test.ts`**
    *   it should resolve a `one` relationship and attach it to the parent record when using `.with()`
    *   it should resolve a `many` relationship and attach it as an array to the parent record when using `.with()`
    *   it should filter nested records within a `.with()` clause
    *   it should select specific fields of nested records within a `.with()` clause

*   **`test/unit/Validation/Constraints.test.ts`**
    *   it should throw a `KonroValidationError` when inserting a record with a non-unique value for a `unique: true` field
    *   it should throw a `KonroValidationError` for a string that violates a `format: 'email'` constraint
    *   it should throw a `KonroValidationError` for a number smaller than the specified `min`
    *   it should throw a `KonroValidationError` for a string longer than the specified `max`
    *   it should throw a `KonroValidationError` for data with an incorrect primitive type (e.g., number for a string field)

### **Integration Tests (`test/integration/`)**

*   **`test/integration/DBContext/Initialization.test.ts`**
    *   it should successfully create a `db` context with a valid schema and adapter
    *   it should correctly generate a pristine, empty `DatabaseState` object via `db.createEmptyState()`
    *   it should have the full schema definition available at `db.schema` for direct reference in queries

*   **`test/integration/InMemoryFlow/CrudCycle.test.ts`**
    *   it should allow inserting a record into an empty state and then immediately querying for it
    *   it should correctly chain mutation operations by passing the `newState` from one operation to the next
    *   it should update a record and verify the change in the returned `newState`
    *   it should delete a record and verify its absence in the returned `newState`
    *   it should correctly execute a query with a `.with()` clause on an in-memory state containing related data

*   **`test/integration/Adapters/SingleFileJson.test.ts`**
    *   it should call the filesystem to write to a temporary file, then rename it, when `db.write()` is used with the single-file adapter
    *   it should correctly serialize the `DatabaseState` to JSON format

*   **`test/integration/Adapters/MultiFileYaml.test.ts`**
    *   it should call the filesystem to write multiple temporary files, then rename all of them, when `db.write()` is used with the multi-file adapter
    *   it should correctly serialize the `DatabaseState` to YAML format if `js-yaml` is installed

*   **`test/integration/Adapters/Read.test.ts`**
    *   it should correctly read and parse a single JSON file into a `DatabaseState` object
    *   it should correctly read and parse multiple YAML files into a `DatabaseState` object
    *   it should create and return a valid empty state when `db.read()` is called and the target file does not exist
    *   it should throw a `KonroStorageError` if the underlying file read fails

*   **`test/integration/Types/InferredTypes.test-d.ts`**
    *   it should produce a fully-typed `User` object (including relational `posts` array) when a type is inferred from a schema with relations
    *   it should cause a TypeScript compilation error if a non-existent field is used in a `where` clause
    *   it should cause a TypeScript compilation error if a value of the wrong type is passed to `db.insert()`

### **End-to-End (E2E) Tests (`test/e2e/`)**

*   **`test/e2e/SingleFileJson/FullLifecycle.test.ts`**
    *   it should initialize an empty database file when `db.write()` is called with an empty state
    *   it should insert a user, then a post, write to disk, read back, and verify the data integrity
    *   it should perform a complex query with relations on the re-read state and get the expected nested object
    *   it should update a record, write the change, read it back, and confirm the update was persisted
    *   it should delete a record, write the change, read it back, and confirm the record is gone

*   **`test/e2e/MultiFileYaml/FullLifecycle.test.ts`**
    *   it should create separate `users.yaml` and `posts.yaml` files in the specified directory
    *   it should successfully write and read relational data across multiple YAML files
    *   it should maintain data integrity after a full cycle of read -> insert -> update -> delete -> write -> read

*   **`test/e2e/ErrorAndEdgeCases/Transaction.test.ts`**
    *   it should prevent writing to disk if an insert operation fails due to a unique constraint violation
    *   it should handle an update `.where()` clause that matches no records, resulting in no change to the database file after `db.write()`

*   **`test/e2e/ErrorAndEdgeCases/Pagination.test.ts`**
    *   it should correctly paginate through a large set of records using `.limit()` and `.offset()` and return the correct data slice from the file
