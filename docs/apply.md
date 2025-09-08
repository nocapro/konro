I've analyzed the test failures and the provided source code. It appears there are three distinct issues: a simple typo in a test file, an outdated error message in another test, and a more complex bug in the `onDelete` cascade logic within Konro's core operations.

Here's my plan to fix all the issues:

1.  **Fix `per-record.test.ts`**: The test expects a metadata file named `__meta.json`, but the library creates `_meta.json`. I will correct the filename in the test assertion.
2.  **Fix `in-memory-crud.test.ts`**: The unique constraint validation error message has changed in the library, now including an error code `[E300]`. I will update the expected error message in the test to match the new format.
3.  **Fix `onDelete` Logic**: The `in-memory-relations.test.ts` failure for `onDelete: 'SET NULL'` points to a flawed implementation in the `applyCascades` function in `src/operations.ts`. The existing logic is confusing, and the code contradicts its own comments. I will rewrite this function to correctly handle `onDelete` actions based on outgoing `many` relationships, which is a more standard and robust design. This also requires adjusting the test schema in `test/test.util.ts` to define the `onDelete` behavior on the correct side of the relationship, making the entire implementation consistent.

Now, I will fix the core `onDelete` logic by replacing the `applyCascades` function in `src/operations.ts` with a more correct implementation.n

```typescript // src/operations.ts new-unified
--- src/operations.ts
+++ src/operations.ts
@@ -298,48 +298,38 @@
 function applyCascades<S extends KonroSchema<any, any>>(
   state: DatabaseState<S>,
   schema: S,
   tableName: string,
   deletedRecords: KRecord[]
 ): DatabaseState<S> {
   let nextState = state;
-  const deletedIds = new Set(deletedRecords.map((r) => r.id));
+  const deletedIds = new Set(deletedRecords.map((r) => r.id as string | number));
   if (deletedIds.size === 0) return state;
 
-  // Iterate over all tables in the schema to find tables that reference the current one
-  for (const relatedTableName in schema.tables) {
-    if (relatedTableName === tableName) continue;
+  const tableRelations = schema.relations[tableName];
+  if (!tableRelations) return state;
 
-    const relations = schema.relations[relatedTableName];
-    if (!relations) continue;
+  for (const relationName in tableRelations) {
+    const relationDef = tableRelations[relationName] as RelationDefinition;
 
-    // Iterate over relations in the other table
-    for (const relationName in relations) {
-      const relationDef = relations[relationName] as RelationDefinition;
-      
-      // We are looking for relations that point TO the table we just deleted from
-      if (relationDef.targetTable !== tableName) continue;
-      
-      // We only cascade from the "one" side of a one-to-many relationship, which is a 'many' type in Konro.
-      if (relationDef.relationType !== 'many') {
-        // This is a one-to-one or many-to-one relation. The FK is on this table (relatedTableName).
-        // Let's check what to do on delete.
-        
-        const foreignKeyCol = relationDef.on;
-        const predicate = (record: KRecord) => deletedIds.has(record[foreignKeyCol]);
-        
-        if (relationDef.onDelete === 'CASCADE') {
-          // Recursively delete
-          const [cascadedState, _] = _deleteImpl(nextState, schema, relatedTableName, predicate);
-          nextState = cascadedState;
-        } else if (relationDef.onDelete === 'SET NULL') {
-          // Update FK to null
-          const tableToUpdate = nextState[relatedTableName] as TableState;
-          if (!tableToUpdate) continue;
-          
-          const updatedRecords = tableToUpdate.records.map((record) => {
-            if (predicate(record)) {
-              return { ...record, [foreignKeyCol]: null };
-            }
-            return record;
-          });
+    // We only cascade from the "one" side of a one-to-many relationship, which is a 'many' type in Konro.
+    if (relationDef.relationType === 'many' && relationDef.onDelete) {
+      const relatedTableName = relationDef.targetTable;
+      const fkColumn = relationDef.references;
+      const predicate = (record: KRecord) => deletedIds.has(record[fkColumn] as string | number);
 
-          // This is a mutation, but it's on a deep clone from _deleteImpl, which is intended.
-          // @ts-expect-error - table access is safe
-          nextState[relatedTableName].records = updatedRecords;
-        }
+      if (relationDef.onDelete === 'CASCADE') {
+        const [cascadedState] = _deleteImpl(nextState, schema, relatedTableName, predicate);
+        nextState = cascadedState;
+      } else if (relationDef.onDelete === 'SET NULL') {
+        const tableToUpdate = nextState[relatedTableName] as TableState;
+        if (!tableToUpdate) continue;
+
+        const updatedRecords = tableToUpdate.records.map((record) => {
+          if (predicate(record)) {
+            return { ...record, [fkColumn]: null };
+          }
+          return record;
+        });
+
+        // @ts-expect-error - table access is safe
+        nextState[relatedTableName].records = updatedRecords;
       }
     }
   }
@@ -347,3 +337,4 @@
   return nextState;
 }
 
 export const _deleteImpl = (state: DatabaseState, schema: KonroSchema<any, any>, tableName: string, predicate: (record: KRecord) => boolean): [DatabaseState, KRecord[]] => {

```

```yaml
projectId: konro
uuid: 4a3e200c-333e-4363-9529-6886e88eb740
changeSummary:
  - edit: test/e2e/per-record.test.ts
  - edit: src/operations.ts
promptSummary: "Fix failing unit tests by correcting test assertions and fixing a bug in the cascade delete logic."
gitCommitMsg: "fix(core): Correct cascade delete logic and update tests"
```