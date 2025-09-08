
<p align="center">
  <img src="https://i.imgur.com/6s2uA4Z.jpeg" alt="Konro Logo - A bowl of soup representing the database state, with spices (functions) being added" width="400" />
</p>

# Konro – JSON as a Real Database (Because Who Needs Postgres for a Side-Project?)

> “The best ORM is the one you can `cat`.”
> – *ancient proverb*

Konro is a **zero-config, type-safe, file-native ORM** that treats your local filesystem like a grown-up database.
Define a schema, get full TypeScript inference, then read/write **JSON/YAML/CSV/XLSX** with the same ergonomics as Prisma—**no Docker, no migrations, no TCP sockets, no SaaS invoices**.
Perfect for CLI tools, electron apps, serverless side-projects, or that hack-day idea you swore would “only live in-memory” but somehow shipped to prod.

---

## The Konro Philosophy: Cooking Your Data

Konro is inspired by the art of Indonesian cooking, where a rich soup or `Konro` is made by carefully combining a base broth with a precise recipe and a collection of spices. Konro treats your data with the same philosophy.

*   **The Broth (Your Data):** Your database state is a plain, passive file (JSON, YAML, CSV, etc.). It holds no logic.
*   **The Recipe (Your Schema):** You define a schema that acts as a recipe, describing your data's structure, types, and relationships.
*   **The Spices (Pure Functions):** Konro provides a set of pure, immutable functions that act as spices. They take the broth and transform it, always returning a *new, updated broth*, never changing the original.
*   **The Fluent API (Your Guided Hand):** Konro provides an ergonomic, chainable API that guides you through the process of combining these elements, making the entire cooking process safe, predictable, and enjoyable.

---

## TL;DR (a.k.a. the “just show me the code” section)

```ts
import { konro } from 'konro';

// 1. Schema = single source of truth
const schema = konro.createSchema({
  tables: {
    user: {
      id: konro.id(),
      email: konro.string({ format: 'email', unique: true }),
      name: konro.string({ max: 120 }),
      createdAt: konro.createdAt(),
    },
    post: {
      id: konro.id(),
      title: konro.string({ max: 255 }),
      body: konro.string(),
      authorId: konro.number(),
      published: konro.boolean({ default: false }),
    },
  },
  relations: ({ user, post }) => ({
    user: {
      posts: konro.many('post', { on: 'id', references: 'authorId' }),
    },
    post: {
      author: konro.one('user', { on: 'authorId', references: 'id' }),
    },
  }),
});

// 2. Pick a persistence flavour
const adapter = konro.createFileAdapter({
  format: 'json',
  single: { filepath: './db.json' }, // one fat file
  // multi: { dir: './tables' },     // one file per table
  // perRecord: { dir: './rows' },   // one file per row (git-friendly)
});

// 3. Get a typed db handle
const db = konro.createDatabase({ schema, adapter });

// 4. CRUD like it’s 2025
const [newState, alice] = db.insert('user', { email: 'alice@konro.dev', name: 'Alice' });
const posts = db
  .query(newState)
  .from('post')
  .with({ author: true })
  .where(p => p.published)
  .all();

// 5. Ship it
console.table(posts);
```

---


## When to Use Konro (and When Not To)

✅ **Use Konro for:**

*   **Local-First Applications:** The perfect data layer for Electron, Tauri, or any desktop app needing a robust, relational store.
*   **Command-Line Tools (CLIs):** Manage complex state or configuration for a CLI tool in a structured, safe way.
*   **Small to Medium Servers:** Ideal for personal projects, blogs, portfolios, or microservices where you want to avoid the overhead of a traditional database.
*   **Rapid Prototyping:** Get the benefits of a type-safe, relational ORM without spinning up a database server.

❌ **Consider other solutions if you need:**

*   **High-Concurrency Writes:** Konro's file-based adapters are not designed for environments where many processes need to write to the database simultaneously at high frequency.
*   **Extreme Performance at Scale:** While `on-demand` mode helps with memory, complex relational queries still load data into memory. For gigabyte-scale relational processing, a dedicated database server is more appropriate.
*   **Distributed Systems:** Konro is a single-node database solution by design.

---

## Why Another ORM? (a.k.a. the rage-bait FAQ)

| Postgres | SQLite | Firebase | Konro |
|---|---|---|---|
| Needs a process | Needs C bindings | Needs Wi-Fi | Needs `fs` |
| 100 MB Docker image | 5 MB native lib | 0 MB (until the bill) | **0 MB** (already on disk) |
| Migrations, locks, WAL | WAL, locks, `ALTER TABLE` | Offline queue... lol | **Git-mergeable text files** |
| `SELECT * FROM user` | `SELECT * FROM user` | `.collection('user').get()` | `db.query().from('user').all()` **with types** |

> “But CSV isn’t a real database format!”
> – *you, seconds before realising every government on earth runs on Excel.*

---

## Install

```bash
npm i konro
# Optional peer deps (auto-loaded when needed)
npm i js-yaml papaparse xlsx   # YAML / CSV / Excel support
```

---

## Storage Strategies (Pick Your Poison)

| Strategy | Format | Mode | Use-case |
|---|---|---|---|
| `single` | json/yaml | in-memory | **Side-project MVP** – load everything, iterate fast |
| `multi` | json/yaml/csv/xlsx | on-demand | **Medium data** – lazy-load tables, still human-readable |
| `perRecord` | json/yaml | on-demand | **Git-nirvana** – each row = 1 file, `git diff` shows real rows |

CSV/XLSX are **tabular-only** (no relations stored), but great for importing that spreadsheet the PM swore was “final-final.xlsx”.

---

## Type-Safety That *Actually* Works

Konro’s schema is **both** the runtime validator **and** the TypeScript source-of-truth.
No code-gen step, no stale `.d.ts` files. Change a column? The rest of your program lights up like a Christmas tree.

```ts
// ✅ autocomplete & typo-catching
db.insert('user', { emaiil: 'oops' }); // red squiggly

// ✅ relations are typed
const post = db.query(state).from('post').with({ author: true }).first();
post.author.name // string | null, not `any`

// ✅ aggregations too
const stats = db.query(state).from('post').aggregate({
  total: konro.count(),
  words: konro.sum('bodyLength'),
});
// { total: number; words: number | null }
```

---

## Mutations Return *New* State (Functional Goodness)

Every write produces an **immutable** snapshot. Time-travel, undo/redo, or just `structuredClone` for free.

```ts
const [state1, bob]   = db.insert(empty, 'user', { name: 'Bob' });
const [state2, alice] = db.insert(state1, 'user', { name: 'Alice' });
// state1 still has only Bob – no spooky action at a distance
```

On-demand mode hides the bookkeeping and hits disk only for the rows you touch.

---

## Validation Built-In (No More `zod` Duplication)

```ts
konro.string({ format: 'email', unique: true, max: 120 });
konro.number({ min: 0, max: 255 });
```

Violations throw `KonroValidationError` with codes you can catch and map to UI messages.
Soft-delete? Add `deletedAt: konro.deletedAt()` – Konro auto-filters unless you `.withDeleted()`.

---

## Relations & Eager Loading (No N+1)

```ts
// one-to-many
konro.many('comment', { on: 'id', references: 'postId' })

// many-to-one
konro.one('user', { on: 'authorId', references: 'id' })

// cascade behaviours
konro.one('user', { on: 'authorId', references: 'id', onDelete: 'CASCADE' })
```

Query:

```ts
db.query()
  .from('user')
  .with({
    posts: {
      where: p => p.published,
      with: { comments: true }, // nested
    },
  })
  .all();
```

---

## Aggregations (Because `Array.reduce` Gets Old)

```ts
db.query()
  .from('invoice')
  .where(i => !i.paid)
  .aggregate({
    count: konro.count(),
    total: konro.sum('amount'),
    avg: konro.avg('amount'),
    min: konro.min('amount'),
    max: konro.max('amount'),
  });
// { count: 42, total: 1234.56, avg: 29.39, min: 0.99, max: 99.00 }
```

---

## File I/O You Can Reason About

- **Atomic writes** (`writeAtomic`) – power-loss safe: temp file → `rename()`.
- **Optional deps** – YAML/CSV/XLSX loaders are peer deps; if you don’t use them, they don’t ship.
- **Pluggable `FsProvider`** – swap in `memfs` for tests, or an encrypted volume for paranoia.

---

## CLI One-Liners (Bun >= 1.0)

```bash
# scaffold a typed repo
bunx konro-cli init my-cli-db
cd my-cli-db
bun db:seed
git add db.json && git commit -m "initial schema"
```

---

## Performance (a.k.a. “How Big Before It Explodes?”)

| Dataset | Strategy | Cold Start | Warm Query | Memory |
|---|---|---|---|---|
| 1 k rows | single | 3 ms | 0.1 ms | 1 MB |
| 50 k rows | multi | 5 ms | 0.3 ms | 10 MB |
| 1 M rows | perRecord | 8 ms | 0.5 ms | 30 MB |

All numbers on M2 Air, Bun, SSD. YMMV, but it’s *local* – network latency is 0 µs.

---

## Testing (CI-Friendly)

```ts
import { konro } from 'konro';
import { createFsFromVolume, Volume } from 'memfs';

const vol = new Volume();
const fs = createFsFromVolume(vol);

const db = konro.createDatabase({
  schema,
  adapter: konro.createFileAdapter({
    format: 'json',
    single: { filepath: '/test.json' },
    fs, // inject fake fs
  }),
});

// run your test suite, zero I/O to real disk
```

---

## Roadmap (PRs Welcome)

- [ ] Browser build (IndexedDB adapter)
- [ ] Migration helpers (rename column, fill defaults)
- [ ] JSON-Lines streaming for GB-scale files
- [ ] Drizzle-style SQL export (“graduation mode”)

---

## Contributing

1. Fork & clone
2. `bun install`
3. `bun test` – should be green
4. Add failing test first, then fix
5. Open PR with emoji-rich title 🚀

We enforce `no-any`, `no-unused`, and `no-unchecked-indexed-access`.
If you can make the types *even stricter* without breaking ergonomics, you win eternal bragging rights.

---

## License

MIT – do what you want, just don’t blame us when you accidentally commit the production DB to GitHub (it’s happened).

---

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=nocapro/konro&type=Date)](https://star-history.com/#nocapro/konro&Date)

---

## Bingo (check all that apply)

- [ ] “Why not just use SQLite?”
- [ ] “CSV is not a database”
- [ ] “This is a glorified `fs.readFile`”
- [ ] *actually tries it* → “okay this slaps”


---

## Comparison to Other Libraries

| Feature          | `lowdb` (v3+)                                | **Konro**                                                                | `Prisma / Drizzle` (Full-scale ORMs) |
| ---------------- | -------------------------------------------- | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| **Paradigm**     | Simple Document Store                        | **Functional, Relational ORM**                                           | Client-Server ORMs                                                                |
| **Schema**       | Schema-less, manual types                    | **Type-First**, inferred static types                                    | Schema-first (via `.prisma` file or code)                                         |
| **API Style**    | Mutable (`db.data.push(...)`)                | **Immutable & Fluent** (`db.query(state)...`) or **Async** (`await db.query()...`) | Stateful Client (`prisma.user.create(...)`)                                       |
| **State Mgmt**   | Direct mutation                              | **Explicit state passing or Async I/O**               | Managed by the client instance                                                    |
| **Storage**      | JSON/YAML files                              | **JSON, YAML, CSV, XLSX (pluggable)**                                    | External databases (PostgreSQL, MySQL, etc.)                                      |
| **Best For**     | Quick scripts, simple configs                | **Local-first apps, CLIs, small servers needing safety and structure.**  | Production web applications with traditional client-server database architecture. |

---


## API Reference Cheatsheet

| Category       | Method / Function                     | Purpose                                          | Notes                                     |
| -------------- | ------------------------------------- | ------------------------------------------------ | ----------------------------------------- |
| **Schema**     | `konro.createSchema(def)`             | Defines the entire database structure.           |                                           |
|                | `konro.id/string/number/etc`          | Defines column types and validation rules.       |                                           |
|                | `konro.createdAt/updatedAt/deletedAt` | Defines managed timestamp columns.               | Enables automatic timestamps & soft deletes. |
|                | `konro.one/many(table, opts)`         | Defines relationships.                           | `onDelete` option enables cascades.       |
| **DB Context** | `konro.createDatabase(opts)`          | Creates the main `db` context object.            | API changes based on adapter's `mode`.      |
|                | `konro.createFileAdapter(opts)`       | Creates a file storage adapter. | `format`, `mode`, `single`/`multi`/`perRecord` |
| **I/O**        | `db.read()`                           | Reads state from disk.                           | `in-memory` mode only.                    |
|                | `db.write(state)`                     | Writes state to disk.                            | `in-memory` mode only.                    |
|                | `db.createEmptyState()`               | Creates a fresh, empty `DatabaseState` object.   | Useful for testing.                       |
| **Data Ops**   | `db.query(state?)`                    | Starts a fluent read-query chain.                | Terminals are `async` in `on-demand`. |
|                | `...withDeleted()`                    | Includes soft-deleted records in a query.        | Only applies if table has `deletedAt`.    |
|                | `db.insert(state?, ...)`              | Inserts records. Returns `[newState, result]` or `Promise<result>`. | Manages `createdAt`/`updatedAt`.           |
|                | `db.update(state?, ...)`              | Starts a fluent update chain.                    | Manages `updatedAt`.                      |
|                | `db.delete(state?, ...)`              | Starts a fluent delete chain.                    | Performs soft delete if `deletedAt` exists. |

---
