import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { konro } from '../../konro-test-import';
import { testSchema, TEST_DIR, cleanup, ensureTestDir, uuidTestSchema } from '../../util';
import path from 'path';
import { promises as fs } from 'fs';
import yaml from 'js-yaml';
import papaparse from 'papaparse';
import xlsx from 'xlsx';
import { KonroError } from '../../../src/utils/error.util';
import type { OnDemandDbContext } from '../../../src/db';

describe('Integration > Adapters > OnDemand', () => {
  const dbDirPath = path.join(TEST_DIR, 'on_demand_db');

  beforeEach(ensureTestDir);
  afterEach(cleanup);

  describe('Initialization', () => {
    it('should successfully create an on-demand db context with a multi-file adapter', () => {
      const adapter = konro.createFileAdapter({
        format: 'yaml',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({
        schema: testSchema,
        adapter,
      });

      expect(db).toBeDefined();
      expect(db.adapter.mode).toBe('on-demand');
      expect(typeof db.insert).toBe('function');
      expect(typeof db.query).toBe('function');
    });

    it('should throw an error when creating an on-demand db context with a single-file adapter', () => {
      expect(() => {
        konro.createFileAdapter({
          format: 'json',
          mode: 'on-demand',
          single: { filepath: path.join(dbDirPath, 'db.json') },
        });
      }).toThrow(KonroError({ code: 'E104' }));
    });
  });

  describe('Unsupported Operations', () => {
    const adapter = konro.createFileAdapter({
      format: 'yaml',
      mode: 'on-demand',
      multi: { dir: dbDirPath },
    });
    const db = konro.createDatabase({
      schema: testSchema,
      adapter,
    });
    
    it('should reject db.read()', async () => {
      expect(db.read()).rejects.toThrow(KonroError({ code: 'E400', methodName: 'read' }));
    });

    it('should reject db.write()', async () => {
      expect(db.write()).rejects.toThrow(KonroError({ code: 'E400', methodName: 'write' }));
    });
  });

  describe('CRUD Operations', () => {
    let db: OnDemandDbContext<typeof testSchema>;

    beforeEach(() => {
      const adapter = konro.createFileAdapter({
        format: 'yaml',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      db = konro.createDatabase({
        schema: testSchema,
        adapter,
      });
    });

    it('should insert a record and write it to the correct file', async () => {
      const user = await db.insert('users', {
        name: 'OnDemand User',
        email: 'ondemand@test.com',
        age: 25,
      });

      expect(user.id).toBe(1);
      expect(user.name).toBe('OnDemand User');

      const userFilePath = path.join(dbDirPath, 'users.yaml');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsedContent = yaml.load(fileContent) as any;

      expect(parsedContent.records.length).toBe(1);
      expect(parsedContent.records[0].name).toBe('OnDemand User');
      expect(parsedContent.meta.lastId).toBe(1);
    });

    it('should query for records', async () => {
      await db.insert('users', { name: 'Query User', email: 'q@test.com', age: 30 });
      
      const user = await db.query().from('users').where({ name: 'Query User' }).first();
      expect(user).toBeDefined();
      expect(user?.name).toBe('Query User');

      const allUsers = await db.query().from('users').all();
      expect(allUsers.length).toBe(1);
    });

    it('should update a record', async () => {
      const user = await db.insert('users', { name: 'Update Me', email: 'u@test.com', age: 40 });
      
      const updatedUsers = await db.update('users')
        .set({ name: 'Updated Name' })
        .where({ id: user.id });

      expect(updatedUsers.length).toBe(1);
      expect(updatedUsers[0]?.name).toBe('Updated Name');

      const userFilePath = path.join(dbDirPath, 'users.yaml');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsedContent = yaml.load(fileContent) as any;
      
      expect(parsedContent.records[0].name).toBe('Updated Name');
    });

    it('should delete a record', async () => {
      const user = await db.insert('users', { name: 'Delete Me', email: 'd@test.com', age: 50 });
      
      await db.delete('users').where({ id: user.id });

      const users = await db.query().from('users').all();
      expect(users.length).toBe(0);

      const userFilePath = path.join(dbDirPath, 'users.yaml');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsedContent = yaml.load(fileContent) as any;
      
      expect(parsedContent.records.length).toBe(0);
    });
    
    it('should query with relations', async () => {
      const user = await db.insert('users', { name: 'Author', email: 'author@test.com', age: 35 });
      await db.insert('posts', { title: 'Post by Author', content: '...', authorId: user.id });
      await db.insert('posts', { title: 'Another Post', content: '...', authorId: user.id });
      
      const userWithPosts = await db.query().from('users').where({ id: user.id }).with({ posts: true }).first();
      
      expect(userWithPosts).toBeDefined();
      expect(userWithPosts?.name).toBe('Author');
      expect(userWithPosts?.posts).toBeInstanceOf(Array);
      expect(userWithPosts?.posts?.length).toBe(2);
      expect(userWithPosts?.posts?.[0]?.title).toBe('Post by Author');
    });

    it('should perform aggregations', async () => {
      await db.insert('users', { name: 'Agg User 1', email: 'agg1@test.com', age: 20 });
      await db.insert('users', { name: 'Agg User 2', email: 'agg2@test.com', age: 30 });
      
      const result = await db.query().from('users').aggregate({
        count: konro.count(),
        avgAge: konro.avg('age'),
        sumAge: konro.sum('age'),
      });
      
      expect(result.count).toBe(2);
      expect(result.avgAge).toBe(25);
      expect(result.sumAge).toBe(50);
    });
  });

  describe('ID Generation', () => {
    it('should auto-increment IDs for new CSV files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'csv_db');
      const adapter = konro.createFileAdapter({
        format: 'csv',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const user1 = await db.insert('users', { name: 'CSV User 1', email: 'csv1@test.com', age: 20 });
      expect(user1.id).toBe(1);

      const user2 = await db.insert('users', { name: 'CSV User 2', email: 'csv2@test.com', age: 21 });
      expect(user2.id).toBe(2);

      // Verify file content
      const userFilePath = path.join(dbDirPath, 'users.csv');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const parsed = papaparse.parse(fileContent, { header: true, dynamicTyping: true, skipEmptyLines: true });
      expect(parsed.data).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 1, name: 'CSV User 1', email: 'csv1@test.com', age: 20, isActive: true }),
          expect.objectContaining({ id: 2, name: 'CSV User 2', email: 'csv2@test.com', age: 21, isActive: true }),
        ])
      );
    });

    it('should auto-increment IDs for new XLSX files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'xlsx_db');
      const adapter = konro.createFileAdapter({
        format: 'xlsx',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const user1 = await db.insert('users', { name: 'XLSX User 1', email: 'xlsx1@test.com', age: 20 });
      expect(user1.id).toBe(1);

      const user2 = await db.insert('users', { name: 'XLSX User 2', email: 'xlsx2@test.com', age: 21 });
      expect(user2.id).toBe(2);

      // Verify file content
      const userFilePath = path.join(dbDirPath, 'users.xlsx');
      const fileContent = await fs.readFile(userFilePath, 'utf-8');
      const workbook = xlsx.read(fileContent, { type: 'base64' });
      const sheetName = workbook.SheetNames[0];
      expect(sheetName).toBeDefined();
      const worksheet = workbook.Sheets[sheetName!];
      expect(worksheet).toBeDefined();
      const data = xlsx.utils.sheet_to_json(worksheet!);
      expect(data).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 1, name: 'XLSX User 1', email: 'xlsx1@test.com', age: 20, isActive: true }),
          expect.objectContaining({ id: 2, name: 'XLSX User 2', email: 'xlsx2@test.com', age: 21, isActive: true }),
        ])
      );
    });

    it('should determine lastId from existing CSV files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'csv_db_read');
      const userFilePath = path.join(dbDirPath, 'users.csv');

      // Manually create a CSV with existing data
      await fs.mkdir(dbDirPath, { recursive: true });
      const initialCsv = papaparse.unparse([{ id: 5, name: 'Existing User', email: 'existing@test.com', age: 50, isActive: true }]);
      await fs.writeFile(userFilePath, initialCsv);

      const adapter = konro.createFileAdapter({ format: 'csv', mode: 'on-demand', multi: { dir: dbDirPath } });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const newUser = await db.insert('users', { name: 'New CSV User', email: 'newcsv@test.com', age: 25 });
      expect(newUser.id).toBe(6);
    });

    it('should determine lastId from existing XLSX files', async () => {
      const dbDirPath = path.join(TEST_DIR, 'xlsx_db_read');
      const userFilePath = path.join(dbDirPath, 'users.xlsx');

      // Manually create an XLSX with existing data
      await fs.mkdir(dbDirPath, { recursive: true });
      const initialData = [{ id: 10, name: 'Existing XLSX User', email: 'existing_xlsx@test.com', age: 60, isActive: false }];
      const worksheet = xlsx.utils.json_to_sheet(initialData);
      const workbook = xlsx.utils.book_new();
      xlsx.utils.book_append_sheet(workbook, worksheet, 'data');
      const fileContent = xlsx.write(workbook, { bookType: 'xlsx', type: 'base64' });
      await fs.writeFile(userFilePath, fileContent, 'utf-8');

      const adapter = konro.createFileAdapter({ format: 'xlsx', mode: 'on-demand', multi: { dir: dbDirPath } });
      const db = konro.createDatabase({ schema: testSchema, adapter });

      const newUser = await db.insert('users', { name: 'New XLSX User', email: 'newxlsx@test.com', age: 35 });
      expect(newUser.id).toBe(11);
    });

    it('should generate UUIDs for id column', async () => {
      const dbDirPath = path.join(TEST_DIR, 'uuid_db');
      const adapter = konro.createFileAdapter({
        format: 'yaml',
        mode: 'on-demand',
        multi: { dir: dbDirPath },
      });
      const db = konro.createDatabase({ schema: uuidTestSchema, adapter });

      const user = await db.insert('uuid_users', { name: 'UUID User' });
      expect(typeof user.id).toBe('string');
      expect(user.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);

      const fileContent = await fs.readFile(path.join(dbDirPath, 'uuid_users.yaml'), 'utf-8');
      const parsed = yaml.load(fileContent) as any;
      expect(parsed.records[0].id).toBe(user.id);
    });
  });
});