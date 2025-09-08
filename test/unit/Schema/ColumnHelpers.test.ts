import { describe, it, expect } from 'bun:test';
import { konro } from '../../konro-test-import';

describe('Unit > Schema > ColumnHelpers', () => {
  it('should create a valid ID column definition object when calling konro.id()', () => {
    const idCol = konro.id();
    expect(idCol).toEqual({
      _type: 'column',
      dataType: 'id',
      options: { unique: true, _pk_strategy: 'auto-increment' },
      _tsType: 0,
    });
  });

  it('should create a valid UUID column definition object when calling konro.uuid()', () => {
    const uuidCol = konro.uuid();
    expect(uuidCol).toEqual({
      _type: 'column',
      dataType: 'id',
      options: { unique: true, _pk_strategy: 'uuid' },
      _tsType: '',
    });
  });

  it('should create a valid string column definition with no options', () => {
    const stringCol = konro.string();
    expect(stringCol).toEqual({
      _type: 'column',
      dataType: 'string',
      options: undefined,
      _tsType: '',
    });
  });

  it('should create a valid string column definition with all specified options', () => {
    const defaultFn = () => 'default';
    const stringCol = konro.string({
      unique: true,
      default: defaultFn,
      min: 5,
      max: 100,
      format: 'email',
    });
    expect(stringCol).toEqual({
      _type: 'column',
      dataType: 'string',
      options: {
        unique: true,
        default: defaultFn,
        min: 5,
        max: 100,
        format: 'email',
      },
      _tsType: '',
    });
  });

  it('should create a valid number column definition with no options', () => {
    const numberCol = konro.number();
    expect(numberCol).toEqual({
      _type: 'column',
      dataType: 'number',
      options: undefined,
      _tsType: 0,
    });
  });

  it('should create a valid number column definition with all specified options', () => {
    const numberCol = konro.number({
      unique: false,
      default: 0,
      min: 0,
      max: 1000,
      type: 'integer',
    });
    expect(numberCol).toEqual({
      _type: 'column',
      dataType: 'number',
      options: {
        unique: false,
        default: 0,
        min: 0,
        max: 1000,
        type: 'integer',
      },
      _tsType: 0,
    });
  });

  it('should create a valid boolean column with no options', () => {
    const boolCol = konro.boolean();
    expect(boolCol).toEqual({
      _type: 'column',
      dataType: 'boolean',
      options: undefined,
      _tsType: false,
    });
  });

  it('should create a valid boolean column definition with a default value', () => {
    const boolCol = konro.boolean({ default: false });
    expect(boolCol).toEqual({
      _type: 'column',
      dataType: 'boolean',
      options: { default: false },
      _tsType: false,
    });
  });

  it('should create a valid date column definition with no options', () => {
    const dateCol = konro.date();
    expect(dateCol).toEqual({
      _type: 'column',
      dataType: 'date',
      options: undefined,
      _tsType: expect.any(Date),
    });
  });

  it('should create a valid date column definition with a default function', () => {
    const defaultDateFn = () => new Date();
    const dateCol = konro.date({ default: defaultDateFn });
    expect(dateCol).toEqual({
      _type: 'column',
      dataType: 'date',
      options: { default: defaultDateFn },
      _tsType: expect.any(Date),
    });
    expect(dateCol.options?.default).toBe(defaultDateFn);
  });

  it('should create a valid string column with a literal default', () => {
    const stringCol = konro.string({ default: 'hello' });
    expect(stringCol).toEqual({
      _type: 'column',
      dataType: 'string',
      options: { default: 'hello' },
      _tsType: '',
    });
  });

  it('should create a valid number column with a function default', () => {
    const defaultFn = () => 42;
    const numberCol = konro.number({ default: defaultFn });
    expect(numberCol).toEqual({
      _type: 'column',
      dataType: 'number',
      options: {
        default: defaultFn,
      },
      _tsType: 0,
    });
    expect(numberCol.options?.default).toBe(defaultFn);
  });

  it('should create a valid boolean column with a function default', () => {
    const defaultFn = () => true;
    const boolCol = konro.boolean({ default: defaultFn });
    expect(boolCol).toEqual({
      _type: 'column',
      dataType: 'boolean',
      options: {
        default: defaultFn,
      },
      _tsType: false,
    });
    expect(boolCol.options?.default).toBe(defaultFn);
  });

  it('should create a valid object column definition', () => {
    const objCol = konro.object<{ meta: string }>();
    expect(objCol).toMatchObject({
      _type: 'column',
      dataType: 'object',
      options: undefined,
    });
  });
});