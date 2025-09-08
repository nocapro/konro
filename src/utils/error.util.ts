import { KONRO_ERROR_CODES, type KonroErrorCode } from './error.codes';

type ErrorContext = Record<string, string | number | undefined | null>;

const renderTemplate = (template: string, context: ErrorContext): string => {
  return template.replace(/\{\{([^}]+)\}\}/g, (_match, key) => {
    const value = context[key.trim()];
    return value !== undefined && value !== null ? String(value) : `{{${key}}}`;
  });
};

// Per user request: no classes. Using constructor functions for errors.
const createKonroError = (name: string) => {
  function KonroErrorConstructor(messageOrContext: string | ({ code: KonroErrorCode } & ErrorContext)) {
    let message: string;
    let code: KonroErrorCode | undefined;

    if (typeof messageOrContext === 'string') {
      message = messageOrContext;
    } else {
      code = messageOrContext.code;
      const template = KONRO_ERROR_CODES[code] || 'Unknown error code.';
      message = `[${code}] ${renderTemplate(template, messageOrContext)}`;
    }

    const error = new Error(message) as Error & { code?: KonroErrorCode };
    error.name = name;
    error.code = code;
    Object.setPrototypeOf(error, KonroErrorConstructor.prototype);
    return error;
  }
  Object.setPrototypeOf(KonroErrorConstructor.prototype, Error.prototype);
  return KonroErrorConstructor;
};

/** Base constructor for all Konro-specific errors. */
export const KonroError = createKonroError('KonroError');

/** Thrown for storage adapter-related issues. */
export const KonroStorageError = createKonroError('KonroStorageError');

/** Thrown for schema validation errors. */
export const KonroValidationError = createKonroError('KonroValidationError');

/** Thrown when a resource is not found. */
export const KonroNotFoundError = createKonroError('KonroNotFoundError');