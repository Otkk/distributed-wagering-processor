import { createHash } from 'crypto';

/**
 * Serializes an object to canonical JSON with sorted keys
 */
export function canonicalJson(obj: unknown): string {
  if (obj === null || typeof obj !== 'object') {
    return JSON.stringify(obj);
  }

  if (Array.isArray(obj)) {
    return '[' + obj.map((item) => canonicalJson(item)).join(',') + ']';
  }

  const sortedKeys = Object.keys(obj as Record<string, unknown>).sort();
  const entries = sortedKeys
    .filter((key) => (obj as Record<string, unknown>)[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${canonicalJson((obj as Record<string, unknown>)[key])}`);

  return '{' + entries.join(',') + '}';
}

/**
 * Computes SHA-256 hash of canonical JSON string
 */
export function computePayloadHash(payload: Record<string, unknown>): string {
  const canonical = canonicalJson(payload);
  return createHash('sha256').update(canonical).digest('hex');
}
