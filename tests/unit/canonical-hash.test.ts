import { describe, expect, it } from 'bun:test';
import { canonicalJson, computePayloadHash } from '../../src/domain/models/canonical-hash';

describe('Canonical Hash Utility', () => {
  it('should produce identical JSON string regardless of key insertion order', () => {
    const obj1 = { b: 2, a: 1, c: { y: 20, x: 10 } };
    const obj2 = { a: 1, c: { x: 10, y: 20 }, b: 2 };

    expect(canonicalJson(obj1)).toBe(canonicalJson(obj2));
    expect(computePayloadHash(obj1)).toBe(computePayloadHash(obj2));
  });

  it('should ignore undefined properties', () => {
    const obj1 = { a: 1, b: undefined };
    const obj2 = { a: 1 };

    expect(canonicalJson(obj1)).toBe(canonicalJson(obj2));
    expect(computePayloadHash(obj1)).toBe(computePayloadHash(obj2));
  });

  it('should generate different hashes for different payloads', () => {
    const obj1 = { providerId: 'prov-a', amount: '25.00' };
    const obj2 = { providerId: 'prov-a', amount: '30.00' };

    expect(computePayloadHash(obj1)).not.toBe(computePayloadHash(obj2));
  });
});
