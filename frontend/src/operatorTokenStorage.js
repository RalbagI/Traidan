import { MIN_OPERATOR_TOKEN_LENGTH } from './adminApi.js';

export const OPERATOR_TOKEN_STORAGE_KEY = 'traidan.operatorToken.v1';

export function readPersistedOperatorToken(storage = defaultSessionStorage()) {
  if (!storage) return '';
  try {
    const token = String(storage.getItem(OPERATOR_TOKEN_STORAGE_KEY) ?? '').trim();
    return token.length >= MIN_OPERATOR_TOKEN_LENGTH ? token : '';
  } catch {
    return '';
  }
}

export function persistOperatorToken(token, storage = defaultSessionStorage()) {
  const trimmed = String(token ?? '').trim();
  if (!storage) return trimmed;
  try {
    if (trimmed.length >= MIN_OPERATOR_TOKEN_LENGTH) {
      storage.setItem(OPERATOR_TOKEN_STORAGE_KEY, trimmed);
    } else {
      storage.removeItem(OPERATOR_TOKEN_STORAGE_KEY);
    }
  } catch {
    // Storage can be unavailable in privacy modes; controls still work in memory.
  }
  return trimmed;
}

export function forgetPersistedOperatorToken(storage = defaultSessionStorage()) {
  if (!storage) return;
  try {
    storage.removeItem(OPERATOR_TOKEN_STORAGE_KEY);
  } catch {
    // Ignore storage failures; clearing React state is handled by the caller.
  }
}

function defaultSessionStorage() {
  if (typeof window === 'undefined') return null;
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}
