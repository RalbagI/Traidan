import assert from 'node:assert/strict';
import test from 'node:test';

import { createTraceCopy } from './traceCopy.js';

test('research-only trace copy never claims an LLM thesis or held trade', () => {
  const copy = createTraceCopy({ researchOnly: true });

  assert.equal(copy.thesisLabel, '2 - מסקנת מחקר ציבורי');
  assert.match(copy.intentEmpty, /LLM לא הופעל/);
  assert.equal(copy.resultLabel, '6 - תוצאת המחקר');
  assert.match(copy.resultEmpty, /ללא קריאת LLM וללא פקודה/);
  assert.doesNotMatch(Object.values(copy).join(' '), /תזה של LLM|הדסק החזיק/);
});

test('trade-cycle trace copy retains the decision terminology', () => {
  const copy = createTraceCopy({ researchOnly: false });

  assert.equal(copy.thesisLabel, '2 - תזה של LLM');
  assert.equal(copy.resultLabel, '6 - תוצאת היומן');
});
