import js from '@eslint/js';
import react from 'eslint-plugin-react';

const browserGlobals = {
  clearTimeout: 'readonly',
  console: 'readonly',
  document: 'readonly',
  setTimeout: 'readonly',
  WebSocket: 'readonly',
  window: 'readonly',
};

export default [
  js.configs.recommended,
  {
    files: ['src/**/*.{js,jsx}'],
    plugins: {
      react,
    },
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'module',
      parserOptions: {
        ecmaFeatures: {
          jsx: true,
        },
      },
      globals: browserGlobals,
    },
    rules: {
      'react/jsx-uses-vars': 'error',
    },
  },
  {
    files: ['src/*.test.js'],
    languageOptions: {
      globals: {
        ...browserGlobals,
        global: 'readonly',
      },
    },
  },
];
