// This file is used to easily switch the import source for 'konro' during testing.
// A script can replace the export line below to target 'src', 'dist', or the 'konro' package.
//
// For example:
// To test against src:       export * from '../src/index';
// To test against dist (mjs): export * from '../dist/index.mjs';
// To test against dist (js):  export * from '../dist/index.js';
// To test against npm package: export * from 'konro';
export * from '../src/index';
// export * from '../dist/index.mjs';
// export * from '../dist/index.js';