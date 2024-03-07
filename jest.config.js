const fs = require('fs');

// Any custom config you want to pass to Jest
const customJestConfig = {
  // setupFilesAfterEnv: ['<rootDir>/jest.setup.js'],
  "testMatch": [
    "<rootDir>/__tests__/*.test.ts"
  ],
  // "testPathIgnorePatterns": [
  //   "<rootDir>/__tests__/lemmatizer.test.ts"
  // ]
  maxWorkers: 1,
}

let currentFile = false;
const files = fs.readdirSync('./__tests__');

// Uncomment for working with a single test file

// currentFile = 'virtual_fs.test.ts';
// currentFile = 'sorteddict.test.ts';
// currentFile = 'dict.test.ts';
// currentFile = 'table.test.ts';
// currentFile = 'table_events.test.ts';
currentFile = 'table_index.test.ts';
// currentFile = 'utils.test.ts';
// currentFile = 'fragmented_dict_stress.test.ts';
// currentFile = 'table_stress.test.ts';
// currentFile = 'db.test.ts';

const runStressTests = false;

customJestConfig.testPathIgnorePatterns = [];
if (currentFile) {
  customJestConfig.testPathIgnorePatterns = files.filter(f => f != currentFile).map(f => "<rootDir>/__tests__/" + f);
}

if (!runStressTests && !currentFile) {
  customJestConfig.testPathIgnorePatterns.push(
    "<rootDir>/__tests__/table_stress.test.ts",
    "<rootDir>/__tests__/fragmented_dict_stress.test.ts",
  );
}

module.exports = customJestConfig;