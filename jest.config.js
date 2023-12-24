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
}

let currentFile = false;
const files = fs.readdirSync('./__tests__');

// currentFile = 'sorteddict.test.ts';
// currentFile = 'dict.test.ts';
currentFile = 'table.test.ts';
// currentFile = 'db.test.ts';

if (currentFile) {
  customJestConfig.testPathIgnorePatterns = files.filter(f => f != currentFile).map(f => "<rootDir>/__tests__/" + f);
}


module.exports = customJestConfig;