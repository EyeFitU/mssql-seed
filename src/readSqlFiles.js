'use strict';

const path = require('path');
const recursive = require('recursive-readdir');

function ignoreNonSQL(file, stats) {
  // Don't ignore directories as we have all SQL all in sub-directories.
  if (stats.isDirectory()) {
    return false;
  }

  return path.extname(file) !== '.sql';
}

/**
 * @returns {Promise<string>}
 */
module.exports = function readSqlFiles(directory) {
  return recursive(directory, [ignoreNonSQL]);
}
