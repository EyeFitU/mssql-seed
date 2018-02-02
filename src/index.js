'use strict';

const assert = require('assert');
const mssql = require('mssql');
const Database = require('./Database');

module.exports = async (options) => {
  assert(options.user);
  assert(options.password);
  assert(options.server);
  assert(options.database);

  const pool = await mssql.connect(Object.assign({}, options, {
    // Database name is not passed into connection because we are not sure on
    // its existence. Setting it while it doesn't exists would result in a
    // login error.
    database: undefined,
  }));

  return new Database({ name: options.database, pool });
};
