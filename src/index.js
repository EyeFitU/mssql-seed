'use strict';

const assert = require('assert');
const mssql = require('mssql');
const Database = require('./Database');

module.exports = (options) => {
  return new Database({ name: options.database, connectionOptions: options });
};
