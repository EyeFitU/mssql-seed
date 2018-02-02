'use strict';

const assert = require('assert');
const Promise = require('bluebird');
const VError = require('verror');
const readSqlFiles = require('./readSqlFiles');
const readFile = Promise.promisify(require('fs').readFile);

class Database {
  constructor({ name, pool }) {
    assert(name, 'database name should be provided');
    assert(pool, 'connection pool from mssql package should be passed');

    this.name = name;
    this.pool = pool;
  }

  drop() {
    return this.pool.request().batch(`DROP DATABASE [${this.name}]`);
  }

  createIfNotExists() {
    return this.pool.request().batch(`
      IF db_id('${this.name}') IS NULL
      BEGIN
        CREATE DATABASE [${this.name}]
      END
    `);
  }

  async applySchema({ directory }) {
    const filelist = await readSqlFiles(directory);
    const result = await Promise.map(filelist, async (file) => {
      const multipleSqlStatements = await readFile(file, { encoding: 'utf8' });

      try {
        return await this.pool.request().batch(multipleSqlStatements);
      } catch (err) {
        throw new VError(err, `Failed to run batch reqeust from ${file}`);
      }
    });

    return result;
  }
}

module.exports = Database;
