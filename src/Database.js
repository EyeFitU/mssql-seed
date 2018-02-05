'use strict';

const assert = require('assert');
const mssql = require('mssql');
const Promise = require('bluebird');
const readFile = Promise.promisify(require('fs').readFile);

const BATCH_SPLIT_RE = /\r?\nGO\r?\n/;
const getStatementsForBatch = (sql) => {
  // Split statements by GO command, which is not part of T-SQL.
  // https://github.com/patriksimek/node-mssql/issues/282
  const statements = sql.split(BATCH_SPLIT_RE);

  // Files could have empty lines, which cause error when passed to batch
  // function. We call .trim() to prevent "Incorrect syntax near ''." error.
  return statements.map(s => s.trim());
};

class Database {
  constructor({ name, connectionOptions }) {
    assert(name, 'database name should be provided');
    assert(connectionOptions, 'connection options for mssql package should be passed');
    assert(connectionOptions.user);
    assert(connectionOptions.password);
    assert(connectionOptions.server);
    assert(connectionOptions.database);

    this.name = name;
    this.connectionOptions = connectionOptions;
  }

  openServerConnection() {
    if (this.serverConnectionPromise) {
      return this.serverConnectionPromise;
    }

    const connectionPool = new mssql.ConnectionPool(Object.assign({}, this.connectionOptions, {
      // Database name is not passed into connection because we are not sure on
      // its existence. Setting it while it doesn't exists would result in a
      // login error.
      database: undefined,
    }));

    this.serverConnectionPromise = connectionPool.connect();

    return this.serverConnectionPromise;
  }

  openDatabaseConnection() {
    if (this.databaseConnectionPromise) {
      return this.databaseConnectionPromise;
    }

    const connectionPool = new mssql.ConnectionPool(this.connectionOptions);

    this.databaseConnectionPromise = connectionPool.connect();

    return this.databaseConnectionPromise;
  }

  async close() {
    if (this.serverConnectionPromise) {
      this.serverConnectionPromise.then(pool => pool.close());
    }

    if (this.databaseConnectionPromise) {
      this.databaseConnectionPromise.then(pool => pool.close());
    }
  }

  async drop() {
    const connection = await this.openServerConnection();

    return connection.request().batch(`
      IF db_id('${this.name}') IS NOT NULL
      BEGIN
        DROP DATABASE [${this.name}]
      END
    `);
  }

  async create() {
    const connection = await this.openServerConnection();

    return connection.request().batch(`
      IF db_id('${this.name}') IS NULL
      BEGIN
        CREATE DATABASE [${this.name}]
      END
    `);
  }

  async createTable({ file }) {
    const sql = await readFile(file, { encoding: 'utf8' });
    const connection = await this.openDatabaseConnection();
    const execute = statement => connection.request().batch(statement);
    const statements = getStatementsForBatch(sql);

    return Promise.each(statements, execute);
  }
}

module.exports = Database;
