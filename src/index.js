import { Handler, model } from 'dblink-core';
import sqlite from 'sqlite3';
import stream from 'stream';
export default class Sqlite extends Handler {
  connectionPool;
  async init() {
    this.connectionPool = await new Promise((res, rej) => {
      const temp = new sqlite.Database(this.config.database, err => {
        if (err) rej(err);
      });
      res(temp);
    });
  }
  async getConnection() {
    return this.connectionPool;
  }
  async initTransaction(conn) {
    await new Promise((res, rej) => {
      conn.run('BEGIN TRANSACTION', (data, err) => {
        if (err) rej(err);
        else res(data);
      });
    });
  }
  async commit(conn) {
    await new Promise((res, rej) => {
      conn.run('COMMIT', (data, err) => {
        if (err) rej(err);
        else res(data);
      });
    });
  }
  async rollback(conn) {
    await new Promise((res, rej) => {
      conn.run('ROLLBACK', (data, err) => {
        if (err) rej(err);
        else res(data);
      });
    });
  }
  async close() {}
  async run(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool;
    const data = await new Promise((res, rej) => {
      conn.all(query, dataArgs, function (err, r) {
        if (err) {
          rej(err);
        } else {
          res(r);
        }
      });
    });
    const result = new model.ResultSet();
    result.rows = data;
    result.rowCount = data.length;
    return result;
  }
  runStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }
  async stream(query, dataArgs, connection) {
    const conn = connection ?? this.connectionPool;
    const dataStream = new stream.Duplex();
    conn.each(
      query,
      dataArgs,
      (err, row) => {
        if (err) throw err;
        dataStream.write(row);
      },
      err => {
        if (err) throw err;
        dataStream.write(null);
      }
    );
    return dataStream;
  }
  streamStatement(queryStmt, connection) {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
//# sourceMappingURL=index.js.map
