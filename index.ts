import { Handler, model, sql } from 'dblink-core';
import sqlite from 'sqlite3';
import stream from 'stream';

/**
 * Sqlite Handler
 *
 * @export
 * @class Sqlite
 * @typedef {Sqlite}
 * @extends {Handler}
 */
export default class Sqlite extends Handler {
  /**
   * Connection Pool
   *
   * @type {!sqlite.Database}
   */
  connectionPool!: sqlite.Database;

  /**
   * Handler initialisation
   *
   * @async
   * @returns {Promise<void>}
   */
  async init(): Promise<void> {
    this.connectionPool = await new Promise((res, rej) => {
      let temp = new sqlite.Database(this.config.database, err => {
        if (err) rej(err);
      });
      res(temp);
    });
  }

  /**
   * Get New Connection
   *
   * @async
   * @returns {Promise<sqlite.Database>}
   */
  async getConnection(): Promise<sqlite.Database> {
    return this.connectionPool;
  }

  /**
   * Initialize Transaction
   *
   * @async
   * @param {sqlite.Database} conn
   * @returns {Promise<void>}
   */
  async initTransaction(conn: sqlite.Database): Promise<void> {
    await new Promise((res, rej) => {
      conn.run('BEGIN TRANSACTION', (data: any, err: any) => {
        if (err) rej(new Error(err));
        else res(data);
      });
    });
  }

  /**
   * Commit Transaction
   *
   * @async
   * @param {sqlite.Database} conn
   * @returns {Promise<void>}
   */
  async commit(conn: sqlite.Database): Promise<void> {
    await new Promise((res, rej) => {
      conn.run('COMMIT', (data: any, err: any) => {
        if (err) rej(new Error(err));
        else res(data);
      });
    });
  }

  /**
   * Rollback Transaction
   *
   * @async
   * @param {sqlite.Database} conn
   * @returns {Promise<void>}
   */
  async rollback(conn: sqlite.Database): Promise<void> {
    await new Promise((res, rej) => {
      conn.run('ROLLBACK', (data: any, err: any) => {
        if (err) rej(new Error(err));
        else res(data);
      });
    });
  }

  /**
   * Close Connection
   *
   * @async
   * @param {sqlite.Database} conn
   * @returns {Promise<void>}
   */
  async close(conn: sqlite.Database): Promise<void> {
    // await new Promise<void>((res, rej) => {
    // 	conn.close((err: any) => {
    // 		if (err) rej(err);
    // 		else res();
    // 	});
    // });
  }

  /**
   * Run string query
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?sqlite.Database} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async run(query: string, dataArgs?: any[], connection?: sqlite.Database): Promise<model.ResultSet> {
    let conn = connection ?? this.connectionPool;

    let data: any[] = await new Promise((res, rej) => {
      conn.all(query, dataArgs, function (err, r) {
        if (err) {
          rej(err);
        } else {
          res(r);
        }
      });
    });

    let result = new model.ResultSet();
    result.rows = data;
    result.rowCount = data.length;
    return result;
  }

  /**
   * Run statements
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?sqlite.Database} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  runStatement(queryStmt: sql.Statement | sql.Statement[], connection?: sqlite.Database): Promise<model.ResultSet> {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }

  /**
   * Run quries and stream output
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?sqlite.Database} [connection]
   * @returns {Promise<stream.Readable>}
   */
  async stream(query: string, dataArgs?: any[], connection?: sqlite.Database): Promise<stream.Readable> {
    let conn = connection ?? this.connectionPool;

    let dataStream = new stream.Duplex();
    conn.each(
      query,
      dataArgs,
      (err, row: any) => {
        if (err) throw err;
        dataStream.write(row);
      },
      (err, count) => {
        if (err) throw err;
        dataStream.write(null);
      }
    );
    return dataStream;
  }

  /**
   * Run statements and stream output
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?sqlite.Database} [connection]
   * @returns {Promise<stream.Readable>}
   */
  streamStatement(queryStmt: sql.Statement | sql.Statement[], connection?: sqlite.Database): Promise<stream.Readable> {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
