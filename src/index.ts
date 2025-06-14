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
   * Creates an instance of Mysql.
   *
   * @constructor
   * @param {string} config
   */
  constructor(config: string) {
    super(config);

    this.connectionPool = new sqlite.Database(config, err => {
      if (err) throw err;
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
      conn.run('BEGIN TRANSACTION', (data: unknown, err: Error | null) => {
        if (err) rej(err);
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
      conn.run('COMMIT', (data: unknown, err: Error | null) => {
        if (err) rej(err);
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
      conn.run('ROLLBACK', (data: unknown, err: Error | null) => {
        if (err) rej(err);
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
  async close(): Promise<void> {
    // conn: sqlite.Database
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
   * @param {?unknown[]} [dataArgs]
   * @param {?sqlite.Database} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async run(query: string, dataArgs?: unknown[], connection?: sqlite.Database): Promise<model.ResultSet> {
    const conn = connection ?? this.connectionPool;

    const data: Record<string, unknown>[] = await new Promise((res, rej) => {
      conn.all(query, dataArgs, function (err, r) {
        if (err) {
          rej(err);
        } else {
          res(r as Record<string, unknown>[]);
        }
      });
    });

    const result = new model.ResultSet();
    result.rows = data;
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
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }

  /**
   * Run quries and stream output
   *
   * @async
   * @param {string} query
   * @param {?unknown[]} [dataArgs]
   * @param {?sqlite.Database} [connection]
   * @returns {Promise<stream.Readable>}
   */
  async stream(query: string, dataArgs?: unknown[], connection?: sqlite.Database): Promise<stream.Readable> {
    const conn = connection ?? this.connectionPool;

    const dataStream = new stream.Duplex();
    conn.each(
      query,
      dataArgs,
      (err, row: unknown) => {
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

  /**
   * Run statements and stream output
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?sqlite.Database} [connection]
   * @returns {Promise<stream.Readable>}
   */
  streamStatement(queryStmt: sql.Statement | sql.Statement[], connection?: sqlite.Database): Promise<stream.Readable> {
    const { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
