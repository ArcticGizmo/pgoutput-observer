const { Client, types } = require('pg');
const pipeline = require('util').promisify(require('stream').pipeline);
const SubscriptionStream = require('./subscription_stream');
const { PgOutputParser } = require('pg-subscription-stream');
const { Writable } = require('stream');
const Block = require('./block');

class Listener {
  constructor(options) {
    const opts = options || {};
    this._client = new Client({
      connectionString: opts.connectionString,
      replication: opts.replication || 'database',
    });
    this._slot = opts.slot;
    this._publication = opts.publication;
  }

  async listen(callback) {
    this._callback = callback;
    await this._connect();
  }

  async _connect() {
    await this._client.connect();
    this._log('Connected');
    this._createPipeline();
  }

  async _createPipeline() {
    const subStream = this._client.query(
      new SubscriptionStream({
        slotName: this._slot,
        pluginOptions: {
          proto_version: 1,
          publication_names: this._publication,
        },
      }),
    );

    const parser = new PgOutputParser({
      typeParsers: types,
      includeLsn: true,
      includeTransactionLsn: true,
      includeXids: true,
      includeTimestamp: true,
    });

    if (typeof this._callback !== 'function') {
      console.error('[Listener] no callback provided, cannot run');
      throw 'No callback provided';
    }

    let curBlock = new Block();

    const ack = lsn => subStream.ack(lsn);

    await pipeline(
      subStream,
      parser,
      new Writable({
        objectMode: true,
        write: (chunk, encoding, cb) => {
          curBlock.add(chunk);

          // we are still reading in the entries
          if (!curBlock.complete) {
            cb();
            return;
          }

          const next = () => {
            curBlock = new Block();
            cb();
          };

          this._callback(curBlock, next, ack);
        },
      }),
    );
  }

  _log(msg) {
    console.log('[Listener] ' + msg);
  }
}

module.exports = Listener;
