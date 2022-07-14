const { Client, types } = require('pg');
const pipeline = require('util').promisify(require('stream').pipeline);
const SubscriptionStream = require('./subscription_stream');
const { Writable } = require('stream');

class Listener {
  constructor(options) {
    const opts = options || {};
    this._client = new Client({
      connectionString: opts.connectionString,
      replication: opts.replication || 'database',
    });
    this._slot = opts.slot;
    this._publication = opts.publication;

    this._connect();
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

    await pipeline(
      subStream,
      // parser,
      new Writable({
        objectMode: true,
        write: (chunk, encoding, cb) => {
          // const { kind, schema, table, KEY, OLD, NEW } = chunk;
          // Write to your desintation, do your stuff...
          console.log('-------------');
          console.log(chunk);
          cb();
        },
      }),
    );
  }

  _log(msg) {
    console.log('[Listener] ' + msg);
  }
}

module.exports = Listener;
