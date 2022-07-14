const { Transform } = require('stream');
const { both } = require('pg-copy-streams');

const FEEDBACK_INTERVAL = 5000;
const INVALID_LSN = 0n;
const NOOP = () => null;

const now = () => BigInt(Date.now()) - 946684800000n;

function fromBigInt(maybeBigInt) {
  if (typeof maybeBigInt === 'bigint') {
    return maybeBigInt.toString(16).padStart(9, '0').replace(/.{8}$/, '/$&');
  }
  return maybeBigInt;
}

function createStartQuery(slotName, lsn, pluginOptions) {
  const optsAsArgs = Object.entries(pluginOptions)
    .map(([k, v]) => `"${k}" '${v}'`)
    .join(',');

  return `START_REPLICATION SLOT ${slotName} LOGICAL ${lsn} (${optsAsArgs})`;
}

function log(msg) {
  console.log('[SubStream] ' + msg);
}

class SubscriptionStream extends Transform {
  constructor(options) {
    super();
    this.options = options || {};

    this.lastLsn = INVALID_LSN;
    this.lastPing = now();

    const {
      slotName,
      feedbackInterval = FEEDBACK_INTERVAL,
      startPos = 0n,
      pluginOptions = {
        proto_version: 1,
        publication_names: slotName,
      },
    } = this.options;

    this.feedbackInterval = feedbackInterval;

    const lsn = fromBigInt(startPos);
    const startQuery = createStartQuery(slotName, lsn, pluginOptions);

    this.copyBoth = new both(startQuery, { alignOnCopyDataFrame: true });
    this.copyBoth.pipe(this);
    this.copyBoth.on('error', e => this.emit('error', e));

    this.interval = setInterval(() => {
      this.ping();
    }, this.feedbackInterval);

    this.on('end', () => {
      clearInterval(this.interval);
      this.copyBoth.end();
    });

    this.disableLog();
  }

  _log(msg) {
    log(msg);
  }

  enableLog() {
    this._log = log;
  }

  disableLog() {
    this._log = NOOP;
  }

  ping() {
    const delta = now() - this.lastPing;

    if (delta > this.feedbackInterval) {
      this._log('[SubStream] Sending ping');
      this._sendStatus(this.lastLsn);
    }
  }

  ack(endLsn) {
    if (endLsn <= this.lastLsn) {
      return;
    }
    this._log('[SubStream] Sending ack');
    this._sendStatus(endLsn + 1n);
  }

  _sendStatus(endLsn) {
    const currentTime = now();
    this.lastPing = currentTime;

    const resp = new DataView(new ArrayBuffer(1 + 8 + 8 + 8 + 8 + 1));
    resp.setUint8(0, 'r'.charCodeAt(0));
    resp.setBigUint64(1, endLsn);
    resp.setBigUint64(1 + 8, endLsn);
    resp.setBigUint64(1 + 8 + 8, INVALID_LSN);
    resp.setBigUint64(1 + 8 + 8 + 8, currentTime);
    resp.setUint8(1 + 8 + 8 + 8 + 8, 0);
    this.copyBoth.write(Buffer.from(resp.buffer));

    this.lastLsn = endLsn;
  }

  _transform(chunk, encoding, callback) {
    const { autoConfirmLSN = true } = this.options;
    const [header] = chunk;
    if (header === 0x77) {
      this.push(chunk);
    } else if (header === 0x6b) {
      // const lsn = chunk.readBigUInt64BE(1);
      // const shouldRespond = chunk.readInt8(1 + 8 + 8);
      // this.outputWrittenLsn = this.outputWrittenLsn > lsn ? this.outputWrittenLsn : lsn;
      // if (autoConfirmLSN || this.flushWrittenLsn === INVALID_LSN) {
      //   this.flushWrittenLsn = this.outputWrittenLsn;
      // }
      // this.sendFeedback(shouldRespond > 0);
    } else {
      callback(new Error(`Unknown Message: ${chunk}`));
      return;
    }
    process.nextTick(callback);
  }

  submit(connection) {
    this.copyBoth.submit(connection);
  }

  confirmLSN(lsn) {
    if (lsn > this.flushWrittenLsn) {
      this.flushWrittenLsn = lsn;
    }
  }

  handleError(e) {
    this.copyBoth.handleError(e);
  }

  handleCopyData(chunk) {
    this.copyBoth.handleCopyData(chunk);
  }

  handleCommandComplete() {
    this.copyBoth.handleCommandComplete();
  }

  handleReadyForQuery() {
    this.copyBoth.handleReadyForQuery();
  }
}

module.exports = SubscriptionStream;
