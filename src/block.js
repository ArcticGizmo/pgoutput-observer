class Block {
  constructor() {
    this._entries = [];
    this._complete = false;
    this.startLsn;
    this.endLsn;
    this.xid;
  }

  get complete() {
    return this._complete;
  }

  add(entry) {
    if (this._complete) {
      throw 'Block already complete';
    }

    this._entries.push(entry);

    switch (entry.kind) {
      case 'BEGIN':
        this._addBegin(entry);
        break;
      case 'COMMIT':
        this._addCommit(entry);
        break;
      default:
        this._add(entry);
    }
  }

  _addBegin(entry) {
    this.xid = entry.xid;
    this.startLsn = entry.lsn;
    this.endLsn = entry.final_lsn;
  }

  _addCommit() {
    this._complete = true;
  }

  _add(entry) {}
}

module.exports = Block;
