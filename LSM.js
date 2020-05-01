const SSTableSegment = require('./ss_tables/SSTableSegment');
const Compaction = require('./Compaction');
const RedBlackTree = require('./RedBlackTree');
const {hashCode} = require('./utils');
const {INTERNAL_ERROR} = require('./errors');

class LSM {
  constructor(
    dirPathToCreateSSTables,
    SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES,
    COMPACTION_THRESHOLD,
    DRAIN_MEM_TABLE_INTERVAL,
    COMPACTION_INTERVAL
  ) {
    this._memTableStack = [new RedBlackTree()];
    this._canDoCompaction = true;
    this._canDoMemtableDrain = true;
    this._compactionBucket = [];
    this._ssTablesBucket = [];
    this._dirPathToCreateSSTables = dirPathToCreateSSTables;
    this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES =   SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES;
    this.COMPACTION_THRESHOLD = COMPACTION_THRESHOLD;
    this.DRAIN_MEM_TABLE_INTERVAL = DRAIN_MEM_TABLE_INTERVAL;
    this.COMPACTION_INTERVAL = COMPACTION_INTERVAL;

    this._drainMemTable = this._drainMemTable.bind(this);
    this._insertIntoMemTable = this._insertIntoMemTable.bind(this);
    this._doCompaction = this._doCompaction.bind(this);

    if (this.DRAIN_MEM_TABLE_INTERVAL > 0) {
      this._drainMemtableIntervalId = setInterval(this._drainMemTable, this.DRAIN_MEM_TABLE_INTERVAL);
    }

    if (this.COMPACTION_INTERVAL > 0) {
      this._doCompactionIntervalId = setInterval(this._doCompaction, this.COMPACTION_INTERVAL);
    }
  }

  async _doCompaction() {
    if (!this._canDoCompaction || this._ssTablesBucket.length < this.COMPACTION_THRESHOLD) {
      return;
    }

    this._canDoCompaction = false;

    // pop out the initially added files into _compactionBucket
    const tempCompactionBucket = [];
    for (let i=0; i<this.COMPACTION_THRESHOLD; i++) {
      tempCompactionBucket.push(this._ssTablesBucket.shift());
    }
    tempCompactionBucket.forEach(bucket => this._compactionBucket.push(bucket));
    tempCompactionBucket.reverse();

    const compaction = new Compaction(
      tempCompactionBucket,
      this._dirPathToCreateSSTables,
      this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES
    );

    const newSSTableSegment = await compaction.doCompaction();

    this._ssTablesBucket.unshift(newSSTableSegment);

    while (this._compactionBucket.length > 0) {
      await this._compactionBucket.pop().deleteFile();
    }

    this._canDoCompaction = true;
  }

  async _drainMemTable() {
    if (!this._canDoMemtableDrain) {
      return;
    }

    // Since memtable drain is happening we cannot accept another drain request until this finishes
    this._canDoMemtableDrain = false;

    // push new memtable to stack so that new logs are stored in this memtable
    this._memTableStack.push(new RedBlackTree());

    // get the old memtable that should be drained
    const memTable = this._memTableStack[0];

    if (!memTable) {
      throw {
        code: INTERNAL_ERROR,
        error: {
          message: 'Memtable stack empty'
        }
      };
    }

    const ssTableSegment = new SSTableSegment(
      this._dirPathToCreateSSTables,
      this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES
    );

    // traverse memtable and put into sstable segment file in ascending order
    const values = memTable.getValues();

    for (let i=0; i<values.length; i++) {
      const {key, value} = values[i];
      await ssTableSegment.put(key, value);
    }

    this._ssTablesBucket.push(ssTableSegment);

    // remove the old memtable
    this._memTableStack.shift();

    // reset flag to allow memtable to drain
    this._canDoMemtableDrain = true;
  }

  _insertIntoMemTable(log) {
    if (this._memTableStack.length <= 0) {
      throw {
        code: INTERNAL_ERROR,
        error: {
          message: 'Memtable stack empty'
        }
      };
    }

    const key = log.key;
    const value = log.data;

    this._memTableStack[this._memTableStack.length - 1].insert(key, value);
  }

  async _getFromMemTable(keyHash) {
    let memTableIndex = this._memTableStack.length - 1;
    let value;

    while (memTableIndex >= 0) {
      value = this._memTableStack[memTableIndex].get(keyHash);
      if (value !== undefined) break;

      memTableIndex--;
    }

    return value;
  }

  async _getFromSSTableBucket(bucket, keyHash) {
    let index = bucket.length - 1;
    let value;
    while (index >= 0) {
      value = await bucket[index].get(keyHash);
      if (value) break;

      index--;
    }
    return value;
  }

  async get(key) {
    const keyHash = hashCode(key);

    let value = await this._getFromMemTable(keyHash);

    if (value === undefined) {
      value = await this._getFromSSTableBucket(this._ssTablesBucket, keyHash);
    }
    if (value === undefined) {
      value = await this._getFromSSTableBucket(this._compactionBucket, keyHash);
    }

    return value;
  }

  put(key, value) {
    const log = {
      key: hashCode(key),
      data: value
    };

    this._insertIntoMemTable(log);
  }

  stop() {
    clearInterval(this._drainMemtableIntervalId);
    clearInterval(this._doCompactionIntervalId);
  }
}

module.exports = LSM;