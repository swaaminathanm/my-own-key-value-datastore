const SSTableSegment = require('./ss_tables/SSTableSegment');
const Compaction = require('./Compaction');
const RedBlackTree = require('./RedBlackTree');
const {hashCode} = require('./utils');
const {
  TOTAL_MEMTABLE_NODES_ACCEPTABLE,
  SM_TABLE_MAX_SIZE_IN_BYTES,
  SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES,
  COMPACTION_THRESHOLD
} = require('./config');
const {INTERNAL_ERROR} = require('./errors');

class LSM {
  constructor(
    dirPathToCreateSSTables
  ) {
    this._memTableStack = [new RedBlackTree(TOTAL_MEMTABLE_NODES_ACCEPTABLE)];
    this._canDoCompaction = true;
    this._canDoMemtableDrain = true;
    this._compactionBucket = [];
    this._ssTablesBucket = [];
    this._dirPathToCreateSSTables = dirPathToCreateSSTables;

    this._drainMemTable = this._drainMemTable.bind(this);
    this._insertIntoMemTable = this._insertIntoMemTable.bind(this);
    this._doCompaction = this._doCompaction.bind(this);

    this._updateMemtableIntervalId = setInterval(this._drainMemTable, 1000);
    this._doCompactionIntervalId = setInterval(this._doCompaction, 10000);
  }

  async _doCompaction() {
    if (!this._canDoCompaction || this._ssTablesBucket.length < COMPACTION_THRESHOLD) {
      return;
    }

    this._canDoCompaction = false;

    // pop out the initially added files into _compactionBucket
    for (let i=0; i<COMPACTION_THRESHOLD; i++) {
      this._compactionBucket.push(this._ssTablesBucket.shift());
    }

    const compaction = new Compaction(
      this._compactionBucket,
      this._dirPathToCreateSSTables,
      SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES
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

    // remove the old memtable that should be drained
    const memTable = this._memTableStack.shift();

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
      SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES,
      SM_TABLE_MAX_SIZE_IN_BYTES
    );

    // traverse memtable and put into sstable segment file in ascending order
    memTable.traverse(async (log) => {
      await ssTableSegment.put(log.key, log.value);
    });

    this._ssTablesBucket.push(ssTableSegment);

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

  async _getFromMemTable(key) {
    const keyHash = hashCode(key);
    let value;
    for (let i=0; i<this._memTableStack.length; i++) {
      value = this._memTableStack[i].get(keyHash);
      if (value) break;
    }
    return value;
  }

  async _getFromSSTableBucket(bucket) {
    let value;
    for (let i=0; i<bucket.length; i++) {
      const ssTableSegment = bucket[i];
      value = await ssTableSegment.get(key);
      if (value) break;
    }
    return value;
  }

  async get(key) {
    let value = await this._getFromMemTable(key);

    if (!value) {
      value = await this._getFromSSTableBucket(this._ssTablesBucket);
    }
    if (!value) {
      value = await this._getFromSSTableBucket(this._compactionBucket);
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

  close() {
    clearInterval(this._updateMemtableIntervalId);
    clearInterval(this._doCompactionIntervalId);
  }
}

module.exports = LSM;