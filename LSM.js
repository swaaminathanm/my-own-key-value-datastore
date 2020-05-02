const { List } = require('immutable');

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
    this._canDoCompactionFlag = true;
    this._canDoMemtableDrainFlag = true;
    this._ssTablesListBucket = []; // latest snapshot of the list will be in the end of this array
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

  _canDoCompaction() {
    const ssTableList = this._ssTablesListBucket[this._ssTablesListBucket.length - 1];
    return this._canDoCompactionFlag && ssTableList && ssTableList.size >= this.COMPACTION_THRESHOLD;
  }

  async _doCompaction() {
    if (this._canDoCompaction()) {
      this._canDoCompactionFlag = false;

      const tempCompactionBucket = this._removeSSTablesFromListInBucket(this.COMPACTION_THRESHOLD);

      if (tempCompactionBucket) {
        tempCompactionBucket.reverse();

        const compaction = new Compaction(
          tempCompactionBucket,
          this._dirPathToCreateSSTables,
          this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES
        );

        const newSSTableSegment = await compaction.doCompaction();

        this._addSSTableToListInBucket(newSSTableSegment);
      }
    }

    this._canDoCompactionFlag = true;
  }

  _canDoMemTableDrain() {
    return this._canDoMemtableDrainFlag && this._memTableStack.length > 0 && !this._memTableStack[0].isEmpty();
  }

  async _drainMemTable() {
    if (this._canDoMemTableDrain()) {
      this._canDoMemtableDrainFlag = false;

      this._memTableStack.push(new RedBlackTree());

      const memTable = this._memTableStack[0];

      const ssTableSegment = new SSTableSegment(
        this._dirPathToCreateSSTables,
        this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES
      );

      const values = memTable.getValues();

      for (let i=0; i<values.length; i++) {
        const {key, value} = values[i];
        await ssTableSegment.put(key, value);
      }

      this._addSSTableToListInBucket(ssTableSegment, true);

      this._memTableStack.shift();
    }

    this._canDoMemtableDrainFlag = true;
  }

  _addSSTableToListInBucket(ssTableFragment, addToLastInList = false) {
    let ssTableList = this._ssTablesListBucket[this._ssTablesListBucket.length - 1];
    if (!ssTableList) {
      ssTableList = List();
    }


    if (addToLastInList) {
      this._ssTablesListBucket.push(ssTableList.push(ssTableFragment));
    } else {
      this._ssTablesListBucket.push(ssTableList.unshift(ssTableFragment));
    }
  }

  _removeSSTablesFromListInBucket(countToPop) {
    let ssTableList = this._ssTablesListBucket[this._ssTablesListBucket.length - 1];
    if (ssTableList && ssTableList.size >= countToPop) {
      const ssTablesToReturn = [];
      let updatesSSTableList = ssTableList;
      for(let i=0; i<countToPop; i++) {
        ssTablesToReturn.push(ssTableList.get(i));
        updatesSSTableList = updatesSSTableList.shift();
      }
      this._ssTablesListBucket.push(updatesSSTableList);
      return ssTablesToReturn;
    }
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

  _getFromMemTable(keyHash) {
    return this._memTableStack.reduce((result,_ , index) => {
      if (result === null) {
        return this._memTableStack[this._memTableStack.length - index - 1].get(keyHash)
      } else {
        return result;
      }
    }, null);
  }

  _getFromSSTableList(ssTableList, keyHash) {
    return ssTableList.reduce((promise, _, index) => {
      const ssTableSegment = ssTableList.get(ssTableList.size - index - 1); // iterate in reverse
      return promise.then((value) => {
        if (value === null) return ssTableSegment.get(keyHash);
        else return value;
      });
    }, Promise.resolve(null));
  }

  async _getFromSSTableListBucket(keyHash) {
    let value = null;
    let index = this._ssTablesListBucket.length - 1;

    while (index >= 0) {
      const ssTableList = this._ssTablesListBucket[index];

      if (ssTableList) {
        value = await this._getFromSSTableList(ssTableList, keyHash);
        if (value !== null) break;
      }

      index--;
    }

    return value;
  }

  async get(key) {
    const keyHash = hashCode(key);

    let value = await this._getFromMemTable(keyHash);

    if (value === null) {
      value = await this._getFromSSTableListBucket(keyHash);
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