const SSTableSegment = require('./ss_tables/SSTableSegment');
const Compaction = require('./Compaction');

class LSM {
  constructor(
    dirPathToCreateSSTables,
    logger,
    memTable,
    SM_TABLE_MAX_SIZE_IN_BYTES,
    SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES,
    COMPACTION_THRESHOLD
  ) {
    this._logger = logger;
    this._memTable = memTable;
    this._canDoCompaction = true;
    this._compactionBucket = [];
    this._ssTablesBucket = [];
    this._dirPathToCreateSSTables = dirPathToCreateSSTables;
    this.SM_TABLE_MAX_SIZE_IN_BYTES = SM_TABLE_MAX_SIZE_IN_BYTES;
    this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES = SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES;
    this.COMPACTION_THRESHOLD = COMPACTION_THRESHOLD;

    this._updateMemTable = this._updateMemTable.bind(this);
    this._insertIntoMemTable = this._insertIntoMemTable.bind(this);
    this._doCompaction = this._doCompaction.bind(this);

    this._updateMemtableIntervalId = setInterval(this._updateMemTable, 1000);
    this._doCompactionIntervalId = setInterval(this._doCompaction, 10000);
  }

  async _doCompaction() {
    if (!this._canDoCompaction && this._ssTablesBucket.length < this.COMPACTION_THRESHOLD) {
      return;
    }

    this._canDoCompaction = false;

    // pop out the initially added files into _compactionBucket
    for (let i=0; i<this.COMPACTION_THRESHOLD; i++) {
      this._compactionBucket.push(this._ssTablesBucket.shift());
    }

    const compaction = new Compaction(
      this._compactionBucket,
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

  async _updateMemTable() {
    if (this._logger.isEmpty()) {
      return;
    }

    const logs = this._logger.drain();
    logs.forEach(this._insertIntoMemTable);

    while(!this._memTable.canAccept) {
      const toRemove = this._memTable.removeLowest();

      if (toRemove.key && toRemove.value) {
        let lastSSTableSegment = this._ssTablesBucket[this._ssTablesBucket.length - 1];

        if (!lastSSTableSegment || !lastSSTableSegment.canWrite()) {
          const ssTableSegment = new SSTableSegment(
            this._dirPathToCreateSSTables,
            this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES,
            this.SM_TABLE_MAX_SIZE_IN_BYTES
          );
          this._ssTablesBucket.push(ssTableSegment);
          lastSSTableSegment = ssTableSegment;
        }

        await lastSSTableSegment.put(toRemove.key, toRemove.value);
      }
    }
  }

  _insertIntoMemTable(log) {
    const logJson = JSON.parse(log);
    const key = logJson.key;
    const value = logJson.data;

    this._memTable.insert(key, value);
  }

  async get(key) {
    // Search in memtable
    const value = this._memTable.get(key);
    if (value) return value;

    // Search in _ssTablesBucket
    for (let i=0; i<this._ssTablesBucket.length; i++) {
      const result = await this._ssTablesBucket[i].get(key);
      if (result.value) return result.value;
    }

    // Search in _compactionBucket
    for (let i=0; i<this._compactionBucket.length; i++) {
      const result = await this._compactionBucket[i].get(key);
      if (result.value) return result.value;
    }

    return null;
  }

  put(key, value) {
    const log = {
      key,
      data: value
    };

    this._logger.addToLog(JSON.stringify(log));
  }

  close() {
    clearInterval(this._updateMemtableIntervalId);
    clearInterval(this._doCompactionIntervalId);
  }
}

module.exports = LSM;