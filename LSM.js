class LSM {
  constructor(logger, memTable) {
    this._logger = logger;
    this._memTable = memTable;

    this._updateMemTable = this._updateMemTable.bind(this);
    this._insertIntoMemTable = this._insertIntoMemTable.bind(this);

    setInterval(this._updateMemTable, 1000);
  }

  _updateMemTable() {
    if (this._logger.isEmpty()) {
      return;
    }

    if (this._memTable.isAvailable()) {
      const logs = this._logger.drain();

      logs.forEach(this._insertIntoMemTable);
    }
  }

  _insertIntoMemTable(log) {
    const logJson = JSON.parse(log);
    const key = logJson.key;
    const value = logJson.data;

    this._memTable.insert(key, value);
  }

  put(key, value) {
    const log = {
      key,
      data: value
    };

    this._logger.addToLog(JSON.stringify(log));
  }
}

module.exports = LSM;