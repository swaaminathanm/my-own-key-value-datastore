class Logger {
  constructor() {
    this._queue = [];
  }

  addToLog(string) {
    this._queue.push(string);
  }

  isEmpty() {
    return this._queue.length === 0;
  }

  drain() {
    const queueCopy = this._queue;
    this._queue = [];
    return queueCopy;
  }
}

module.exports = Logger;