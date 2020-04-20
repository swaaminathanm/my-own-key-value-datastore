const fs = require('fs');
const path = require('path');

const {FS_OPEN_ERROR, FS_WRITE_ERROR, FILE_FULL_ERROR} = require('../errors');
const {SM_TABLE_MAX_SIZE_IN_BYTES, SM_TABLE_IN_MEMORY_SPARSE_KEYS_TRESHOLD_BYTES} = require('../config');

class SSTableSegment {
  constructor(basePath) {
    this.basePath = basePath;
    this.fileName = new Date().getTime();
    this.fileExtension = "txt";
    this._position = 0;
    this._index = {};
    this._indexBucket = -1;
  }

  getIndex() {
    return this._index;
  }

  getFileFullPath() {
    return path.join(this.basePath, `${this.fileName}.${this.fileExtension}`);
  }

  canWrite() {
    return this._position <= SM_TABLE_MAX_SIZE_IN_BYTES;
  }

  _shouldStoreLogInIndex() {
    const indexBucket = Math.floor(this._position/SM_TABLE_IN_MEMORY_SPARSE_KEYS_TRESHOLD_BYTES);
    if (indexBucket !== this._indexBucket) {
      this._indexBucket = indexBucket;
      return true;
    }

    return false;
  }

  _storeLogInIndex(key, position) {
    if (this._shouldStoreLogInIndex()) {
      this._index[key] = position;
    }
  }

  /*
  Steps:
    1. Encode string as <length>:<key>:<value>
    2. Note down the starting byte offset
    3. Write to a file
    4. Store starting offset to index
  */
  write(key, value) {
    return new Promise((resolve, reject) => {
      if (!this.canWrite()) {
        reject({
          code: FILE_FULL_ERROR
        });
      }

      const logLength = `${key}:${value}`.length;
      const logToWrite = `${logLength}:${key}:${value}`;
      const bufferToWrite = Buffer.from(logToWrite);
      const filePath = this.getFileFullPath();

      fs.open(filePath, 'a', (err, fd) => {
        if(err) {
          reject({
            code: FS_OPEN_ERROR,
            error: err
          });
        }else {
          fs.write(
            fd,
            bufferToWrite,
            0,
            bufferToWrite.length,
            this._position,
            (err,_) => {
              if(err) {
                reject({
                  code: FS_WRITE_ERROR,
                  error: err
                });
              } else {
                this._storeLogInIndex(key, this._position);
                this._position += bufferToWrite.length;
                resolve();
              }
            });
        }
      });
    });
  }
}

module.exports = SSTableSegment;