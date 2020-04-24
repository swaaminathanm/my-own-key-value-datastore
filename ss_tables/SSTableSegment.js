const fs = require('fs');
const path = require('path');

const {FS_OPEN_ERROR, FS_WRITE_ERROR, FILE_FULL_ERROR} = require('../errors');
const {SM_TABLE_MAX_SIZE_IN_BYTES, SM_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES} = require('../config');

class SSTableSegment {
  constructor(basePath) {
    this.basePath = basePath;
    this.fileName = new Date().getTime();
    this.fileExtension = "txt";
    this._position = 0;
    this._index = {};
    this._indexedKeys = [];
    this._indexBucket = -1;
  }

  getFileFullPath() {
    return path.join(this.basePath, `${this.fileName}.${this.fileExtension}`);
  }

  canWrite() {
    return this._position <= SM_TABLE_MAX_SIZE_IN_BYTES;
  }

  get(key) {
    const nearest = SSTableSegment.findNearestKey(this._indexedKeys, key);

    if (nearest.key) {
      return this._getLogFromPositionRange(
        this._index[nearest.key],
        -1,
        key
      );
    } else if (nearest.nearestMinima && !nearest.nearestMaxima) {
      return this._getLogFromPositionRange(
        this._index[nearest.nearestMinima],
        -1,
        key
      );
    } else if (nearest.nearestMinima && nearest.nearestMaxima) {
      return this._getLogFromPositionRange(
        this._index[nearest.nearestMinima],
        this._index[nearest.nearestMaxima],
        key
      );
    }

    return null;
  }

  deleteFile() {
    return new Promise((resolve, reject) => {
      fs.unlink(this.getFileFullPath(), (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      })
    });
  }

  put(key, value) {
    return this._write(`${key}`, value);
  }

  _getIndex() {
    return this._index;
  }

  _getLogFromPositionRange(fromPosition, toPosition, keyToSearch) {
    return new Promise((resolve, reject) => {
      fs.open(this.getFileFullPath(), 'r', (err, fd) => {
        fs.fstat(fd, (err, stats) => {
          if (err) {
            reject(err);
            return;
          }

          if (toPosition < 0) toPosition = stats.size - 1;

          const lengthToBeRead = (toPosition - fromPosition + 1);
          const buffer =  Buffer.alloc(lengthToBeRead);
          const positionToBeReadFrom = fromPosition;

          fs.readSync(fd, buffer, 0, lengthToBeRead, positionToBeReadFrom);

          resolve(this._traverseAndGetLogForKey(buffer.toString(), keyToSearch));
        });
      });
    })
  };

  _traverseAndGetLogForKey(stringChunk, keyToSearch) {
    let i = 0;

    while (i < stringChunk.length) {
      let indexOfFirstDelimiterInGivenChunk = stringChunk.indexOf(":", i);

      if (indexOfFirstDelimiterInGivenChunk < 0) return null;

      let bytesToCountInString = stringChunk.substring(i, indexOfFirstDelimiterInGivenChunk);

      const bytesToCount = parseInt(bytesToCountInString);

      if (isNaN(bytesToCount)) {
        return null;
      }

      i = indexOfFirstDelimiterInGivenChunk;
      i++;

      const log = stringChunk.substring(i, (i + bytesToCount));
      const {key, value} = this._parseLogToGetKeyAndValue(log);

      if (key === keyToSearch) {
        return {
          key,
          value
        }
      }

      i += bytesToCount;
    }

    return null;
  }

  _parseLogToGetKeyAndValue(log) {
    const str = log.split(":");
    return {
      key: str[0],
      value: str[1]
    }
  }

  _shouldStoreLogInIndex() {
    const indexBucket = Math.floor(this._position/SM_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES);
    if (indexBucket !== this._indexBucket) {
      this._indexBucket = indexBucket;
      return true;
    }

    return false;
  }

  _storeLogInIndex(key, position) {
    if (this._shouldStoreLogInIndex()) {
      this._index[key] = position;
      this._indexedKeys.push(key);
    }
  }

  /*
  Steps:
    1. Encode string as <length>:<key>:<value>
    2. Note down the starting byte offset
    3. Write to a file
    4. Store starting offset to index
  */
  _write(key, value) {
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
        } else {
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

  static findNearestKey(arr, key) {
    let leftIndex = 0;
    let rightIndex = arr.length - 1;
    let wasKeyFound = false;
    let foundKey;
    let nearestLowerIndex;
    let nearestHigherIndex;
    let tempLowestDifference = Number.MAX_SAFE_INTEGER;
    let tempHighestDifference = Number.MAX_SAFE_INTEGER;

    while (rightIndex >= leftIndex) {
      const pivotIndex = leftIndex + Math.floor((rightIndex - leftIndex) / 2);
      const pivotElement = arr[pivotIndex];
      const diff = Math.abs(pivotElement - key);

      if (pivotElement ===  key) {
        foundKey = key;
        wasKeyFound = true;
        break;
      } else if (pivotElement > key) {
        rightIndex = pivotIndex - 1;
        if (diff < tempHighestDifference) {
          nearestHigherIndex = pivotIndex;
          tempHighestDifference = diff;
        }
      } else {
        leftIndex = pivotIndex + 1;
        if (diff < tempLowestDifference) {
          nearestLowerIndex = pivotIndex;
          tempLowestDifference = diff;
        }
      }
    }

    const result = {};

    if (wasKeyFound) {
      result.key = key;
    } else {
      result.nearestMinima = arr[nearestLowerIndex];
      result.nearestMaxima = arr[nearestHigherIndex];
    }

    return result;
  }
}

module.exports = SSTableSegment;