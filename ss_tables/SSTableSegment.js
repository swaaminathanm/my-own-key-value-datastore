const fs = require('fs');
const path = require('path');
const uuid = require('uuid');

const uuidv4 = uuid.v4;

const {FS_OPEN_ERROR, FS_WRITE_ERROR, ONLY_NUMERIC_KEYS_ACCEPTED, FS_CLOSE_ERROR} = require('../errors');

/**
 * Criteria:
 * + Keys should be passed in ascending order (should not call put with random ordering of keys)
 * + Only numeric keys are allowed
 */
class SSTableSegment {
  constructor(
    basePath,
    SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES
  ) {
    this.basePath = basePath;
    this.fileName = uuidv4();
    this.fileExtension = "txt";
    this._position = 0;
    this._index = {};
    this._indexedKeys = [];
    this._indexBucket = -1;
    this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES = SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES;
  }

  toString() {
    return this.fileName;
  }

  getFileFullPath() {
    return path.join(this.basePath, `${this.fileName}.${this.fileExtension}`);
  }

  get(key) {
    return new Promise( async (resolve) => {
      const nearest = SSTableSegment._findNearestKey(this._indexedKeys, key);
      let value = null;

      if (nearest.key === 0 || nearest.key) {
        const result = await this.readKeyValueFromPosition(this._index[nearest.key]);

        value = result.value;
      } else if (nearest.nearestMinima && !nearest.nearestMaxima) {
        let position = this._index[nearest.nearestMinima];
        while (position >= 0) {
          const result  = await this.readKeyValueFromPosition(position);

          if (key == result.key) {
            value = result.value;
            position = -1;
          } else {
            position = result.cursor.nextPosition;
          }
        }
      } else if (nearest.nearestMinima && nearest.nearestMaxima) {
        let position = this._index[nearest.nearestMinima];
        while (position >= 0) {
          const result  = await this.readKeyValueFromPosition(position);

          if (key == result.key) {
            value = result.value;
            position = -1;
          } else {
            position = result.cursor.nextPosition;
          }

          if (position >= this._index[nearest.nearestMaxima]) {
            position = -1;
          }
        }
      }

      resolve(value);
    });
  }

  deleteFile() {
    return new Promise((resolve, reject) => {
      fs.unlink(this.getFileFullPath(), (err) => {
        if (err) {
          console.error("=========", err);
          reject(err);
        } else {
          resolve();
        }
      })
    });
  }

  put(key, value) {
    if (!isNaN(key)) {
      return this._write(key, value);
    }

    throw {
      code: ONLY_NUMERIC_KEYS_ACCEPTED,
      error: {
        message: 'Only numeric keys are accepted'
      }
    };
  }

  readKeyValueFromPosition(position) {
    return new Promise( async (resolve, reject) => {
      fs.open(this.getFileFullPath(), 'r', (err, fd) => {
        if (err) {
          reject({
            code: FS_OPEN_ERROR,
            error: err
          });
          return;
        }

        fs.fstat(fd, async (err, stats) => {
          if (err) {
            reject(err);
          } else {
            const result = this._readKeyValueFromPosition(position, fd, stats.size);
            fs.close(fd, (err) => {
              if (err) {
                reject({
                  code: FS_CLOSE_ERROR,
                  error: err
                });
              }
              else {
                resolve(result);
              }
            });
          }
        });
      });
    });
  }

  /**
   * This function reads a log from the given position in the SSTableSegment file
   * @param position - The position of the log to read
   * @param fileDescriptor
   * @param totalLengthOfContent - The total length of file content in bytes
   * returns - object with key, value and a cursor which contains the next key's position if present
   */
  _readKeyValueFromPosition(position, fileDescriptor, totalLengthOfContent) {
    return new Promise((resolve) => {
      const result = {
        key: null,
        value: null,
        cursor: {
          nextPosition: -1
        }
      };

      // The maximum length of a key:value pair will definitely be smaller than the SM_TABLE_MAX_SIZE_IN_BYTES
      const lengthToRead = `${this.SM_TABLE_MAX_SIZE_IN_BYTES}`.length * 2;

      const buffer = Buffer.alloc(lengthToRead);

      fs.readSync(fileDescriptor, buffer, 0, lengthToRead, position);

      const logChunk = buffer.toString();
      const indexOfFirstDelimiterInGivenChunk = logChunk.indexOf(":");

      if (indexOfFirstDelimiterInGivenChunk > -1) {
        const lengthOfChunkTillFirstDelimiter = indexOfFirstDelimiterInGivenChunk + 1;
        let lengthOfKeyValueInString = logChunk.substring(0, indexOfFirstDelimiterInGivenChunk);
        const lengthOfKeyValue = parseInt(lengthOfKeyValueInString);

        if (!isNaN(lengthOfKeyValue)) {
          const lengthOfLog = lengthOfKeyValue + lengthOfChunkTillFirstDelimiter;
          const logBuffer = Buffer.alloc(lengthOfLog);
          fs.readSync(fileDescriptor, logBuffer, 0, lengthOfLog, position);

          const logString = logBuffer.toString();
          const [_, key, value] = logString.split(":");

          const nextPosition = position + lengthOfLog;

          result.key = key;
          result.value = value;
          result.cursor.nextPosition = (nextPosition >= totalLengthOfContent) ? -1 : nextPosition;
        }
      }

      resolve(result);
    })
  }

  _getIndex() {
    return this._index;
  }

  _shouldStoreLogInIndex() {
    const indexBucket = Math.floor(this._position/this.SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES);
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
                fs.close(fd, (err) => {
                  if (err) {
                    reject({
                      code: FS_CLOSE_ERROR,
                      error: err
                    });
                  } else {
                    resolve();
                  }
                })
              }
            });
        }
      });
    });
  }

  static _findNearestKey(arr, key) {
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