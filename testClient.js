const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");
const assert = require('assert').strict;

const LSM = require('./LSM');
const Logger = require('./Logger');
const MemTable = require('./RedBlackTree');

const basePath = path.join(__dirname, 'example');
const logger = new Logger();
const memTable = new MemTable(5);

if (!fs.existsSync(basePath)) {
  fs.mkdirSync(basePath);
}

const shouldGetDataFromSMTableFile = async () => {
  return new Promise((resolve) => {
    const lsm = new LSM(
      basePath,
      logger,
      memTable,
      5,
      2,
      2
    );

    lsm.put(1, 1);
    lsm.put(2, 2);
    lsm.put(3, 3);
    lsm.put(4, 4);

    setTimeout(async () => {
      const value = await lsm.get(4);
      assert.equal(value, '4');
      lsm.close();
      resolve();
    }, 2000);
  })
};

const shouldGetDataFromSSTableSegmentFile = () => {
  return new Promise((resolve) => {
    const lsm = new LSM(
      basePath,
      logger,
      memTable,
      5,
      2,
      2
    );

    lsm.put(1, 1);
    lsm.put(2, 2);
    lsm.put(3, 3);
    lsm.put(4, 4);
    lsm.put(5, 5);
    lsm.put(6, 6);

    setTimeout(async () => {
      const value = await lsm.get(6);
      assert.equal(value, '6');
      lsm.close();
      resolve();
    }, 5000);
  });
};

const checkCompaction = async () => {
  return new Promise((resolve) => {
    const lsm = new LSM(
      basePath,
      logger,
      memTable,
      5,
      2,
      2
    );

    lsm.put(6, 6);
    lsm.put(1, 1);
    lsm.put(2, 2);
    lsm.put(3, 3);
    lsm.put(4, 4);
    lsm.put(5, 5);

    setTimeout(() => {
      lsm.put(9, 3);
      lsm.put(7, 1);
      lsm.put(8, 2);
      lsm.put(10, 4);
      lsm.put(11, 5);
      lsm.put(1, 10);
    }, 2000);

    setTimeout(async () => {
      let value;

      value = await lsm.get(10);
      assert.equal(value, '4');

      value = await lsm.get(1);
      assert.equal(value, '10');

      lsm.close();
      resolve();
    }, 15000);
  });
};

const testClient = async () => {
  // await shouldGetDataFromSMTableFile();
  // await shouldGetDataFromSSTableSegmentFile();
  await checkCompaction();

  // rimraf.sync(basePath);
};

testClient();