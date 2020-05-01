const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");
const assert = require('assert').strict;

const LSM = require('./LSM');

const basePath = path.join(__dirname, 'example');

if (!fs.existsSync(basePath)) {
  fs.mkdirSync(basePath);
}

const shouldGetDataFromMemTable = () => {
  return new Promise(async (resolve) => {
    const lsm = new LSM(
      basePath,
      1000,
      2,
      0,
      0
    );

    lsm.put(6, 6);

    const value = await lsm.get(6);

    assert.equal(value, 6);

    lsm.stop();

    resolve();
  });
};

const shouldGetDataFromMemTableAfterDrainEvent = () => {
  return new Promise(async (resolve) => {
    const lsm = new LSM(
      basePath,
      1000,
      2,
      0,
      0
    );

    lsm.put(6, 6);

    await lsm._drainMemTable();

    assert.equal(lsm._ssTablesBucket.length, 1);

    lsm.put(6, "6-updated");
    lsm.put(7, 7);

    let value;
    value = await lsm.get(6);
    assert.equal(value, '6-updated');

    value = await lsm.get(7);
    assert.equal(value, 7);

    resolve();
  })
};

const shouldGetDataFromSSTableSegmentFile = () => {
  return new Promise(async (resolve) => {
    const lsm = new LSM(
      basePath,
      1000,
      2,
      0,
      0
    );
    const dummyDb = {};

    lsm.put(1, "1");
    dummyDb[1] = "1";
    lsm.put(2, "2");
    dummyDb[2] = "2";
    lsm.put(3, "3");
    dummyDb[3] = "3";

    await lsm._drainMemTable();

    lsm.put(4, "4");
    dummyDb[4] = "4";
    lsm.put(5, "5");
    dummyDb[5] = "5";
    lsm.put(1, "1-updated");
    dummyDb[1] = "1-updated";

    await lsm._drainMemTable();

    assert.equal(lsm._ssTablesBucket.length, 2);

    for (let i=1; i<=5; i++) {
      const value = await lsm.get(i);
      assert.equal(value, dummyDb[i]);
    }

    resolve();
  });
};

const checkCompaction = async () => {
  return new Promise(async (resolve) => {
    const lsm = new LSM(
      basePath,
      1000,
      2,
      0,
      0
    );

    const dummyDb = {};

    lsm.put(1, "1");
    dummyDb[1] = "1";
    lsm.put(2, "2");
    dummyDb[2] = "2";
    lsm.put(3, "3");
    dummyDb[3] = "3";

    await lsm._drainMemTable();

    lsm.put(4, "4");
    dummyDb[4] = "4";
    lsm.put(5, "5");
    dummyDb[5] = "5";
    lsm.put(1, "1-updated");
    dummyDb[1] = "1-updated";

    await lsm._drainMemTable();

    lsm.put(4, "4-updated");
    dummyDb[4] = "4-updated";
    lsm.put(5, "5");
    dummyDb[5] = "5";
    lsm.put(1, "1-updated-again");
    dummyDb[1] = "1-updated-again";
    lsm.put(6, "6");
    dummyDb[6] = "6";

    await lsm._drainMemTable();

    assert.equal(lsm._ssTablesBucket.length, 3);

    await lsm._doCompaction();

    assert.equal(lsm._ssTablesBucket.length, 2);

    for (let i=1; i<=6; i++) {
      const value = await lsm.get(i);
      assert.equal(value, dummyDb[i]);
    }

    resolve();
  });
};

const dryRun = async () => {
  return new Promise(async (resolve) => {
    const N = 100;
    const MEMTABLE_DRAIN_THRESHOLD = 5;
    const COMPACTION_THRESHOLD = 5;
    const lsm = new LSM(
      basePath,
      1000,
      COMPACTION_THRESHOLD,
      0,
      0
    );
    const dummyDb = {};
    let drainCounter = 0;

    for (let i=0; i<N; i++) {
      lsm.put(i, i);
      dummyDb[i] = i;

      if (i%MEMTABLE_DRAIN_THRESHOLD === 0 && i > 0) {
        await lsm._drainMemTable();
        drainCounter++;
      }

      if (drainCounter%COMPACTION_THRESHOLD === 0 && drainCounter > 0) {
        await lsm._doCompaction();
      }
    }

    for (let i=0; i<N; i++) {
      const value = await lsm.get(i);
      assert.equal(parseInt(value), dummyDb[i]);
    }

    resolve();
  });
};

const testClient = async () => {
  await shouldGetDataFromMemTable();
  await shouldGetDataFromMemTableAfterDrainEvent();
  await shouldGetDataFromSSTableSegmentFile();
  await checkCompaction();
  await dryRun();

  rimraf.sync(basePath);
};

testClient();