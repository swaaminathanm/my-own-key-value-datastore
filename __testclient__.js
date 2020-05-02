const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");
const assert = require('assert').strict;

const LSM = require('./LSM');

const basePath = path.join(__dirname, 'example');

const makeRandomValue = (length) => {
  let result = '';
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
};

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

const endToEndTest = async () => {
  return new Promise(async (resolve) => {
    const lsm = new LSM(
      basePath,
      1000,
      5,
      1 * 1000,
      3 * 1000
    );
    const dummyDb = {};

    const timerId = setInterval(
      () => {
      const batchSize = 10;
      const startTime = new Date();

      for (let i=0; i<batchSize; i++) {
        const key = makeRandomValue(10);
        const value = makeRandomValue(20);
        lsm.put(key, value);
        dummyDb[key] = value;
      }

      console.log('time_taken:batch_insert', `batch_size:${batchSize}`, `time:${new Date() - startTime}`)
    }, 500);

    setTimeout(async () => {
        clearInterval(timerId);

        console.log('total_insert_count', Object.keys(dummyDb).length);

        const startTime = new Date();
        const keys = Object.keys(dummyDb);

        for (let i=0; i<keys.length; i++) {
          const key = keys[i];
          const value = await lsm.get(key);
          assert.equal(value, dummyDb[key]);
        }

        console.log('time_taken:get', `time:${new Date() - startTime}`);

        lsm.stop();

        resolve();
    },
      1 * 60 * 1000
    );
  });
};

const main = async () => {
  await shouldGetDataFromMemTable();
  await shouldGetDataFromMemTableAfterDrainEvent();
  await shouldGetDataFromSSTableSegmentFile();
  await checkCompaction();
  await dryRun();
  await endToEndTest();

  rimraf.sync(basePath);
};

main();