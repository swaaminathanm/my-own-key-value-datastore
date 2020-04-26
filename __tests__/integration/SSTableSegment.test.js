const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");

const {ONLY_NUMERIC_KEYS_ACCEPTED} = require('../../errors');
const SSTableSegment = require('../../ss_tables/SSTableSegment');

const basePath = path.join(__dirname, 'example');

const makeRandomValue = (length) => {
  let result           = '';
  const characters       = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
};

test('check if file path is correct', () => {
  const ssTableSegment = new SSTableSegment(basePath, 1000000, 1000);

  expect(ssTableSegment.getFileFullPath())
    .toBe(`${basePath}/${ssTableSegment.fileName}.${ssTableSegment.fileExtension}`);
});

test('check if write is possible until threshold is reached', async () => {
  if (!fs.existsSync(basePath)){
    fs.mkdirSync(basePath);
  }

  const ssTableSegment = new SSTableSegment(basePath, 1000000, 1000);

  const N = 1000;
  for(let i=0;i<N;i++) {
    const key = Math.floor(9 + Math.random() * 90);
    const value = makeRandomValue(5);
    await ssTableSegment.put(key, value);
  }

  expect(ssTableSegment.canWrite()).toBe(true);
  expect(Object.keys(ssTableSegment._getIndex()).length).toBeGreaterThanOrEqual(8);
  expect(Object.keys(ssTableSegment._getIndex()).length).toBeLessThanOrEqual(10);

  rimraf.sync(basePath);
});

test('check if findNearestKey function is working properly', () => {
  let arr = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91];
  let result;

  result = SSTableSegment.findNearestKey(arr, 23);
  expect(result.key).toBe(23);

  result = SSTableSegment.findNearestKey(arr, 21);
  expect(result.nearestMinima).toBe(16);
  expect(result.nearestMaxima).toBe(23);

  result = SSTableSegment.findNearestKey(arr, 91);
  expect(result.key).toBe(91);

  result = SSTableSegment.findNearestKey(arr, 99);
  expect(result.nearestMinima).toBe(91);
  expect(result.nearestMaxima).toBe(undefined);

  result = SSTableSegment.findNearestKey(arr, 0);
  expect(result.nearestMinima).toBe(undefined);
  expect(result.nearestMaxima).toBe(2);
});

test('check get value of key that is present in range of internal index', async () => {
  if (!fs.existsSync(basePath)){
    fs.mkdirSync(basePath);
  }

  const ssTableSegment = new SSTableSegment(basePath, 1000000, 1000);

  const dummyDb = {};
  const N = 250;
  for(let i=0;i<N;i++) {
    const key = Math.floor(i);
    const value = makeRandomValue(5);
    dummyDb[i] = value;
    await ssTableSegment.put(key, value);
  }

  let key = "150";
  let result = await ssTableSegment.get(key);

  expect(result.key).toBe(key);
  expect(result.value).toBe(dummyDb[key]);

  key = "249";
  result = await ssTableSegment.get(key);

  expect(result.key).toBe(key);
  expect(result.value).toBe(dummyDb[key]);

  rimraf.sync(basePath);
});

test('check get value of key that is present exactly in internal index', async () => {
  if (!fs.existsSync(basePath)){
    fs.mkdirSync(basePath);
  }

  const ssTableSegment = new SSTableSegment(basePath, 1000000, 1000);

  const dummyDb = {};
  const N = 10;
  for(let i=0;i<N;i++) {
    const key = Math.floor(i);
    const value = makeRandomValue(5);
    dummyDb[i] = value;
    await ssTableSegment.put(key, value);
  }

  let key = "0";
  let result = await ssTableSegment.get(key);

  expect(result.key).toBe(key);
  expect(result.value).toBe(dummyDb[key]);

  rimraf.sync(basePath);
});

test('check get value return null if key not present', async () => {
  if (!fs.existsSync(basePath)){
    fs.mkdirSync(basePath);
  }

  const ssTableSegment = new SSTableSegment(basePath, 1000000, 1000);

  const dummyDb = {};
  const N = 10;
  for(let i=0;i<N;i++) {
    const key = Math.floor(i);
    const value = makeRandomValue(5);
    dummyDb[i] = value;
    await ssTableSegment.put(key, value);
  }

  let key = "10";
  let result = await ssTableSegment.get(key);

  expect(result.value).toBeNull();

  rimraf.sync(basePath);
});

test('should throw error if key is non-numeric', async () => {
  const ssTableSegment = new SSTableSegment(basePath, 1000000, 1000);

  try {
    await ssTableSegment.put("test", "test")
  } catch(err) {
    expect(err.code).toBe(ONLY_NUMERIC_KEYS_ACCEPTED);
    expect(err.error.message).toBe('Only numeric keys are accepted');
  }
});