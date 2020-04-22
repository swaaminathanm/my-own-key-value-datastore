const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");

const SSTableSegment = require('../../ss_tables/SSTableSegment');

const basePath = path.join(__dirname, 'example');
const ssTableSegment = new SSTableSegment(basePath);

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
  expect(ssTableSegment.getFileFullPath())
    .toBe(`${basePath}/${ssTableSegment.fileName}.${ssTableSegment.fileExtension}`);
});

test('check if write is possible until threshold is reached', async () => {
  if (!fs.existsSync(basePath)){
    fs.mkdirSync(basePath);
  }

  const N = 1000;
  for(let i=0;i<N;i++) {
    const key = Math.floor(9 + Math.random() * 90);
    const value = makeRandomValue(5);
    await ssTableSegment.write(key, value);
  }

  expect(ssTableSegment.canWrite()).toBe(true);
  expect(Object.keys(ssTableSegment.getIndex()).length).toBeGreaterThanOrEqual(8);
  expect(Object.keys(ssTableSegment.getIndex()).length).toBeLessThanOrEqual(10);

  rimraf.sync(basePath);
});

test('check if findNearestKey function is working properly', () => {
  let arr = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91];
  let result;

  result = SSTableSegment.findNearestKey(arr, 23);
  expect(result).toEqual([23]);

  result = SSTableSegment.findNearestKey(arr, 21);
  expect(result).toEqual([16, 23]);

  result = SSTableSegment.findNearestKey(arr, 91);
  expect(result).toEqual([91]);

  result = SSTableSegment.findNearestKey(arr, 99);
  expect(result).toEqual([]);

  result = SSTableSegment.findNearestKey(arr, 0);
  expect(result).toEqual([]);

  arr = [-20, -18, -10, -2, 0, 1, 10, 20, 22];

  result = SSTableSegment.findNearestKey(arr, -18);
  expect(result).toEqual([-18]);

  result = SSTableSegment.findNearestKey(arr, 12);
  expect(result).toEqual([10, 20]);

  result = SSTableSegment.findNearestKey(arr, -17);
  expect(result).toEqual([-18, -10]);
});