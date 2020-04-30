const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");

const SSTableSegment = require('../../ss_tables/SSTableSegment');
const {hashCode} = require('../../utils');

const {ONLY_NUMERIC_KEYS_ACCEPTED} = require("../../errors");

describe('Test SSTableSegment', () => {
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

  const hashKey = (key) => hashCode(`${key}`);

  beforeAll(() => {
    if (!fs.existsSync(basePath)){
      fs.mkdirSync(basePath);
    }
  });

  afterAll(() => {
    rimraf.sync(basePath);
  });

  describe('Integer Keys', () => {
    const dummyDb = {};
    const ssTableSegment = new SSTableSegment(basePath, 100);

    beforeAll(async () => {
      const N = 50;
      for(let i=0;i<N;i++) {
        const key = hashKey(i);
        const value = makeRandomValue(5);
        dummyDb[key] = value;
        await ssTableSegment.put(key, value);
      }
    });

    it('should correct file path', () => {
      expect(ssTableSegment.getFileFullPath())
        .toBe(`${basePath}/${ssTableSegment.fileName}.${ssTableSegment.fileExtension}`);
    });

    it('should get value of key that is present in range of internal index', async () => {
      const key = hashKey(45);
      const value = await ssTableSegment.get(key);

      expect(value).toBe(dummyDb[key]);
    });

    it('should get value of key that is preset in internal index', async () => {
      const key = hashKey(0);
      const value = await ssTableSegment.get(key);

      expect(value).toBe(dummyDb[key]);
    });

    it('should return null if key is not present', async () => {
      let key = hashKey(51);
      let value = await ssTableSegment.get(key);

      expect(value).toBeNull();
    });
  });

  describe('String Keys', () => {
    const dummyDb = {};
    const ssTableSegment = new SSTableSegment(basePath, 100);

    beforeAll(async () => {
      const N = 50;
      const tempKeys = [];
      for(let i=0;i<N;i++) {
        const randomValue = hashKey(makeRandomValue(5));
        dummyDb[randomValue] = makeRandomValue(5);
        tempKeys.push(randomValue);
      }

      tempKeys.sort((a, b) => a - b);

      for(let i=0; i<tempKeys.length; i++) {
        const key = tempKeys[i];
        await ssTableSegment.put(key, dummyDb[key]);
      }
    });

    it('should get value of key', async () => {
      const key = Object.keys(dummyDb)[0];
      const value = await ssTableSegment.get(key);
      expect(value).toBe(dummyDb[key]);
    });

    it('should get value of key that is present in range of internal index', async () => {
      const key = Object.keys(dummyDb)[45];
      const value = await ssTableSegment.get(key);

      expect(value).toBe(dummyDb[key]);
    });

    it('should return null if key is not present', async () => {
      const key = Object.keys(dummyDb)[45] + "sample-random-hash";
      let value = await ssTableSegment.get(key);

      expect(value).toBeNull();
    });
  });

  test('should throw error if key is non-numeric', async () => {
    const ssTableSegment = new SSTableSegment(basePath, 1000);

    try {
      await ssTableSegment.put("test", "test")
    } catch(err) {
      expect(err.code).toBe(ONLY_NUMERIC_KEYS_ACCEPTED);
      expect(err.error.message).toBe('Only numeric keys are accepted');
    }
  });
});
