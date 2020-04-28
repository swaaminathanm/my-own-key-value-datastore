const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");

const LSM = require('../../LSM');
const Logger = require('../../Logger');
const MemTable = require('../../RedBlackTree');

describe('LSM Tests', () => {
  const basePath = path.join(__dirname, 'example');
  let logger;
  let memTable;

  beforeAll(async () => {
    if (!fs.existsSync(basePath)) {
      fs.mkdirSync(basePath);
    }
  });

  beforeEach(() => {
    logger = new Logger();
    memTable = new MemTable(5);
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.resetAllMocks();
  });

  afterAll(() => {
    rimraf.sync(basePath);
  });

  it('should get value from memtable if valid key is passed', async () => {
    const lsm = new LSM(
      basePath,
      logger,
      memTable,
      5,
      2
    );

    lsm.put(1, 1);
    lsm.put(2, 2);
    lsm.put(3, 3);

    jest.advanceTimersByTime(1000);

    const value = await lsm.get(2);

    expect(value).toBe(2);
  });

  it('should get null from memtable if invalid key is passed', async () => {
    const lsm = new LSM(
      basePath,
      logger,
      memTable,
      5,
      2
    );

    lsm.put(1, 1);
    lsm.put(2, 2);
    lsm.put(3, 3);

    jest.advanceTimersByTime(1000);

    const value = await lsm.get(20);

    expect(value).toBeNull();
  });

  xit('should get value from SSTableSegment if valid key is passed', async () => {
    const lsm = new LSM(
      basePath,
      logger,
      memTable,
      5,
      2
    );

    lsm.put(1, 1);
    lsm.put(2, 2);
    lsm.put(3, 3);
    lsm.put(4, 4);
    lsm.put(5, 5);
    lsm.put(6, 6);

    jest.advanceTimersByTime(1000);

    const value = await lsm.get(20);

    expect(value).toBeNull();
  });
});