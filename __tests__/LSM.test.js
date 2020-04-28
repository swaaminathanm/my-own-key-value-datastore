const path = require('path');

const LSM = require('../LSM');

jest.mock('../ss_tables/SSTableSegment');

describe('LSM', () => {
  let lsm;
  const basePath = path.join(__dirname, 'example');

  const fakeLogger = {
    addToLog: jest.fn(),
    isEmpty: jest.fn(),
    drain: jest.fn()
  };

  const fakeMemTable = {
    insert: jest.fn(),
    canAccept: jest.fn(),
    getValues: jest.fn(),
    removeLowest: jest.fn()
  };

  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.resetAllMocks();
  });

  it('should log data when put is called', () => {
    lsm = new LSM(basePath, fakeLogger, fakeMemTable, 10, 5);

    lsm.put(1, 'sample value');

    const expectedLog = JSON.stringify({
      key: 1,
      data: 'sample value'
    });

    expect(fakeLogger.addToLog).toHaveBeenCalledWith(expectedLog);
  });

  it('should insert into memtable', () => {
    lsm = new LSM(basePath, fakeLogger, fakeMemTable, 10, 5);

    const fakeLog1 = JSON.stringify({
      key: 1,
      data: 'sample value 1'
    });

    const fakeLog2 = JSON.stringify({
      key: 2,
      data: 'sample value 2'
    });

    fakeLogger.isEmpty.mockReturnValue(false);
    fakeMemTable.canAccept.mockReturnValue(true);
    fakeLogger.drain.mockReturnValue([
      fakeLog1,
      fakeLog2
    ]);

    jest.advanceTimersByTime(1000);

    expect(fakeLogger.drain).toHaveBeenCalledTimes(1);
    expect(fakeMemTable.insert).toHaveBeenCalledTimes(2);
    expect(fakeMemTable.insert.mock.calls[0][0]).toBe(1);
    expect(fakeMemTable.insert.mock.calls[0][1]).toBe('sample value 1');
    expect(fakeMemTable.insert.mock.calls[1][0]).toBe(2);
    expect(fakeMemTable.insert.mock.calls[1][1]).toBe('sample value 2');
  });

  it('should not insert into memtable if logger is empty', () => {
    lsm = new LSM(basePath, fakeLogger, fakeMemTable, 10, 5);

    fakeLogger.isEmpty.mockReturnValue(true);

    jest.advanceTimersByTime(1000);

    expect(fakeMemTable.insert).toHaveBeenCalledTimes(0);
  });
});