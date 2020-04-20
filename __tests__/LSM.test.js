const LSM = require('../LSM');

describe('LSM', () => {
  const fakeLogger = {
    addToLog: jest.fn(),
    isEmpty: jest.fn(),
    drain: jest.fn()
  };

  const fakeMemTable = {
    insert: jest.fn(),
    isAvailable: jest.fn()
  };

  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.resetAllMocks();
  });

  it('should log data when put is called', () => {
    let lsm = new LSM(fakeLogger, fakeMemTable);

    lsm.put(1, 'sample value');

    const expectedLog = JSON.stringify({
      key: 1,
      data: 'sample value'
    });

    expect(fakeLogger.addToLog).toHaveBeenCalledWith(expectedLog);
  });

  it('should insert into memtable', () => {
    let lsm = new LSM(fakeLogger, fakeMemTable);

    const fakeLog1 = JSON.stringify({
      key: 1,
      data: 'sample value 1'
    });

    const fakeLog2 = JSON.stringify({
      key: 2,
      data: 'sample value 2'
    });

    fakeLogger.isEmpty.mockReturnValue(false);
    fakeMemTable.isAvailable.mockReturnValue(true);
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
    let lsm = new LSM(fakeLogger, fakeMemTable);

    fakeLogger.isEmpty.mockReturnValue(true);

    jest.advanceTimersByTime(1000);

    expect(fakeMemTable.insert).toHaveBeenCalledTimes(0);
  });

  it('should not insert into memtable if memtable is not available', () => {
    let lsm = new LSM(fakeLogger, fakeMemTable);

    fakeLogger.isEmpty.mockReturnValue(false);
    fakeMemTable.isAvailable.mockReturnValue(false);

    jest.advanceTimersByTime(1000);

    expect(fakeMemTable.insert).toHaveBeenCalledTimes(0);
  });
});
