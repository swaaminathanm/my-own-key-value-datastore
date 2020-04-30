const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");

const SSTableSegment = require('../../ss_tables/SSTableSegment');
const Compaction = require('../../Compaction');
const {hashCode} = require('../../utils');

const basePath = path.join(__dirname, 'example');

describe('Compaction Tests', () => {
  const ssTableSegments = [];
  let compaction;

  const hashKey = (key) => hashCode(`${key}`);

  beforeAll(async () => {
    if (!fs.existsSync(basePath)){
      fs.mkdirSync(basePath);
    }

    let ssTableSegment1 = new SSTableSegment(basePath, 100);
    await ssTableSegment1.put(hashKey(1), "A");
    await ssTableSegment1.put(hashKey(2), "B");
    await ssTableSegment1.put(hashKey(3), "C");

    let ssTableSegment2 = new SSTableSegment(basePath, 100);
    await ssTableSegment2.put(hashKey(2), "D");
    await ssTableSegment2.put(hashKey(3), "E");

    let ssTableSegment3 = new SSTableSegment(basePath, 100);
    await ssTableSegment3.put(hashKey(1), "F");
    await ssTableSegment3.put(hashKey(2), "G");

    ssTableSegments.push(ssTableSegment3);
    ssTableSegments.push(ssTableSegment2);
    ssTableSegments.push(ssTableSegment1);

    compaction = new Compaction(ssTableSegments, basePath, 1000);
  });

  afterAll(() => {
    rimraf.sync(basePath);
  });

  it("should do proper compaction", async () => {
    const ssTableSegment = await compaction.doCompaction();

    let value;

    value = await ssTableSegment.get(hashKey(1));
    expect(value).toBe("F");

    value = await ssTableSegment.get(hashKey(2));
    expect(value).toBe("G");

    value = await ssTableSegment.get(hashKey(3));
    expect(value).toBe("E");
  });
});