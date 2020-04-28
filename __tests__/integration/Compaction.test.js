const path = require('path');
const fs = require('fs');
const rimraf = require("rimraf");

const SSTableSegment = require('../../ss_tables/SSTableSegment');
const Compaction = require('../../Compaction');

const basePath = path.join(__dirname, 'example');

describe('Compaction Tests', () => {
  const ssTableSegments = [];
  let compaction;

  beforeAll(async () => {
    if (!fs.existsSync(basePath)){
      fs.mkdirSync(basePath);
    }

    let ssTableSegment1 = new SSTableSegment(basePath, 1000);
    await ssTableSegment1.put("1", "A");
    await ssTableSegment1.put("2", "B");
    await ssTableSegment1.put("3", "C");

    let ssTableSegment2 = new SSTableSegment(basePath, 1000);
    await ssTableSegment2.put("2", "D");
    await ssTableSegment2.put("3", "E");

    let ssTableSegment3 = new SSTableSegment(basePath, 1000);
    await ssTableSegment3.put("1", "F");
    await ssTableSegment3.put("2", "G");

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

    let log1, log2, log3;

    log1 = await ssTableSegment.get("1");
    expect(log1.key).toBe("1");
    expect(log1.value).toBe("F");

    log2 = await ssTableSegment.get("2");
    expect(log2.key).toBe("2");
    expect(log2.value).toBe("G");

    log3 = await ssTableSegment.get("3");
    expect(log3.key).toBe("3");
    expect(log3.value).toBe("E");
  });
});