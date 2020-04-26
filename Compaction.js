const SSTableSegment = require('./ss_tables/SSTableSegment');

class Compaction {

  /**
   *
   * @param ssTableSegments - Array of SSTableSegment objects in descending order (latest file first)
   * @param basePath - The base path for the new SSTableSegment file that will be created after compaction process
   * @param SM_TABLE_MAX_SIZE_IN_BYTES - Maximum size of new SSTableSegment file in bytes
   * @param SM_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES - Maximum sparse space for in-memory index of new SSTableSegment
   */
  constructor(
    ssTableSegments,
    basePath,
    SM_TABLE_MAX_SIZE_IN_BYTES,
    SM_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES
  ) {
    this._ssTableSegmentsToCompact = ssTableSegments;
    this.ssTableSegment = new SSTableSegment(basePath, SM_TABLE_MAX_SIZE_IN_BYTES, SM_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES);
  }

  getSSTableSegment() {
    return this.ssTableSegment;
  }

  async doCompaction() {
    /**
     * Helper function to check if the compaction process is done
     * @returns {boolean} - true if compaction process is over
     */
    const isCompactionDone = (compactionMetaInfoObjects) => {
      return compactionMetaInfoObjects.reduce((result, metaInfo) => {
        return result && (metaInfo.position === -1)
      }, true);
    };

    const initializeCompactionMetaInfo = () => {
      const compactionMetaInfoObjects = [];

      this._ssTableSegmentsToCompact.forEach((ssTableSegment, i) => {
        const metaInfo = {};
        metaInfo.priority = i+1;
        metaInfo.ssTableSegmentObject = ssTableSegment;
        metaInfo.position = 0;
        metaInfo.name = ssTableSegment.toString();

        compactionMetaInfoObjects.push(metaInfo);
      });

      return compactionMetaInfoObjects;
    };

    /**
     * Helper function that returns updated meta info objects with valid cursor position.
     * The updated meta info objects contain additional information in data
     * @param compactionMetaInfoObjects
     * @returns {Promise<[]>}
     */
    const getUpdatedMetaInfoObjects = async (compactionMetaInfoObjects) => {
      const updatedCompactionMetaInfoObjects = [];

      for (let i = 0; i < compactionMetaInfoObjects.length; i++) {
        const metaInfo = { ...compactionMetaInfoObjects[i] };

        if (metaInfo.position < 0) continue;

        const { key, value, cursor } =
          await metaInfo.ssTableSegmentObject.readKeyValueFromPosition(metaInfo.position);

        metaInfo.data = {
          key,
          value,
          cursor
        };

        updatedCompactionMetaInfoObjects.push(metaInfo);
      }

      return updatedCompactionMetaInfoObjects;
    };

    /**
     * Helper function to generate map between key: [compactionMetaInfo, compactionMetaInfo]
     * @param compactionMetaInfoObjects
     */
    const getKeyMetaInfoObjectIndexMap = (compactionMetaInfoObjects) => {
      const map = {};
      compactionMetaInfoObjects.forEach((metaInfo, i) => {
        const key = metaInfo.data.key;
        if (!map[key]) {
          map[key] = [];
        }
        map[key].push(i);
      });

      return map;
    };

    let compactionMetaInfoObjects = initializeCompactionMetaInfo();

    /**
     * Algorithm:
     * 1. While loop condition: Check if position of all files have reached -1
     * 2. Read values from files for the current position (greater than -1) and append key, value and cursor
     * to compactionMetaInfo object
     * 3. Sort the compactionMetaInfo objects based on key and priority in ascending order
     * 4. Get the first object from the array and add to the new SSTableSegment
     * 5. Get changed compactionMetaInfo objects and increase their positions
     * 6. When while loop is done return the newly created SSTableSegment object
     */
    while (!isCompactionDone(compactionMetaInfoObjects)) {
      compactionMetaInfoObjects = await getUpdatedMetaInfoObjects(compactionMetaInfoObjects);
      compactionMetaInfoObjects.sort((metaInfo1, metaInfo2) => {
        const key1 = metaInfo1.data.key.toUpperCase();
        const key2 = metaInfo2.data.key.toUpperCase();
        const priority1 = metaInfo1.priority;
        const priority2 = metaInfo2.priority;

        if (key1 < key2) {
          return -1;
        } else if (key1 > key2) {
          return 1;
        }
        return priority1 < priority2 ? -1 : 1;
      });

      const metaObjectWithLowestKey = compactionMetaInfoObjects[0];
      await this.ssTableSegment.put(metaObjectWithLowestKey.data.key, metaObjectWithLowestKey.data.value);

      // Get changed compactionMetaInfo objects and increase their positions
      const keyMetaInfoObjectIndexMap = getKeyMetaInfoObjectIndexMap(compactionMetaInfoObjects);
      keyMetaInfoObjectIndexMap[metaObjectWithLowestKey.data.key].forEach((metaInfoObjectIndex) => {
        const metaInfoObject = compactionMetaInfoObjects[metaInfoObjectIndex];
        metaInfoObject.position = metaInfoObject.data.cursor.nextPosition;
      });
    }

    return this.ssTableSegment;
  }
}

module.exports = Compaction;