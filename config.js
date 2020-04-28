module.exports = {
  RED: 'red',
  BLACK: 'black',
  TOTAL_MEMTABLE_NODES_ACCEPTABLE: 2 * Math.pow(10, 6),
  SM_TABLE_MAX_SIZE_IN_BYTES: 100 * Math.pow(10, 7), // 100MB
  COMPACTION_SM_TABLE_MAX_SIZE_IN_BYTES: 100 * Math.pow(10, 9), // 100GB
  SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES: 500000, // 0.5MB
  COMPACTION_THRESHOLD: 5
};