const net = require('net');
const path = require('path');

const LSM = require('./LSM');

const PORT = 1337;
const HOSTNAME = '127.0.0.1';
const DB_DIRECTORY_NAME = 'db_data';
const SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES = 5000;
const COMPACTION_THRESHOLD = 5;
const DRAIN_MEM_TABLE_INTERVAL = 30 * 1000;
const COMPACTION_INTERVAL = 60 * 1000;

const dirPathToCreateSSTables = path.join(__dirname, DB_DIRECTORY_NAME);

const lsm = new LSM(
  dirPathToCreateSSTables,
  SS_TABLE_IN_MEMORY_SPARSE_KEYS_THRESHOLD_BYTES,
  COMPACTION_THRESHOLD,
  DRAIN_MEM_TABLE_INTERVAL,
  COMPACTION_INTERVAL
);

const writeSuccess = (socket, data, requestId) => {
  socket.write(JSON.stringify({
    requestId,
    type: 'SUCCESS',
    data
  }));
};

const writeError = (socket, message, requestId) => {
  socket.write(JSON.stringify({
    requestId,
    message: message || 'Operational error',
    type: 'ERROR'
  }));
};

const handlePut = (data = {}, socket, requestId) => {
  const key = data.key;
  const value = data.value;

  if (!key || !value) {
    writeError(socket, 'Format not proper.', requestId);
  } else {
    lsm.put(key, value);
    writeSuccess(socket, null, requestId);
  }
};

const handleGet = async (data = {}, socket, requestId) => {
  const key = data.key;

  if (!key) {
    writeError(socket, 'Format not proper.', requestId);
  } else {
    const value = await lsm.get(key);
    writeSuccess(socket, { value }, requestId);
  }
};

const connectionListener = (socket) => {
  /**
   * @param data
   * structure:
   * {
   *   requestId: <id>,
   *   type: <PUT/GET>,
   *   data: <object>
   * }
   */
  socket.on('data', async (socketData) => {
    const dataJson = JSON.parse(socketData);
    const data = dataJson.data;
    const type = dataJson.type;
    const requestId = dataJson.requestId;

    if (type === 'PUT') {
      handlePut(data, socket, requestId);
    } else if (type === 'GET') {
      await handleGet(data, socket, requestId);
    } else {
      writeError(socket, 'Unrecognized type. Only GET and PUT supported.', requestId);
    }
  });
};

const server = net.createServer(connectionListener);

server.listen(PORT, HOSTNAME, () => {
  console.log('opened server on', server.address());
});
