const net = require('net');
const uuid = require('uuid');

const uuidv4 = uuid.v4;

const PORT = 1337;
const HOSTNAME = '127.0.0.1';

class Client {
  constructor(clientSocket) {
    this.clientSocket = clientSocket;
    this.callbackToRequestMapping = {};
  }

  onDataReceived(buffer) {
    const data = JSON.parse(buffer.toString());
    const requestId = data.requestId;
    delete data.requestId;
    delete data.type;

    const callback = this.callbackToRequestMapping[requestId];

    if (callback) {
      callback(data);
    }

    delete this.callbackToRequestMapping[requestId];
  };

  generateRequestId() {
    return uuidv4();
  }

  put(key, value, cb) {
    if (key && value) {
      const requestId = this.generateRequestId();
      this.clientSocket.write(JSON.stringify({
        requestId,
        type: 'PUT',
        data: {
          key,
          value
        }
      }));
      this.callbackToRequestMapping[requestId] = cb;
    } else {
      throw new Error('key/value must be valid');
    }
  };

  get(key, cb) {
    if (key) {
      const requestId = this.generateRequestId();
      this.clientSocket.write(JSON.stringify({
        requestId,
        type: 'GET',
        data: {
          key
        }
      }));
      this.callbackToRequestMapping[requestId] = cb;
    } else {
      throw new Error('key must be valid');
    }
  };

  close() {
    this.clientSocket.destroy();
    this.callbackToRequestMapping = {};
  };
}

const connect = (cb) => {
  const clientSocket = new net.Socket();
  clientSocket.connect(PORT, HOSTNAME, () => {
    const client = new Client(clientSocket);
    clientSocket.on('data', client.onDataReceived.bind(client));
    cb(client);
  });
};

module.exports = connect;