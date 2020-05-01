const net = require('net');

const client = new net.Socket();

const connectionListener = () => {
  client.write(JSON.stringify({
    key: 'sample-key',
    value: 'sample-value'
  }));
};

client.connect(1337, '127.0.0.1', connectionListener);

client.on('connect', () => {
  console.log('Connection established');
});

client.on('close', () => {
  console.log('Connection closed');
});