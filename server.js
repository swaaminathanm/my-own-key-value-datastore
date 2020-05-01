const net = require('net');

const onDataReceived = (socket) => {
  socket.on('data', (data) => {
    console.log('Received: ' + data);
  });
};

const server = net.createServer(onDataReceived);

server.listen(1337, '127.0.0.1', () => {
  console.log('opened server on', server.address());
});