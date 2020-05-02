const connect = require('./client');

connect((client) => {
  client.put('Alice', 'alice@gmail.com', (data) => {
    console.log(data);
    client.get('Alice', (data) => {
      console.log(data);
      client.close();
    });
  });
});