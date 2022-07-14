const Listener = require('./src/listener');

const CONNECTION_STRING = 'postgresql://postgres@localhost:5432/dev';
const SLOT_NAME = 'my_slot';
const PUBLICATION_NAME = 'my_publication';

const opts = {
  connectionString: CONNECTION_STRING,
  slot: SLOT_NAME,
  publication: PUBLICATION_NAME,
};

let once = true;

const client = new Listener(opts);

client.listen((block, complete) => {
  console.log('=============');
  console.log(block);
});

require('net').createServer().listen();
