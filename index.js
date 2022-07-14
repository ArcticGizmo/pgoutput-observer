const Listener = require('./src/listener');
const readline = require('readline');

const CONNECTION_STRING = 'postgresql://postgres@localhost:5432/dev';
const SLOT_NAME = 'my_slot';
const PUBLICATION_NAME = 'my_publication';

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const prompt = query => new Promise(resolve => rl.question(query, resolve));

function strikeThru(any) {
  return `${any}`
    .split('')
    .map(c => '\u0336' + c)
    .join('');
}

const opts = {
  connectionString: CONNECTION_STRING,
  slot: SLOT_NAME,
  publication: PUBLICATION_NAME,
};

const client = new Listener(opts);

let blocks = [];

client.listen(async (block, next, ack) => {
  console.log('=============');
  console.log(block.xid);

  blocks.push(block);

  const message = await prompt('acknowledge?');

  if (message.toLowerCase().startsWith('n')) {
    console.log(`We now have '${blocks.length}' outstanding blocks`);
  } else {
    ack(block.endLsn);
    console.log(blocks.map(b => strikeThru(b.xid)));
    blocks = [];
  }

  next();
});

require('net').createServer().listen();
