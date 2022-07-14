const Listener = require("./src/listener");

const CONNECTION_STRING = "postgresql://postgres@localhost:5432/dev";
const SLOT_NAME = "my_slot";
const PUBLICATION_NAME = "my_publication";

const opts = {
  connectionString: CONNECTION_STRING,
  slot: SLOT_NAME,
  publication: PUBLICATION_NAME,
};

const Client = new Listener(opts);

async function start() {}

require("net").createServer().listen();
start();
