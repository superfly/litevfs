'use strict';
const sqlite = require('better-sqlite3');
const path = require('path');

function Database(filename, options) {
  const extdb = sqlite(":memory:");
  extdb.loadExtension(path.join(__dirname, "../target/release/liblitevfs.so"));
  extdb.close();
  
  const db = new sqlite(filename, options);
  db.pragma("journal_mode=memory");
  return db;
}

module.exports = Database;
