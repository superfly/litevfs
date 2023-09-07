'use strict';
const sqlite = require('better-sqlite3');
const path = require('path');

function Database(filename, options) {
  const extdb = sqlite(":memory:");
  extdb.loadExtension(path.join(__dirname, "../target/release/liblitevfs.so"));
  extdb.close();
  
  return new sqlite(filename, options);
}

sqlite.prototype.acquire_write_lease = function() {
  this.pragma('litevfs_acquire_lease');
};
sqlite.prototype.release_write_lease = function() {
  this.pragma('litevfs_release_lease');
};
sqlite.prototype.with_write_lease = function(cb) {
  this.acquire_write_lease();
  try {
    cb();
  } finally {
    this.release_write_lease();
  }
};

module.exports = Database;
