'use strict';
const sqlite = require('better-sqlite3');
const path = require('path');

var requireFunc =
  typeof __webpack_require__ === 'function'
    ? __non_webpack_require__
    : require;

const extensionPath = requireFunc.resolve("litevfs/build/litevfs");

function Database(filename, options) {
  const extdb = sqlite(":memory:");
  extdb.loadExtension(extensionPath, "sqlite3_litevfs_init_default_vfs");
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
