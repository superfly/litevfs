'use strict';
const sqlite = require('better-sqlite3');
const path = require('path');
const process = require('node:process');
const fs = require('node:fs');


const supportedPlatforms = [
  ["darwin", "x64"],
  ["darwin", "arm64"],
  ["linux", "x64"],
];

function validPlatform(platform, arch) {
  return (
    supportedPlatforms.find(([p, a]) => platform == p && arch === a) !== null
  );
}

function extensionSuffix(platform) {
  if (platform === "win32") return "dll";
  if (platform === "darwin") return "dylib";
  return "so";
}

function platformPackageName(platform, arch) {
  const os = platform === "win32" ? "windows" : platform;
  return `litevfs-${os}-${arch}`;
}

function getLoadablePath() {
  if (!validPlatform(process.platform, process.arch)) {
    throw new Error(
      `Unsupported platform for litevfs, on a ${platform}-${arch} machine, but not in supported platforms (${supportedPlatforms
        .map(([p, a]) => `${p}-${a}`)
        .join(",")}). Consult the litevfs NPM package README for details. `
    );
  }
  const packageName = platformPackageName(process.platform, process.arch);
  const loadablePath = path.join(
    __dirname,
    "..",
    "..",
    packageName,
    "lib",
    `liblitevfs.${extensionSuffix(process.platform)}`
  );
  if (!fs.statSync(loadablePath, { throwIfNoEntry: false })) {
    throw new Error(
      `Loadble extension for litevfs not found. Was the ${packageName} package installed? Avoid using the --no-optional flag, as the optional dependencies for litevfs are required.`
    );
  }

  return loadablePath;
}

function Database(filename, options) {
  const extdb = sqlite(":memory:");
  extdb.loadExtension(getLoadablePath());
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
