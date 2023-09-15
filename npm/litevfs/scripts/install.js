#!/usr/bin/env node

'use strict';
const path = require('path');
const process = require('node:process');
const fs = require('node:fs');

const supportedPlatforms = [
  ["darwin", "x64"],
  ["darwin", "arm64"],
  ["linux", "x64"],
  ["linux", "arm64"],
  ["windows", "x64"],
];

function validPlatform(platform, arch) {
  return (
    supportedPlatforms.find(([p, a]) => platform == p && arch === a) !== null
  );
}

function extensionPrefix(platform) {
  if (platform == "win32") return "";
  return "lib";
}

function extensionSuffix(platform) {
  if (platform === "win32") return "dll";
  if (platform === "darwin") return "dylib";
  return "so";
}

function platformPackageName(platform, arch) {
  function isMusl() {
    if (!process.report || typeof process.report.getReport !== 'function') {
      try {
        return readFileSync('/usr/bin/ldd', 'utf8').includes('musl')
      } catch (e) {
        return true
      }
    } else {
      const { glibcVersionRuntime } = process.report.getReport().header
      return !glibcVersionRuntime
    }
  }

  const os = platform === "win32" ? "windows" : platform;
  const abi = platform == "linux" ? (isMusl() ? "-musl" : "-gnu") : "";

  return `litevfs-${os}-${arch}${abi}`;
}

var requireFunc =
  typeof __webpack_require__ === 'function'
    ? __non_webpack_require__
    : require;

function getLoadablePath() {
  if (!validPlatform(process.platform, process.arch)) {
    throw new Error(
      `Unsupported platform for litevfs, on a ${platform}-${arch} machine, but not in supported platforms (${supportedPlatforms
        .map(([p, a]) => `${p}-${a}`)
        .join(",")}). Consult the litevfs NPM package README for details. `
    );
  }

  const packageName = platformPackageName(process.platform, process.arch);
  const fileName = `${extensionPrefix(process.platform)}litevfs.${extensionSuffix(process.platform)}`;
  const loadablePath = requireFunc.resolve(packageName + "/lib/" + fileName);
  if (!fs.statSync(loadablePath, { throwIfNoEntry: false })) {
    throw new Error(
      `Loadble extension for litevfs not found. Was the ${packageName} package installed? Avoid using the --no-optional flag, as the optional dependencies for litevfs are required.`
    );
  }

  return loadablePath;
}

const outPath = path.join(__dirname, "..", "build");
const extensionPath = getLoadablePath();

fs.mkdirSync(outPath);
fs.copyFileSync(extensionPath, path.join(outPath, "litevfs"));
