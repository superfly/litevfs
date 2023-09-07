{
  pkgs,
  inputs,
  lib,
  ...
}: let
  fenix = inputs.fenix.packages.${builtins.currentSystem};
in {
  languages.rust = {
    enable = true;
    channel = "stable";
    toolchain.rustc = fenix.combine [
      fenix.stable.rustc
      fenix.targets.wasm32-unknown-emscripten.stable.rust-std
    ];
  };

  packages = [
    # Basic rust tools
    pkgs.cargo-nextest
    pkgs.rust-bindgen

    # Emscripten target
    pkgs.emscripten
    pkgs.wabt

    # NPM packaging
    pkgs.nodejs
  ];

  pre-commit = {
    hooks = {
      cargo-check.enable = true;
      clippy.enable = true;
      rustfmt.enable = true;
    };
    settings = {
      clippy.denyWarnings = true;
    };
  };

  env = {
    EM_CACHE = "/tmp/emcache";
  };
}
