{
  pkgs,
  inputs,
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
      fenix.targets.wasm32-unknown-unknown.stable.rust-std
    ];
  };

  packages = [
    pkgs.emscripten
    pkgs.cargo-nextest
    pkgs.rust-bindgen
  ];

  pre-commit.hooks = {
    cargo-check.enable = true;
    clippy.enable = true;
    rustfmt.enable = true;
  };

  env = {
    EMCC_CFLAGS = "--no-entry";
    EM_CACHE = "/tmp/emcache";
  };
}
