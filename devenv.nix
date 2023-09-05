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
    pkgs.wabt
    pkgs.cargo-nextest
    pkgs.rust-bindgen
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
