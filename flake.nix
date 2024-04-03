{
  description = "litevfs-dev";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    flake-utils,
    nixpkgs,
    fenix,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {inherit system;};
      toolchain = with fenix.packages.${system};
        combine [
          stable.toolchain
          targets.wasm32-unknown-emscripten.stable.rust-std
        ];
    in {
      devShells.default = pkgs.mkShell {
        buildInputs = [
          toolchain
          pkgs.cargo-nextest
          pkgs.rust-bindgen

          # Emscripten target
          pkgs.emscripten
          pkgs.wabt

          # NPM packaging
          pkgs.nodejs
        ];
      };
    });
}
