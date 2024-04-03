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
      toolchain = fenix.packages.${system}.fromToolchainFile {
        file = ./rust-toolchain.toml;
        sha256 = "sha256-3St/9/UKo/6lz2Kfq2VmlzHyufduALpiIKaaKX4Pq0g=";
      };
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
