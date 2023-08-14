{pkgs, ...}: {
  languages.rust = {
    enable = true;
    channel = "stable";
  };

  packages = [
    pkgs.cargo-nextest
    pkgs.rust-bindgen
  ];

  pre-commit.hooks = {
    cargo-check.enable = true;
    clippy.enable = true;
    rustfmt.enable = true;
  };
}
