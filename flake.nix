{
  description = "EasyDB — Rust 关系型数据库";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs }: {
    devShells.x86_64-linux =
      let
        pkgs = import nixpkgs { system = "x86_64-linux"; config = {}; overlays = []; };
      in
      {
        default = pkgs.mkShell {
          packages = with pkgs; [
            rustc
            cargo
            rust-analyzer
            clippy
            rustfmt
            nodejs_20
            nodePackages.npm
            (pkgs.python3.withPackages (python-pkgs: [
              python-pkgs.websockets
            ]))
          ];
        };
      };

    packages.x86_64-linux =
      let
        pkgs = import nixpkgs { system = "x86_64-linux"; config = {}; overlays = []; };
      in
      rec {
        easydb = pkgs.rustPlatform.buildRustPackage {
          pname = "easydb";
          version = "0.1.0";
          src = ./.;

          cargoLock.lockFile = ./Cargo.lock;

          # 服务端与 TUI 客户端两个二进制由 Cargo.toml [[bin]] 定义，
          # buildRustPackage 默认安装全部 bin。
        };
        default = easydb;
      };
  };
}
