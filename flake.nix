{
  description = "flake for easydb";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    # nixpkgs.url = "https://github.com/NixOS/nixpkgs/tarball/nixos-24.11";
  };

  outputs = { self, nixpkgs }: {
    devShells.x86_64-linux = 
      let 
        pkgs = import nixpkgs { system = "x86_64-linux"; config = {}; overlays = []; };
      in 
      {
        default = pkgs.mkShell {
          packages = with pkgs; [
            meson
            ninja
            pkg-config 
            clang
            bison 
            flex 
            nlohmann_json
            readline
            nodejs_20
            nodePackages.npm
            (
              pkgs.python3.withPackages (
                python-pkgs: [
                  python-pkgs.websockets
                ]
              )
            )
          ];

          shellHook = ''
            export CC=${pkgs.clang}/bin/clang
            export CXX=${pkgs.clang}/bin/clang++
            export ASAN_SYMBOLIZER_PATH=${pkgs.clang}/bin/llvm-symbolizer
            export ASAN_OPTIONS=detect_leaks=0:abort_on_error=1:disable_coredump=0
          '';
        };
      };

    packages.x86_64-linux = 
      let 
        pkgs = import nixpkgs { system = "x86_64-linux"; config = {}; overlays = []; };
      in  
      {
        default = pkgs.stdenv.mkDerivation {
          name = "easydb";
          src = ./.;
          nativeBuildInputs = with pkgs; [ meson ninja pkg-config clang ];
          buildInputs = with pkgs; [ flex bison nlohmann_json readline gtest ];

          mesonFlags = [
            "--native-file=${./meson-clang.ini}"
            "--buildtype=debug"
            "-Deasydb_enable_asan=true"
          ];

          installPhase = ''
            runHook preInstall
            mkdir -p $out/
            mkdir -p $out/bin
            cp easydb_server easydb_client $out/bin/
            # 复制 compile_commands.json 到输出目录，方便 clangd 使用
            if [ -f compile_commands.json ]; then
              cp compile_commands.json $out/
            fi
            runHook postInstall
          '';
        };
      };
  };
}
