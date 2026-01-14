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
            cmake 
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
          nativeBuildInputs = with pkgs; [ cmake pkg-config clang ];
          buildInputs = with pkgs; [ flex bison nlohmann_json readline ];

          cmakeFlags = [
            "-DCMAKE_C_COMPILER=${pkgs.clang}/bin/clang"
            "-DCMAKE_CXX_COMPILER=${pkgs.clang}/bin/clang++"
            "-DCMAKE_BUILD_TYPE=Debug"
            "-DEASYDB_ENABLE_ASAN=ON"
          ];

          installPhase = ''
            runHook preInstall
            mkdir -p $out/
            cp -r bin $out/
            cp -r test $out/
            runHook postInstall
          '';
        };
      };
  };
}
