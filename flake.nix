{
  description = "flake for easydb";

  inputs = {
    nixpkgs.url = "https://github.com/NixOS/nixpkgs/tarball/nixos-24.11";
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
          ];

          shellHook = ''
            export CC=${pkgs.clang}/bin/clang
            export CXX=${pkgs.clang}/bin/clang++
          '';
        };
      };

    packages.x86_64-linux = 
      let 
        pkgs = import nixpkgs { system = "x86_64-linux"; config = {}; overlays = []; };
      in  
      {
        default = pkgs.callPackage ./easydb.nix {};
      };
  };
}
