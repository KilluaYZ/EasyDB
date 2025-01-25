{ 
  stdenv, 
  cmake, 
  pkg-config, 
  clang, 
  bison, 
  flex, 
  nlohmann_json, 
  readline
}:
stdenv.mkDerivation {
  name = "easydb";
  src = ./.;
  nativeBuildInputs = [ cmake pkg-config clang ];
  buildInputs = [ flex bison nlohmann_json readline ];

  cmakeFlags = [
    "-DCMAKE_C_COMPILER=${clang}/bin/clang"
    "-DCMAKE_CXX_COMPILER=${clang}/bin/clang++"
  ];

  installPhase = ''
    runHook preInstall
    mkdir -p $out/
    cp -r bin $out/
    cp -r test $out/
    runHook postInstall
  '';
}