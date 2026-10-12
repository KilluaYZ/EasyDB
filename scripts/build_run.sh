#!/bin/zsh
export ASAN_OPTIONS=detect_leaks=0
CUR_DIR=`readlink -f $0`
SCRIPT_DIR=`dirname $CUR_DIR`
ROOT_DIR=`dirname $SCRIPT_DIR`
echo "[INFO] Create dir $ROOT_DIR/build"
rm -rf $ROOT_DIR/build
mkdir $ROOT_DIR/build
cd $ROOT_DIR/build
echo "[INFO] Build"
meson setup . "$ROOT_DIR" \
  --native-file "$ROOT_DIR/meson-clang.ini" \
  -Deasydb_enable_asan=true
meson compile -j
echo "[INFO] Run"
./comprehensive_test
