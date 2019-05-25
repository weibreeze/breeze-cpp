#!/usr/bin/env bash
set -x -e

PWD_PATH=`pwd`
DEPS_PREFIX=${PWD_PATH}/third

mkdir -p ${DEPS_PREFIX} || true
cd ${DEPS_PREFIX}
git clone https://github.com/google/googletest
cd googletest
mkdir build || true
cd build
cmake ..
make
sudo make install
make clean