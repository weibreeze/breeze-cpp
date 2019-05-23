#!/usr/bin/env bash
set -x -e

DEPS_PREFIX=`pwd`/third

cd ${DEPS_PREFIX}
tar xvf googletest-release-1.8.1.tar.gz
cd googletest-release-1.8.1
mkdir build-gtest
cd build-gtest
cmake ..
make
make install
make clean
cd -
