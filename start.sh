#!/usr/bin/env bash
set -x -e

DEPS_PREFIX=$(pwd)/third-party
mkdir -p "${DEPS_PREFIX}/src"

install_libraries() {
  # 安装gtest
  if [ "$1"x == "gtest"x ] || [ "$2"x == "gtest"x ]; then
    cd "${DEPS_PREFIX}"/src
    git clone https://github.com/google/googletest
    cd googletest
    cmake .
    make
    sudo cp lib/libgtest*.a  /usr/lib
    sudo cp -a googletest/include/gtest/ /usr/include
    cd -
  fi

  # 安装boost
  if [ "$1"x == "boost"x ] || [ "$2"x == "boost"x ]; then
    cd "${DEPS_PREFIX}"/src
    if [ ! -f boost_1_71_0.tar.gz ]; then
      wget http://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.gz
    fi
    tar zxvf boost_1_71_0.tar.gz
    cd boost_1_71_0
    ./bootstrap.sh --with-libraries=all --with-toolset=gcc
    ./b2 toolset=gcc
    ./b2 install --prefix=/usr/local
    ldconfig
    cd -
  fi
}

usage() {
  echo '
  install：安装依赖库，后跟依赖库名，多个依赖库用空格分割
  示例:
  ./start.sh install boost gtest                  安装boost和gtest'
}

if [ $# == 0 ]; then
  usage
  exit 1
fi

case $1 in
install)
  install_libraries $2 $3
  ;;
*)
  echo "Unknown command $1"
  usage
  ;;
esac
