#!/bin/bash

BUILD_TYPE="release"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -gt 0 ]; then
  BUILD_TYPE="$1"
fi

ant -buildfile ant/dxram-build.xml -Dbasedir=$SCRIPT_DIR -Dbuild=../build/classes $BUILD_TYPE
	
