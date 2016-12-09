#!/bin/bash

BUILD_TYPE="release"

if [ "$#" -gt 0 ]; then
  BUILD_TYPE="$1"
fi

ant -buildfile dxram-build.xml -Dbuild=build/classes $BUILD_TYPE
	
