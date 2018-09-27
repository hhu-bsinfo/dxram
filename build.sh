#!/bin/bash

build_type="release"

if [ "$1" ]; then
    build_type="$1"
fi

clean=""

case "$build_type" in
    debug)
        ;;
    release)
        ;;
    performance)
        ;;
    clean)
        clean="1"        
        ;;
    *)
        echo "Invalid build type \"$build_type\" specified"
        exit -1
esac

echo "Build type: $build_type"

if [ ! "$clean" ]; then
    TERM=dumb ./gradlew distZip -PbuildVariant="$build_type"
else
    TERM=dumb ./gradlew cleanAll
fi 
