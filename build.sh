#!/bin/bash

set -e

usage() {
  echo "Usage: $0 [type] [-d destination]"
  exit 1
}

DEST=/home/$USER
BUILD_TYPE="release"

while getopts d: option
do
  case "${option}"
  in
    d) DEST=${OPTARG};;
  esac
done

shift $((OPTIND-1))

if [ "$1" ]; then
    BUILD_TYPE="$1"
fi

if [ -z "${DEST}" ] || [ -z "${BUILD_TYPE}" ]; then
    usage
fi

mkdir -p "${DEST}/dxram"

./gradlew installDist -PoutputDir="${DEST}" -PbuildVariant="${BUILD_TYPE}"
