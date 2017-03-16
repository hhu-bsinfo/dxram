#!/bin/bash

BIN=$(pwd)/script/deploy/bin
BIN_HILBERT=$(pwd)/script/hilbert/bin

export PATH=${PATH}:$BIN:$BIN_HILBERT

echo "DXRAM environment configured"
bash
