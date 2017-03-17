#!/bin/bash

DIR="$(cd "$(dirname "$0")"; pwd)"
echo $DIR
BIN=$DIR/script/deploy/bin
SCRIPTS=$DIR/script/deploy
SCRIPTS_MODS=$DIR/script/deploy/modules
BIN_HILBERT=$DIR/script/hilbert/bin
HILBERT_SCRIPTS=$DIR/script/hilbert

# Make all scripts executable
chmod +x $BIN/*
chmod +x $SCRIPTS/*
chmod +x $SCRIPTS_MODS/*
chmod +x $BIN_HILBERT/*
chmod +x $HILBERT_SCRIPTS/*

export PATH=${PATH}:$BIN:$BIN_HILBERT

echo "DXRAM environment configured"
bash
