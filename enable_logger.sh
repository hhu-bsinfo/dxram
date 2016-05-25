#!/bin/bash


#####################################
# This script re-enables the logger #
#####################################


find src/ -name "*.java" -print | while read file
do
  sed -i "s/\/\/delbysc//g" $file
  sed -i "s/\/\/ delbysc//g" $file
done
