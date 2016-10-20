#!/bin/bash


############################################################################
# Re-enables all calls removed by preprocessor. Is called in build script. #
############################################################################


# Determine logger command
logger_cmd="sed -e '/#if[ \t]*LOGGER/,/#endif[ \t]*\/\*[ \t]*LOGGER/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)\/\/ /\1/' -e '}'"

# Determine statistics command
statistics_cmd="sed -e '/#ifdef[ \t]*STATISTICS/,/#endif[ \t]*\/\*[ \t]*STATISTICS/{/#ifdef/n;/#endif/"'!'"s/^\([ \t]*\)\/\/ /\1/' -e '}'"


find ../src/ -name "*.java" -print | while read input
do  
  eval "$logger_cmd $input > \"$input.postprocessed\""
  eval "$statistics_cmd \"$input.postprocessed\" > $input"
  rm "$input.postprocessed"
done