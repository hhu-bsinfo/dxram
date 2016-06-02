#!/bin/bash


######################################################################################
# Disables all calls that do not match the configuration.                            #
# Is called in build script.                                                         #
######################################################################################


# Read parameters
if [ "$#" -ne 2 ];
then
  LOGGER_LEVEL="INFO"
  STATISTICS="ENABLED"
  echo "Using default parameters (LOGGER_LEVEL=INFO; STATISTICS=ENABLED)"
else
  LOGGER_LEVEL="$1"
  STATISTICS="$2"
  echo "Applying given parameters (LOGGER_LEVEL=$1; STATISTICS=$2)"
fi


##########
# LOGGER #
########## 
# Determine logger command
if [ "$LOGGER_LEVEL" == "TRACE" ];
then
  # Nothing to do!
  logger_cmd="cat"
elif [ "$LOGGER_LEVEL" == "DEBUG" ];
then
  # Remove logger trace calls
  logger_cmd="sed '/#if[ \t]*LOGGER[ \t]*==[ \t]*TRACE/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*==[ \t]*TRACE/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /}'"
elif [ "$LOGGER_LEVEL" == "INFO" ];
then
  # Remove logger trace and debug calls
  logger_cmd="sed '/#if[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\)/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\)/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /}'"
elif [ "$LOGGER_LEVEL" == "WARN" ];
then
  # Remove logger trace, debug and info calls
  logger_cmd="sed '/#if[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\)/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\)/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /}'"
elif [ "$LOGGER_LEVEL" == "ERROR" ];
then
  # Remove logger trace, debug, info and warn calls
  logger_cmd="sed '/#if[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\|>=[ \t]*WARN\)/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\|>=[ \t]*WARN\)/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /}'"
elif [ "$LOGGER_LEVEL" == "DISABLED" ];
then
  # Remove all logger calls
  logger_cmd="sed '/#if[ \t]*LOGGER/,/#endif[ \t]*\/\*[ \t]*LOGGER/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /}'"
else
  # Invalid!
  echo "Option $LOGGER_LEVEL not available. Choose between DISABLED, ERROR, INFO, DEBUG and TRACE."
  exit -1
fi


##############
# STATISTICS #
##############
# Determine statistics command
if [ "$STATISTICS" == "ENABLED" ];
then
  # Nothing to do!
  statistics_cmd="cat"
elif [ "$STATISTICS" == "DISABLED" ];
then
  # Remove logger trace calls
  statistics_cmd="sed '/#ifdef[ \t]*STATISTICS/,/#endif[ \t]*\/\*[ \t]*STATISTICS/{/#ifdef/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /}'"
else
  # Invalid!
  echo "Option $STATISTICS not available. Choose between ENABLED and DISABLED."
  exit -1
fi
   

find ../src/ -name "*.java" -print | while read input
do
  ##########
  # LOGGER #
  ##########  
  eval "$logger_cmd $input > \"$input.preprocessed\""
  
  ##############
  # STATISTICS #
  ##############
  eval "$statistics_cmd \"$input.preprocessed\" > $input"
  
  rm "$input.preprocessed"

done