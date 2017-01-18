#!/bin/bash


######################################################################################
# Disables all calls that do not match the configuration.                            #
# Called in build script.                                                            #
######################################################################################


# Read parameters
if [ "$#" -ne 3 ];
then
  LOGGER_LEVEL="INFO"
  STATISTICS="ENABLED"
  ASSERT_NODE_ROLE="ENABLED"
  echo "Using default parameters (LOGGER_LEVEL=INFO; STATISTICS=ENABLED; ASSERT_NODE_ROLE=ENABLED)"
else
  LOGGER_LEVEL="$1"
  STATISTICS="$2"
  ASSERT_NODE_ROLE="$3"
  echo "Applying given parameters (LOGGER_LEVEL=$1; STATISTICS=$2; ASSERT_NODE_ROLE=$3)"
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
  logger_cmd="sed -e '/#if[ \t]*LOGGER[ \t]*==[ \t]*TRACE/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*==[ \t]*TRACE/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /' -e '}'"
elif [ "$LOGGER_LEVEL" == "INFO" ];
then
  # Remove logger trace and debug calls
  logger_cmd="sed -e '/#if[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\)/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\)/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /' -e '}'"
elif [ "$LOGGER_LEVEL" == "WARN" ];
then
  # Remove logger trace, debug and info calls
  logger_cmd="sed -e '/#if[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\)/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\)/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /' -e '}'"
elif [ "$LOGGER_LEVEL" == "ERROR" ];
then
  # Remove logger trace, debug, info and warn calls
  logger_cmd="sed -e '/#if[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\|>=[ \t]*WARN\)/,/#endif[ \t]*\/\*[ \t]*LOGGER[ \t]*\(==[ \t]*TRACE\|>=[ \t]*DEBUG\|>=[ \t]*INFO\|>=[ \t]*WARN\)/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /' -e '}'"
elif [ "$LOGGER_LEVEL" == "DISABLED" ];
then
  # Remove all logger calls
  logger_cmd="sed -e '/#if[ \t]*LOGGER/,/#endif[ \t]*\/\*[ \t]*LOGGER/{/#if/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /' -e '}'"
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
  statistics_cmd="sed -e '/#ifdef[ \t]*STATISTICS/,/#endif[ \t]*\/\*[ \t]*STATISTICS/{/#ifdef/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /' -e '}'"
else
  # Invalid!
  echo "Option $STATISTICS not available. Choose between ENABLED and DISABLED."
  exit -1
fi


####################
# ASSERT_NODE_ROLE #
####################
# Determine assert node role command
if [ "$ASSERT_NODE_ROLE" == "ENABLED" ];
then
  # Nothing to do!
  assert_node_role_cmd="cat"
elif [ "$ASSERT_NODE_ROLE" == "DISABLED" ];
then
  # Remove
  assert_node_role_cmd="sed -e '/#ifdef[ \t]*ASSERT_NODE_ROLE/,/#endif[ \t]*\/\*[ \t]*ASSERT_NODE_ROLE/{/#ifdef/n;/#endif/"'!'"s/^\([ \t]*\)/\1\/\/ /' -e '}'"
else
  # Invalid!
  echo "Option $ASSERT_NODE_ROLE not available. Choose between ENABLED and DISABLED."
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

  ####################
  # ASSERT_NODE_ROLE #
  ####################
  eval "$assert_node_role_cmd \"$input.preprocessed\" > $input"

  rm "$input.preprocessed"

done
