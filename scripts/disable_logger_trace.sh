#!/bin/bash


##################################################################################################
# This script removes all logger trace calls within DXRAM if executed from DXRAM main directory. #
# For best performance, use disable_logger.sh, instead.
# To re-enable logger execute enable_logger.sh                                                   #
##################################################################################################


find ../src/ -name "*.java" -print | while read file
do
  echo "Removing logger calls from $file"
  # Search for LoggerComponent attribute and store name of reference
  var_name=$(cat $file | grep "LoggerComponent " | grep ";" | sed -e "s/.*LoggerComponent[ \t]*\([a-zA-Z0-9_-]*\).*/\1/")

  # Search for LoggerService attribute and store name of reference (previous search unsuccessful)
  if [ -z "$var_name" ]; then
    var_name=$(cat $file | grep "LoggerService " | grep ";" | sed -e "s/.*LoggerService[ \t]*\([a-zA-Z0-9_-]*\).*/\1/")
  fi

  # Search for LoggerInterface attribute and store name of reference (previous searches unsuccessful)
  if [ -z "$var_name" ]; then                                                                                                                                         
    var_name=$(cat $file | grep "LoggerInterface " | grep ";" | sed -e "s/.*LoggerInterface[ \t]*\([a-zA-Z0-9_-]*\).*/\1/")
  fi

  #  Search for LoggerComponent within methods and store name of reference (previous searches unsuccessful)
  #  TODO: This only works if for all methods in this class the LoggerComponent parameter is named equally (beginning with "p_")
  if [ -z "$var_name" ]; then
    var_name=$(cat $file | grep "LoggerComponent p_"  | sed -e "s/.*LoggerComponent[ \t]*\([a-zA-Z0-9_-]*\).*/\1/" | head -n 1)
  fi

  #  Store menet specific reference (previous searches unsuccessful)
  if [ -z "$var_name" ] && [[ {$file} == *"menet"* ]]; then
    var_name="NetworkHandler.getLogger()"
  fi

  #  Store log.header specific reference (previous searches unsuccessful)
  if [ -z "$var_name" ] && [[ {$file} == *"log/header"* ]]; then
    var_name="AbstractLogEntryHeader.getLogger()"
  fi
  
  if [ -n "$var_name" ]; then
    echo "Reference name is $var_name"
    newFile="$file.aux"
    rm -f $newFile
    touch $newFile

    commentNextLine=false
    IFS=''
    while read -r line; do
      if $commentNextLine
      then
        if [[ {$line} == *";"* ]];
        then
          commentNextLine=false
        fi
        echo "//delbysc$line" >> $newFile
        continue
      fi
 
      if [[ {$line} == *"$var_name.trace"* ]]
      then
        if [[ {$line} != *";"* ]];
        then
          commentNextLine=true
        fi
        echo "//delbysc$line" >> $newFile
        continue
      fi
      echo "$line" >> $newFile
    done < $file
    mv $newFile $file
  fi
done
