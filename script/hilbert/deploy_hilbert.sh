#!/bin/bash

DEPLOY_CONFIG=""

HILBERT_NODE_COUNT=""
HILBERT_CPUS_PER_NODE=""
HILBERT_MEM_PER_NODE_GB=""
HILBERT_WALL_TIME=""
HILBERT_JOB_NAME=""

TMP=/scratch_gs/$USER/dxram_tmp
TEMPLATE=dxram_template.job

JOB_FILE_PATH=""
DEPLOY_CONFIG_PATH=""

bootstrap() 
{
	if [ ! -e "$TMP" ]; then
		mkdir $TMP
	fi
}

assert_paths_absolute()
{
  	local tmp=`echo "$DEPLOY_CONFIG" | grep DXRAM_PATH`
  	if [ "$tmp" = "" ] ; then
    	echo "Missing DXRAM_PATH in deploy config"
		exit -1
	fi

	if [[ "${tmp:0:1}" = "/" ]]; then
		echo "DXRAM_PATH must be absolute on hilbert"
		exit -1
	fi

  	local tmp=`echo "$DEPLOY_CONFIG" | grep ZOOKEEPER_PATH`
  	if [ "$tmp" = "" ] ; then
    	echo "Missing ZOOKEEPER_PATH in deploy config"
		exit -1
	fi

	if [[ "${tmp:0:1}" = "/" ]]; then
		echo "ZOOKEEPER_PATH must be absolute on hilbert"
		exit -1
	fi
}

determine_hilbert_parameters() 
{
  	local tmp=`echo "$DEPLOY_CONFIG" | grep HILBERT_NODE_COUNT`
  	if [ "$tmp" = "" ] ; then
    	echo "Missing HILBERT_NODE_COUNT in deploy config"
		exit -1
	fi
	HILBERT_NODE_COUNT=`echo "$tmp" | cut -d '=' -f 2`
  
  	local tmp=`echo "$DEPLOY_CONFIG" | grep HILBERT_CPUS_PER_NODE`
  	if [ "$tmp" = "" ] ; then
    	echo "Missing HILBERT_CPUS_PER_NODE in deploy config"
		exit -1
	fi
	HILBERT_CPUS_PER_NODE=`echo "$tmp" | cut -d '=' -f 2`

  	local tmp=`echo "$DEPLOY_CONFIG" | grep HILBERT_MEM_PER_NODE_GB`
  	if [ "$tmp" = "" ] ; then
    	echo "Missing HILBERT_MEM_PER_NODE_GB in deploy config"
		exit -1
	fi
	HILBERT_MEM_PER_NODE_GB=`echo "$tmp" | cut -d '=' -f 2`

  	local tmp=`echo "$DEPLOY_CONFIG" | grep HILBERT_WALL_TIME`
  	if [ "$tmp" = "" ] ; then
    	echo "Missing HILBERT_WALL_TIME in deploy config"
		exit -1
	fi
	HILBERT_WALL_TIME=`echo "$tmp" | cut -d '=' -f 2`

  	local tmp=`echo "$DEPLOY_CONFIG" | grep HILBERT_JOB_NAME`
  	if [ "$tmp" = "" ] ; then
    	echo "Missing HILBERT_JOB_NAME in deploy config"
		exit -1
	fi
	HILBERT_JOB_NAME=`echo "$tmp" | cut -d '=' -f 2`

  	# Trim config file, remove hilbert parameters
  	DEPLOY_CONFIG=`echo "$DEPLOY_CONFIG" | grep -v 'HILBERT_NODE_COUNT'`
  	DEPLOY_CONFIG=`echo "$DEPLOY_CONFIG" | grep -v 'HILBERT_CPUS_PER_NODE'`
	DEPLOY_CONFIG=`echo "$DEPLOY_CONFIG" | grep -v 'HILBERT_MEM_PER_NODE_GB'`
	DEPLOY_CONFIG=`echo "$DEPLOY_CONFIG" | grep -v 'HILBERT_WALL_TIME'`
	DEPLOY_CONFIG=`echo "$DEPLOY_CONFIG" | grep -v 'HILBERT_JOB_NAME'`
}

create_hilbert_deployable_job()
{
	# Job template
	JOB_FILE_PATH="dxram_${HILBERT_JOB_NAME}_${HILBERT_NODE_COUNT}_${HILBERT_CPUS_PER_NODE}.job"
	echo "Creating deployable hilbert job: $JOB_FILE_PATH"

	JOB_FILE_PATH="$TMP/$JOB_FILE_PATH"
	cp $TEMPLATE $JOB_FILE_PATH

	# Config file
	DEPLOY_CONFIG_PATH="dxram_${HILBERT_JOB_NAME}_${HILBERT_NODE_COUNT}_${HILBERT_CPUS_PER_NODE}.conf"
	echo "Deploy config for hilbert job: $DEPLOY_CONFIG_PATH"

	DEPLOY_CONFIG_PATH="$TMP/$DEPLOY_CONFIG_PATH"

	if [ -e $DEPLOY_CONFIG_PATH ]; then
		rm $DEPLOY_CONFIG_PATH
	fi

	touch $DEPLOY_CONFIG_PATH
	# Use trimmed config
	echo "$DEPLOY_CONFIG" >> $DEPLOY_CONFIG_PATH

	# Replace placeholders in job template
	sed -i "s/%%%HILBERT_NODE_COUNT%%%/$HILBERT_NODE_COUNT/g" $JOB_FILE_PATH
	sed -i "s/%%%HILBERT_CPUS_PER_NODE%%%/$HILBERT_CPUS_PER_NODE/g" $JOB_FILE_PATH
	sed -i "s/%%%HILBERT_MEM_PER_NODE_GB%%%/$HILBERT_MEM_PER_NODE_GB/g" $JOB_FILE_PATH
	sed -i "s/%%%HILBERT_WALL_TIME%%%/$HILBERT_WALL_TIME/g" $JOB_FILE_PATH
	sed -i "s/%%%HILBERT_JOB_NAME%%%/$HILBERT_JOB_NAME/g" $JOB_FILE_PATH

	sed -i "s#%%%DXRAM_DEPLOY_CONFIG_PATH%%%#${DEPLOY_CONFIG_PATH}#g" $JOB_FILE_PATH
}

submit_hilbert_job()
{
	echo "Submitting generated job: $JOB_FILE_PATH"
	qsub $JOB_FILE_PATH
}

###############
# Entry point #
###############

if [ "$1" = "" ] ; then
  echo "Missing parameter: Configuration file"
  echo "  Example: $0 SimpleHilbertTest.conf"
  exit
fi

node_file="./$1"
if [ "${node_file: -5}" != ".conf" ] ; then
  node_file="${node_file}.conf"
fi

# Trim node file
DEPLOY_CONFIG=`cat "$node_file" | grep -v '#' | sed 's/, /,/g' | sed 's/,\t/,/g'`

bootstrap
assert_paths_absolute
determine_hilbert_parameters
create_hilbert_deployable_job
submit_hilbert_job
