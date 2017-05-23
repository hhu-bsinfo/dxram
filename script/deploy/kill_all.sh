#!/bin/bash

######################################################
# Check which shell is running and bash version
# Globals:
# Arguments:
######################################################
check_shell() 
{
	if [ "$(echo $SHELL | grep "bash")" = "" ]; then
		echo "ERROR: Current shell not supported by deploy script, bash only"
		exit -1
	fi

	# Some features like "declare -A" require version 4
	if [ $(echo ${BASH_VERSION%%[^0-9]*}) -lt 4 ]; then
	    read versionCheck <<< $(echo ${BASH_VERSION%%[^0-9]* } | awk -F '.' '{split($3, a, "("); print a[1]; print ($1 >= 3 && $2 > 2) ? "YES" : ($2 == 2 && a[1] >= 57) ? "YES" : "NO" }')
        if [ "$versionCheck" == "NO" ]; then
		    echo "ERROR: Bash version >= 3.2.57 required (Recommended is version 4)"
		    exit -1
		fi
	fi
}

######################################################
# Check if all neccessary programs are installed
# Globals:
# Arguments:
#   node_file - The configuration file
######################################################
check_programs() 
{
	local node_file=$1

	if [ ! hash cat 2>/dev/null ]; then
		echo "Please install coreutils. Used for cat, cut and readlink. Exiting..."
		exit
	fi

	if [ ! hash grep 2>/dev/null ]; then
		echo "Please install grep. Exiting..."
		exit
	fi

	if [ ! hash sed 2>/dev/null ]; then
		echo "Please install sed. Exiting..."
		exit
	fi

	if [ ! hash hostname 2>/dev/null ]; then
		echo "Please install hostname. Exiting..."
		exit
	fi

	if [ ! hash pkill 2>/dev/null ]; then
		echo "Please install procps. Used for pkill. Exiting..."
		exit
	fi

	if [ ! hash host 2>/dev/null ]; then
		echo "Please install bind9-host. Used for host. Exiting..."
		exit
	fi

	if [ ! hash getent 2>/dev/null ]; then
		echo "Please install libc-bin. Used for getent. Exiting..."
		exit
	fi

	if [ ! hash ssh 2>/dev/null ]; then
		echo "Please install openssh-client. Used for scp and ssh. Exiting..."
		exit
	fi
}

######################################################
# Resolve hostname to IP
# Globals:
# Arguments:
#   hostname - The hostname to resolve
# Return:
#   ip - The IP address
######################################################
resolve() 
{
	local hostname=$1
	local ip=""

	ip=`host $hostname | cut -d ' ' -f 4 | grep -E "[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"`
	if [ "$ip" = "" ]; then
		ip=`getent hosts $hostname | cut -d ' ' -f 1 | grep -E "[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"`
		if [ "$ip" = "" ]; then
			ip="127.0.01"
		fi
	fi
	echo "$ip"
}

######################################################
# Close all instances
# Globals:
#   NODES
#   LOCALHOST
#	THIS_HOST
# Arguments:
#   None
######################################################
close() 
{
	echo "Closing all dxram instances..."
	local node=""

	while read node || [[ -n "$node" ]]; do
		# Skip empty lines	
		if [ "$node" = "" ]; then
			continue
		fi

		local hostname=`echo $node | cut -d ',' -f 1`
		local role=`echo $node | cut -d ',' -f 2`
		local ip=`resolve $hostname`

		if [ "$role" = "Z" ]; then
			# Stop ZooKeeper?
			echo "ZooKeeper might stay alive"
		else
			echo "Killing instance on $ip..."
			if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ]; then
				pkill -9 -f "de.hhu.bsinfo.dxram.DXRAM"
			else
				ssh $hostname -n "pkill -9 -f de.hhu.bsinfo.dxram.DXRAM"
			fi
		fi
	done <<< "$NODES"

	echo "Exiting..."
	exit
}

###############
# Entry point #
###############

check_shell

if [ "$1" = "" ]; then
	echo "Missing parameter: Configuration file"
	echo "  Example: $0 SimpleTest.conf"
	exit
fi

node_file="./$1"
if [ "${node_file: -5}" != ".conf" ]; then
	node_file="${node_file}.conf"
fi

check_programs "$node_file"

# Trim node file
NODES=`cat "$node_file" | grep -v '#' | sed 's/, /,/g' | sed 's/,\t/,/g'`
NODES=`echo "$NODES" | grep -v 'DXRAM_PATH'`
NODES=`echo "$NODES" | grep -v 'ZOOKEEPER_PATH'`

# Set default values
readonly LOCALHOST=`resolve "localhost"`
if [ `echo $LOCALHOST | cut -d "." -f 1` != "127" ]; then
	echo "Illegal loopback device (ip: $LOCALHOST). Exiting..."
	exit
fi
readonly THIS_HOST=`resolve $(hostname)`

echo "########################################"
echo "Killing all DXRAM instances of $(echo $1 | cut -d '.' -f 1)"
echo "########################################"
echo -e "\n\n"

close
