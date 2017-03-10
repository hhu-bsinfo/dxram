#!/bin/bash

#############
# Functions #
#############

######################################################
# Check which shell is running and bash version
# Globals:
# Arguments:
######################################################
check_shell() {
	if [ "$(echo $SHELL | grep "bash")" = "" ] ; then
		echo "ERROR: Current shell not supported by deploy script, bash only"
		exit -1
	fi

	# Some features like "declare -A" require version 4
	if [ $(echo ${BASH_VERSION%%[^0-9]*}) -lt 4 ]; then
		echo "ERROR: Bash version >= 4 required"
		exit -1
	fi
}

######################################################
# Check if all neccessary programs are installed
# Globals:
# Arguments:
#   node_file - The configuration file
######################################################
check_programs() {
  local node_file=$1

  if ! hash cat 2>/dev/null ; then
    echo "Please install coreutils. Used for cat, cut, mkdir, readlink, rm and sleep. Exiting..."
    exit
  fi
  if ! hash grep 2>/dev/null ; then
    echo "Please install grep. Exiting..."
    exit
  fi
  if ! hash sed 2>/dev/null ; then
    echo "Please install sed. Exiting..."
    exit
  fi
  if ! hash hostname 2>/dev/null ; then
    echo "Please install hostname. Exiting..."
    exit
  fi
  if ! hash pkill 2>/dev/null ; then
    echo "Please install procps. Used for pkill. Exiting..."
    exit
  fi
  if ! hash host 2>/dev/null ; then
    echo "Please install bind9-host. Used for host. Exiting..."
    exit
  fi
  if ! hash dig 2>/dev/null ; then
    echo "Please install dnsutils. Used for dig. Existing..."
    exit
  fi
  if ! hash ssh 2>/dev/null ; then
    echo "Please install openssh-client. Used for scp and ssh. Exiting..."
    exit
  fi
  if ! hash java 2>/dev/null ; then
    if [ "`cat $node_file | grep localhost`" != "" ] ; then
      echo "Please install Java 8 for local execution of DXRAM. Exiting..."
      exit
    fi
  fi
}

######################################################
# Read paths from configuration or set default values
# Globals:
#   DXRAM_PATH
#   ZOOKEEPER_PATH
#   NODES
# Arguments:
#   None
######################################################
determine_configurable_paths() {
  local tmp=`echo "$NODES" | grep DXRAM_PATH`
  if [ "$tmp" != "" ] ; then
    local dxram_path=`echo "$tmp" | cut -d '=' -f 2`

    if [[ "$dxram_path" = /* || "${dxram_path:0:1}" = "~" ]]; then
       readonly DXRAM_PATH=$dxram_path
    else
		readonly DXRAM_PATH="$(cd "${NODE_FILE_DIR}$dxram_path"; pwd)/"
    fi

    echo "DXRAM root folder path: $DXRAM_PATH"
  else
    readonly DXRAM_PATH="~/dxram/"
    echo "DXRAM root folder path undefined. Using default: $DXRAM_PATH"
  fi

  tmp=`echo "$NODES" | grep ZOOKEEPER_PATH`
  if [ "$tmp" != "" ] ; then
    local zookeeper_path=`echo "$tmp" | cut -d '=' -f 2`

    if [[ "$zookeeper_path" = /* || "${zookeeper_path:0:1}" = "~" ]]; then
       readonly ZOOKEEPER_PATH=$zookeeper_path
    else
		readonly ZOOKEEPER_PATH="$(cd "${NODE_FILE_DIR}$zookeeper_path"; pwd)/"
    fi

    echo "ZooKeeper root folder path: $ZOOKEEPER_PATH"
  else
    readonly ZOOKEEPER_PATH="~/zookeeper/"
    echo "ZooKeeper root folder path undefined. Using default: $ZOOKEEPER_PATH"
  fi

  # Trim node file, remove paths at top
  NODES=`echo "$NODES" | grep -v 'DXRAM_PATH'`
  NODES=`echo "$NODES" | grep -v 'ZOOKEEPER_PATH'`
}

######################################################
# Remove file/directories from last execution
# Globals:
#   EXECUTION_DIR
# Arguments:
#   None
######################################################
clean_up() {
  rm -rf $DEPLOY_TMP_DIR

  mkdir $DEPLOY_TMP_DIR
  mkdir $LOG_DIR
}

######################################################
# Check DXRAM configuration file, generate it if it is missing or obviously corrupted
# Globals:
#   CONFIG_FILE
#   DXRAM_PATH
#   DEFAULT_CLASS
#   EXECUTION_DIR
# Arguments:
#   None
######################################################
check_configuration() {
  local config_content=`cat "$CONFIG_FILE" 2> /dev/null`
  if [ "$config_content" = "" ] ; then
    # There is no configuration file -> start dxram once to create configuration
    if ! hash java 2>/dev/null ; then
      echo "DXRAM configuration was not found and new configuration cannot be created as Java 8 is not installed!"
      exit
    fi

    cd "$DXRAM_PATH"
    java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -cp $LIBRARIES $DEFAULT_CLASS > /dev/null 2>&1
    echo -e "File not found: DXRAM configuration file was created\n"
    cd "$EXECUTION_DIR"
  else
    local component_header=`echo $config_content | grep "m_components"`
    local service_header=`echo $config_content | grep "m_services"`
    if [ "$component_header" = "" -o "$service_header" = "" ] ; then
      # Configuration file seems to be corrupted -> start dxram once to create new configuration
      if ! hash java 2>/dev/null ; then
	echo "DXRAM configuration is corrupted and new configuration cannot be created as Java 8 is not installed!"
	exit
      fi

      rm "$CONFIG_FILE"
      cd "$DXRAM_PATH"
      java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -cp $LIBRARIES $DEFAULT_CLASS > /dev/null 2>&1
      echo -e "File corruption: DXRAM configuration file was created\n"
      cd "$EXECUTION_DIR"
    fi
  fi
}

######################################################
# Write DXRAM configuration file with updated node and ZooKeeper information
# Update node table: Hostname is replaced by resolved ip and determined port
# Globals:
#   CONFIG_FILE
#   NODES
#   EXECUTION_DIR
# Arguments:
#   None
######################################################
write_configuration() {
  # Initialize hashtable for port determination
  declare -A NODE_ARRAY
  local current_port=0

  # Create replacement string for nodes configuration:
  local default_node="
        {
          \"m_address\": {
            \"m_ip\": \"IP_TEMPLATE\",
            \"m_port\": PORT_TEMPLATE
          },
          \"m_role\": \"ROLE_TEMPLATE\",
          \"m_rack\": 0,
          \"m_switch\": 0,
          \"m_readFromFile\": 1
        }"

  local current_config=`cat $CONFIG_FILE`
  local config_string=""
  local end=""
  local node=""
  local new_node=""
  local new_nodes=""
  local first_iterartion=true
  while read node || [[ -n "$node" ]]; do
    local hostname=`echo $node | cut -d ',' -f 1`
    local role=`echo $node | cut -d ',' -f 2`
    local ip=`resolve $hostname`
    if [ "$ip" = "" ] ; then
      echo "ERROR: Unknown host: \"$hostname\"."
      close
    fi
    local port="0"

    if [ "$role" = "Z" ] ; then
      port="$ZOOKEEPER_PORT"

      # Create replacement string for zookeeper configuration
      local zookeeper_config_string="
      \"m_path\": \"/dxram\",
      \"m_connection\": {
        \"m_ip\": \"$ip\",
        \"m_port\": $port
      },"

      # Replace zookeeper configuration
      current_config=`sed '/ZookeeperBootComponent/q' $CONFIG_FILE`
      current_config="$current_config$zookeeper_config_string"
      end=`sed -ne '/ZookeeperBootComponent/{s///; :a' -e 'n;p;ba' -e '}' $CONFIG_FILE`
      end=`echo "$end" | sed -ne '/},/{s///; :a' -e 'n;p;ba' -e '}'`
      current_config=`echo -e "$current_config\n$end"`

      # Replace hostname by ip and port in nodes table
      new_nodes=`echo "$node" | sed "s/\([a-zA-Z0-9\-\.]*\)/$ip,$port,\1/"`
      continue
    elif [ "$role" = "S" ] ; then
      current_port=${NODE_ARRAY["$hostname"]}
      if [ "$current_port" = "" ] ; then
		current_port=22221
      else
		current_port=$(($current_port + 1))
      fi
      port=$current_port
      NODE_ARRAY["$hostname"]=$current_port

      role="SUPERPEER"
    elif [ "$role" = "T" ] ; then
      port="22220"

      role="TERMINAL"
    else
	  echo "$hostname"
		echo $NODE_ARRAY
      current_port=${NODE_ARRAY["$hostname"]}
	echo "$current_port"
      if [ "$current_port" = "" ] ; then
		current_port=22222
      else
		current_port=$(($current_port + 1))
      fi
      port=$current_port
      NODE_ARRAY["$hostname"]=$current_port

      role="PEER"
	fi

    local node_string=`echo "$default_node" | sed "s/IP_TEMPLATE/$ip/" | sed "s/PORT_TEMPLATE/$port/" | sed "s/ROLE_TEMPLATE/$role/"`

    if [ "$first_iterartion" == true ] ; then
      config_string="$config_string$node_string"
      first_iterartion=false
    else
      config_string="$config_string,$node_string"
    fi

    # Replace hostname by ip and port in nodes table
    new_node=`echo "$node" | sed "s/\([a-zA-Z0-9\-\.]*\)/$ip,$port,\1/"`
    new_nodes=`echo -e "$new_nodes\n$new_node"`
  done <<< "$NODES"
  readonly NODES="$new_nodes"

  config_string=`echo -e "$config_string\n      ],"`

  # Replace nodes configuration:
  local new_config=`echo "$current_config" | sed '/m_nodesConfig/q'`
  new_config="$new_config$config_string"
  end=`echo "$current_config" | sed -ne '/m_nodesConfig/{s///; :a' -e 'n;p;ba' -e '}'`
  end=`echo "$end" | sed -ne '/],/{s///; :a' -e 'n;p;ba' -e '}'`
  new_config=`echo -e "$new_config\n$end"`

  echo "$new_config" > "${DEPLOY_TMP_DIR}dxram.json"
}

######################################################
# Copy DXRAM configuration to remote node
# Globals:
#   NFS_MODE
#   DXRAM_PATH
#   EXECUTION_DIR
# Arguments:
#   copied - Whether the remote config has to be copied
#   hostname - The hostname of the remote node
# Return:
#   copied - Whether the local config was copied
######################################################
copy_remote_configuration() {
    local copied=$1
    local hostname=$2

    if [ "$NFS_MODE" = false -o "$copied" = false ] ; then
      scp "${DEPLOY_TMP_DIR}dxram.json" "${hostname}:${DXRAM_PATH}config/"
      copied=true
    fi
    echo "$copied"
}

######################################################
# Copy DXRAM configuration for local execution
# Globals:
#   DXRAM_PATH
#   EXECUTION_DIR
# Arguments:
#   copied - Whether the local config has to be copied
# Return:
#   copied - Whether the local config was copied
######################################################
copy_local_configuration() {
  local copied=$1

  if [ "$copied" = false ] ; then
    cp ${DEPLOY_TMP_DIR}/dxram.json "${DXRAM_PATH}config/"
    copied=true
  fi
  echo "$copied"
}

######################################################
# Start ZooKeeper on remote node
# Globals:
#   ZOOKEEPER_PATH
# Arguments:
#   ip - The IP of the remote node
#   port - The port of the ZooKeeper server to start
#   hostname - The hostname of the ZooKeeper server
######################################################
start_remote_zookeeper() {
  local ip=$1
  local port=$2
  local hostname=$3

  ssh $hostname -n "cd $ZOOKEEPER_PATH && sed -i -e \"s/clientPort=[0-9]*/clientPort=$port/g\" \"conf/zoo.cfg\" && rm conf/zoo.cfg-e && bin/zkServer.sh start"
}

######################################################
# Start ZooKeeper locally
# Globals:
#   ZOOKEEPER_PATH
#   EXECUTION_DIR
# Arguments:
#   port - The port of the ZooKeeper server to start
######################################################
start_local_zookeeper() {
  local port=$1
  cd $ZOOKEEPER_PATH
  sed -i -e "s/clientPort=[0-9]*/clientPort=$port/g" conf/zoo.cfg
  # delete backup config file created by sed -e
  rm conf/zoo.cfg-e

  "bin/zkServer.sh" start
  cd "$EXECUTION_DIR"
}

######################################################
# Check ZooKeeper startup, exit on failure
# Globals:
#   LOG_DIR
#   ZOOKEEPER_PATH
#   EXECUTION_DIR
#	LOCALHOST
# 	THIS_HOST
# Arguments:
#   ip - The IP of the ZooKeeper server
#   port - The port of the ZooKeeper server
#   hostname - The hostname of the ZooKeeper server
######################################################
check_zookeeper_startup() {
  local ip=$1
  local port=$2
  local hostname=$3

  local logfile="${LOG_DIR}${hostname}_${port}_zookeeper_server"

  while true ; do
    local success_started=`cat "$logfile" 2> /dev/null | grep "STARTED"`
    local success_running=`cat "$logfile" 2> /dev/null | grep "already running"`
    local fail_file=`cat "$logfile" 2> /dev/null | grep "No such file or directory"`
    local fail_pid=`cat "$logfile" 2> /dev/null | grep "FAILED TO WRITE PID"`
    local fail_started=`cat "$logfile" 2> /dev/null | grep "SERVER DID NOT START"`
    if [ "$success_started" != "" -o "$success_running" != "" ] ; then
      echo "ZooKeeper ($ip $port) started"

      if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ] ; then
	# Remove all dxram related entries
	cd "$ZOOKEEPER_PATH"
	echo "rmr /dxram" | "bin/zkCli.sh" > "${LOG_DIR}${hostname}_${port}_zookeeper_client" 2>&1
	cd "$EXECUTION_DIR"
      else
	ssh $hostname -n "echo \"rmr /dxram\" | ${ZOOKEEPER_PATH}bin/zkCli.sh" > "${LOG_DIR}${hostname}_${port}_zookeeper_client" 2>&1
      fi

      while true ; do
	local success=`cat "${LOG_DIR}${hostname}_${port}_zookeeper_client" | grep "CONNECTED"`
	local fail=`cat "${LOG_DIR}${hostname}_${port}_zookeeper_client" | grep -i "exception"`
	if [ "$success" != "" ] ; then
	  echo "ZooKeeper clean-up successful."
	  break
	elif [ "$fail" != "" ] ; then
	  echo "ZooKeeper server not available."
	  close
	fi
      done

      break
    elif [ "$fail_file" != "" -o "$fail_pid" != "" -o "$fail_started" != "" ] ; then
      echo "ERROR: ZooKeeper ($ip $port) could not be started. See log file $logfile"
      close
    fi
    sleep 1.0
  done
}

######################################################
# Start all instances
# Globals:
#   LOCALHOST
#	THIS_HOST
#   LOG_DIR
#   NODES
#   DEPLOY_SCRIPT_DIR
# Arguments:
#   None
######################################################
execute() {
	local zookeeper_started=false
  	local local_config_was_copied=false
  	local remote_config_was_copied=false

    local zookeeper_ip=""
    local zookeeper_port=""

  	local node=""
  	local number_of_lines=`echo "$NODES" | wc -l`
  	local counter=1
  	while [  $counter -le $number_of_lines ]; do
		node=`echo "$NODES" | sed "${counter}q;d"`
		counter=$(($counter + 1))
		local ip=`echo $node | cut -d ',' -f 1`
		local port=`echo $node | cut -d ',' -f 2`
		local hostname=`echo $node | cut -d ',' -f 3`
		local role=`echo $node | cut -d ',' -f 4`
		local is_remote=false

		# Copy generated config once on localhost and for remote nodes to each node
		if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ] ; then
		    local_config_was_copied=`copy_local_configuration "$local_config_was_copied"`
		    is_remote=false
	  	else
		    remote_config_was_copied=`copy_remote_configuration "$remote_config_was_copied" "$hostname"`
		    is_remote=true
	  	fi

    	if [ "$role" = "Z" ] ; then
      		if [ "$zookeeper_started" = false ] ; then
				if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ] ; then
	  				start_local_zookeeper "$port" > "${LOG_DIR}${hostname}_${port}_zookeeper_server" 2>&1
	  				check_zookeeper_startup "$ip" "$port" "$hostname"
				else
	  				start_remote_zookeeper "$ip" "$port" "$hostname" > "${LOG_DIR}${hostname}_${port}_zookeeper_server" 2>&1
	  				check_zookeeper_startup "$ip" "$port" "$hostname"
				fi
				zookeeper_started=true
				zookeeper_ip="$ip"
				zookeeper_port="$port"
      		else
				echo "ERROR: More than one ZooKeeper instance defined."
				close
      		fi
		else
			# call "external" module
			local module="${DEPLOY_SCRIPT_DIR}/modules/${role}"
			if [ -f "$module" ] ; then
				# fix role on non S, T or P, everything else is a P (peer)
				if [[ "$role" != "S" || "$role" != "P" || "$role" != "T" ]] ; then
					role="P"
				fi

				$module "$EXECUTION_DIR" "$LOG_DIR" "$DXRAM_PATH" "$DEFAULT_CLASS" "$LIBRARIES" "$DEFAULT_CONDITION" "$ip" "$port" "$hostname" "$role" "$is_remote" "$node" "$zookeeper_ip" "$zookeeper_port"
	    		if [ "$?" != "0" ] ; then
					close
				fi
			else
				echo "ERROR: Unknown role $role defined in deploy configuration"
				close
			fi
		fi
  	done
}

######################################################
# Resolve hostname to IP
# Globals:
# Arguments:
#   hostname - The hostname to resolve
# Return:
#   ip - The IP address
######################################################
resolve() {
  local hostname=$1
  local ip=""

  ip=`host $hostname | cut -d ' ' -f 4 | grep -E "[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"`
  if [ "$ip" = "" ] ; then
    read ip <<< $(dig $hostname | grep -E "[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}" | awk '{ if ($3 == "IN" && $4 == "A") print $5 }')
    if [ "$ip" = "" ] ; then
	  ip="127.0.0.1"
    fi
  fi
  echo $ip
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
close() {
  echo "Closing all dxram instances..."
  local node=""
  while read node || [[ -n "$node" ]]; do
    local ip=`echo $node | cut -d ',' -f 1`
    local hostname=`echo $node | cut -d ',' -f 3`
    local role=`echo $node | cut -d ',' -f 4`

    if [ "$role" = "Z" ] ; then
      # Stop ZooKeeper?
      echo "ZooKeeper might stay alive"
    else
      if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ] ; then
        pkill -9 -f DXRAM.jar
      else
        ssh $hostname -n "pkill -9 -f DXRAM.jar"
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

if [ "$1" = "" ] ; then
  echo "Missing parameter: Configuration file"
  echo "  Example: $0 SimpleTest.conf"
  exit
fi

node_file="$1"
if [ "${node_file: -5}" != ".conf" ] ; then
  node_file="${node_file}.conf"
fi

check_programs "$node_file"

# Trim node file
NODES=`cat "$node_file" | grep -v '#' | sed 's/, /,/g' | sed 's/,\t/,/g'`

# Set default values
readonly LOCALHOST=`resolve "localhost"`
if [ `echo $LOCALHOST | cut -d "." -f 1` != "127" ] ; then
	echo "Illegal loopback device (ip: $LOCALHOST). Exiting..."
	exit
fi

readonly THIS_HOST=`resolve $(hostname)`
readonly DEFAULT_CLASS="de.hhu.bsinfo.dxram.run.DXRAMMain"
readonly LIBRARIES="lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:lib/jline-1.0.jar:DXRAM.jar"
readonly DEFAULT_CONDITION="!---ooo---!"
readonly ZOOKEEPER_PORT="2181"

echo "########################################"
echo "Deploying $(echo $1 | cut -d '.' -f 1) on $THIS_HOST"
echo "########################################"
echo ""

# Set execution paths, all paths absolute
readonly NODE_FILE_DIR="$(cd "$(dirname "$1")"; pwd)/"
readonly EXECUTION_DIR="`pwd`/"
readonly DEPLOY_SCRIPT_DIR=$(dirname "$0")
determine_configurable_paths
readonly DEPLOY_TMP_DIR="${EXECUTION_DIR}deploy_tmp_"$(date +%s)"/"
readonly LOG_DIR="${DEPLOY_TMP_DIR}logs/"
readonly CONFIG_FILE="${DXRAM_PATH}config/dxram.json"
echo -e "\n\n"

# Detect NFS mounted FS
if [ $(df -P -T $DXRAM_PATH | tail -n +2 | awk '{print $2}' | grep "nfs") != "" ] ; then
	readonly NFS_MODE=false
else
	readonly NFS_MODE=true
fi

clean_up

check_configuration

write_configuration

execute

echo -e "\n\n"
close
