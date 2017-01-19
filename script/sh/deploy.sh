#!/bin/bash
SHELL_TYPE=`readlink /proc/$$/exe | tr '/' '\n' | tail -1`
if [ "$SHELL_TYPE" != "bash" ] ; then
  echo "Script must be executed in bash. Exiting..."
  exit
fi


#############
# Functions #
#############

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
  if ! hash getent 2>/dev/null ; then
    echo "Please install libc-bin. Used for getent. Exiting..."
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
#   LOCAL_EXEC_PATH
#   REMOTE_EXEC_PATH
#   LOCAL_ZOOKEEPER_PATH
#   REMOTE_ZOOKEEPER_PATH
#   NODES
# Arguments:
#   None
######################################################
determine_configurable_paths() {
  local tmp=`echo "$NODES" | grep LOCAL_EXEC_PATH`
  if [ "$tmp" != "" ] ; then
    readonly LOCAL_EXEC_PATH=`echo "$tmp" | cut -d '=' -f 2`
    echo "Local execution path: $LOCAL_EXEC_PATH"
  else
    readonly LOCAL_EXEC_PATH="../../"
    echo "Local execution path undefined. Using default: $LOCAL_EXEC_PATH"
  fi
  
  tmp=`echo "$NODES" | grep REMOTE_EXEC_PATH`
  if [ "$tmp" != "" ] ; then
    readonly REMOTE_EXEC_PATH=`echo "$tmp" | cut -d '=' -f 2`
    echo "Remote execution path: $REMOTE_EXEC_PATH"
  else
    readonly REMOTE_EXEC_PATH="~/dxram/"
    echo "Remote execution path undefined. Using default: $REMOTE_EXEC_PATH"
  fi
  
  tmp=`echo "$NODES" | grep LOCAL_ZOOKEEPER_PATH`
  if [ "$tmp" != "" ] ; then
    readonly LOCAL_ZOOKEEPER_PATH=`echo "$tmp" | cut -d '=' -f 2`
    echo "Local ZooKeeper path: $LOCAL_ZOOKEEPER_PATH"
  else
    readonly LOCAL_ZOOKEEPER_PATH="~/zookeeper/"
    echo "Local ZooKeeper path undefined. Using default: $LOCAL_ZOOKEEPER_PATH"
  fi
  
  tmp=`echo "$NODES" | grep REMOTE_ZOOKEEPER_PATH`
  if [ "$tmp" != "" ] ; then
    readonly REMOTE_ZOOKEEPER_PATH=`echo "$tmp" | cut -d '=' -f 2`
    echo "Remote ZooKeeper path: $REMOTE_ZOOKEEPER_PATH"
  else
    readonly REMOTE_ZOOKEEPER_PATH="~/zookeeper/"
    echo "Remote ZooKeeper path undefined. Using default: $REMOTE_ZOOKEEPER_PATH"
  fi

  # Trim node file
  NODES=`echo "$NODES" | grep -v '_EXEC_PATH'`
  NODES=`echo "$NODES" | grep -v '_ZOOKEEPER_PATH'`
}

######################################################
# Remove file/directories from last execution
# Globals:
#   EXECUTION_DIR
#   LOG_DIR
#   NODES
# Arguments:
#   None
######################################################
clean_up() {
  rm -f "${EXECUTION_DIR}dxram.json"
  rm -rf "${EXECUTION_DIR}logs"

  mkdir "$LOG_DIR"
}

######################################################
# Check DXRAM configuration file, generate it if it is missing or obviously corrupted
# Globals:
#   CONFIG_FILE
#   LOCAL_EXEC_PATH
#   DEFAULT_CLASS
#   EXECUTION_DIR
# Arguments:
#   None
######################################################
check_configuration() {
  local config_content=`cat "$CONFIG_FILE" 2> /dev/null`
  if [ "$config_content" = "" ] ; then
    # There is no configuration file -> start dxram once to create configuration
    cd "$LOCAL_EXEC_PATH"
    java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -cp $LIBRARIES $DEFAULT_CLASS > /dev/null 2>&1
    echo -e "File not found: DXRAM configuration file was created\n"
    cd "$EXECUTION_DIR"
  else
    local component_header=`echo $config_content | grep "m_components"`
    local service_header=`echo $config_content | grep "m_services"`
    if [ "$component_header" = "" -o "$service_header" = "" ] ; then
      # Configuration file seems to be corrupted -> start dxram once to create new configuration
      rm "$CONFIG_FILE"
      cd "$LOCAL_EXEC_PATH"
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
    elif [ "$role" = "P" ] ; then
      current_port=${NODE_ARRAY["$hostname"]}
      if [ "$current_port" = "" ] ; then
	current_port=22222
      else
	current_port=$(($current_port + 1))
      fi
      port=$current_port
      NODE_ARRAY["$hostname"]=$current_port

      role="PEER"
    elif [ "$role" = "T" ] ; then
      port="22220"

      role="TERMINAL"
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
  
  echo "$new_config" > "${EXECUTION_DIR}dxram.json"
}

######################################################
# Copy DXRAM configuration to remote node
# Globals:
#   NFS_MODE
#   REMOTE_EXEC_PATH
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
      scp "dxram.json" "${hostname}:${REMOTE_EXEC_PATH}config/"
      copied=true
    fi
    echo "$copied"
}

######################################################
# Copy DXRAM configuration for local execution
# Globals:
#   LOCAL_EXEC_PATH
# Arguments:
#   copied - Whether the local config has to be copied
# Return:
#   copied - Whether the local config was copied
######################################################
copy_local_configuration() {
  local copied=$1

  if [ "$copied" = false ] ; then
    cp dxram.json "${LOCAL_EXEC_PATH}config/"
    copied=true
  fi
  echo "$copied"
}

######################################################
# Start ZooKeeper on remote node
# Globals:
#   REMOTE_ZOOKEEPER_PATH
# Arguments:
#   ip - The IP of the remote node
#   port - The port of the ZooKeeper server to start
#   hostname - The hostname of the ZooKeeper server
######################################################
start_remote_zookeeper() {
  local ip=$1
  local port=$2
  local hostname=$3

  ssh $hostname -n "cd $REMOTE_ZOOKEEPER_PATH && sed -i \"s/clientPort=[0-9]*/clientPort=$port/g\" \"conf/zoo.cfg\" && bin/zkServer.sh start"
}

######################################################
# Start ZooKeeper locally
# Globals:
#   LOCAL_ZOOKEEPER_PATH
#   EXECUTION_DIR
# Arguments:
#   port - The port of the ZooKeeper server to start
######################################################
start_local_zookeeper() {
  local port=$1

  cd "$LOCAL_ZOOKEEPER_PATH"
  sed -i "s/clientPort=[0-9]*/clientPort=$port/g" "conf/zoo.cfg"
  "bin/zkServer.sh" start
  cd "$EXECUTION_DIR"
}

######################################################
# Check ZooKeeper startup, exit on failure
# Globals:
#   LOG_DIR
#   LOCAL_ZOOKEEPER_PATH
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
	cd "$LOCAL_ZOOKEEPER_PATH"
	echo "rmr /dxram" | "bin/zkCli.sh" > "${LOG_DIR}${hostname}_${port}_zookeeper_client" 2>&1
	cd "$EXECUTION_DIR"
      else
	ssh $hostname -n "echo \"rmr /dxram\" | ${REMOTE_ZOOKEEPER_PATH}bin/zkCli.sh" > "${LOG_DIR}${hostname}_${port}_zookeeper_client" 2>&1
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
# Compile the java vm options string for a superpeer
# Globals:
#   VM_OPTS - Created by function ("return value")
# Arguments:
#   ip - The IP of the superpeer
#   port - The port of superpeer
#   vm_options - Further VM options
######################################################
compile_vm_options_string_superpeer() {
  local ip=$1
  local port=$2
  local vm_options="$3"

  VM_OPTS=""
  VM_OPTS="$VM_OPTS -Dlog4j.configurationFile=config/log4j.xml"
  VM_OPTS="$VM_OPTS -Ddxram.config=config/dxram.json"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_address.m_ip=$ip"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_address.m_port=$port"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_role=Superpeer"
  VM_OPTS="$VM_OPTS $vm_options"
}

######################################################
# Start a Superpeer on a remote node
# Globals:
#   REMOTE_EXEC_PATH
#   DEFAULT_CLASS
# Arguments:
#   ip - The IP of the Superpeer
#   port - The port of Superpeer
#   hostname - The hostname
#   vm_options - The VM options
######################################################
start_remote_superpeer() {
  local ip=$1
  local port=$2
  local hostname=$3
  local vm_options="$4"

  compile_vm_options_string_superpeer $ip $port $vm_options

  echo "Executing superpeer on $3 ($ip, $port):"
  ssh $hostname -n "cd $REMOTE_EXEC_PATH && java $VM_OPTS -cp $LIBRARIES $DEFAULT_CLASS"
}

######################################################
# Start a Superpeer locally
# Globals:
#   LOCAL_EXEC_PATH
#   DEFAULT_CLASS
#   EXECUTION_DIR
# Arguments:
#   ip - The IP of the Superpeer
#   port - The port of Superpeer
#   vm_options - The VM options
######################################################
start_local_superpeer() {
  local ip=$1
  local port=$2
  local vm_options="$3"

  compile_vm_options_string_superpeer $ip $port $vm_options

  cd "$LOCAL_EXEC_PATH"
  java $VM_OPTS -cp $LIBRARIES $DEFAULT_CLASS
  cd "$EXECUTION_DIR"
}

######################################################
# Check Superpeer startup, exit on failure
# Globals:
#   LOG_DIR
# Arguments:
#   ip - The IP of the Superpeer
#   port - The port of Superpeer
#   hostname - The hostname
#   vm_options - The VM options
######################################################
check_superpeer_startup() {
  local ip=$1
  local port=$2
  local hostname=$3
  local vm_options="$4"

  local logfile="${LOG_DIR}${hostname}_${port}_superpeer"

  while true ; do
    local success=`cat "$logfile" 2> /dev/null | sed "s,\x1B\[[0-9;]*[a-zA-Z],,g" | grep "$DEFAULT_CONDITION"`
    local fail=`cat "$logfile" 2> /dev/null | sed "s,\x1B\[[0-9;]*[a-zA-Z],,g" | grep "^Initializing DXRAM failed.$"`
    if [ "$success" != "" ] ; then
      echo "Superpeer ($ip $port $vm_options) started"
     break
    elif [ "$fail" != "" ] ; then
      echo "ERROR: Superpeer ($ip $port $vm_options) could not be started. See log file $logfile"
      close
    fi
    sleep 1.0
  done
}

######################################################
# Compile the java vm options string for a peer
# Globals:
#   VM_OPTS - Created by function ("return value")
# Arguments:
#   ip - The IP of the Peer
#   port - The port of Peer
#   ram_size_in_gb - The key-value store size (optional)
#   compute_node_role - The role of the node if part of a compute group (optional)
#   compute_group_ID - The compute group id if taking part on master-slave computations (optional)
#   vm_options - Further VM options
######################################################
compile_vm_options_string_peer() {
  local ip=$1
  local port=$2
  local ram_size_in_gb=$3
  local compute_node_role=$4
  local compute_group_id=$5
  local vm_options="$6"

  VM_OPTS=""
  VM_OPTS="$VM_OPTS -Dlog4j.configurationFile=config/log4j.xml"
  VM_OPTS="$VM_OPTS -Ddxram.config=config/dxram.json"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_address.m_ip=$ip"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_address.m_port=$port"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_role=Peer"

  if [ "$ram_size_in_gb" ] ; then
    VM_OPTS="$VM_OPTS -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=$ram_size_in_gb"
    VM_OPTS="$VM_OPTS -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=gb"
  fi    

  if [ "$compute_node_role" ] ; then
    VM_OPTS="$VM_OPTS -Ddxram.m_services[MasterSlaveComputeService].m_role=$compute_node_role"
  fi

  if [ "$compute_group_id" ] ; then
    VM_OPTS="$VM_OPTS -Ddxram.m_services[MasterSlaveComputeService].m_computeGroupId=$compute_group_id"
  fi

  VM_OPTS="$VM_OPTS $vm_options"
}

######################################################
# Start a Peer on a remote node
# Globals:
#   REMOTE_EXEC_PATH
# Arguments:
#   ip - The IP of the Peer
#   port - The port of Peer
#   hostname - The hostname
#   ram_size_in_gb - The key-value store size
#   compute_node_role - The role of the node if part of a compute group
#   compute_group_ID - The compute group id if taking part on master-slave computations
#   class - The class to execute
#   arguments - The arguments
#   vm_options - The VM options
######################################################
start_remote_peer() {
  local ip=$1
  local port=$2
  local hostname=$3
  local ram_size_in_gb=$4
  local compute_node_role=$5
  local compute_group_id=$6
  local class=$7
  local arguments="$8"
  local vm_options="$9"

  compile_vm_options_string_peer $ip $port $ram_size_in_gb $compute_node_role $compute_group_id $vm_options

  echo "Executing peer on $3 ($ip, $port):"
  ssh $hostname -n "cd $REMOTE_EXEC_PATH && java $VM_OPTS -cp $LIBRARIES $class $arguments"
}

######################################################
# Start a Peer locally
# Globals:
#   LOCAL_EXEC_PATH
# Arguments:
#   ip - The IP of the Peer
#   port - The port of Peer
#   ram_size_in_gb - The key-value store size
#   compute_node_role - The role of the node if part of a compute group
#   compute_group_ID - The compute group id if taking part on master-slave computations
#   class - The class to execute
#   arguments - The arguments
#   vm_options - The VM options
######################################################
start_local_peer() {
  local ip=$1
  local port=$2
  local ram_size_in_gb=$3
  local compute_node_role=$4
  local compute_group_id=$5
  local class=$6
  local arguments="$7"
  local vm_options="$8"

  compile_vm_options_string_peer $ip $port $ram_size_in_gb $compute_node_role $compute_group_id $vm_options

  cd "$LOCAL_EXEC_PATH"
  java $VM_OPTS -cp $LIBRARIES $class $arguments
  cd "$EXECUTION_DIR"
}

######################################################
# Check Peer startup, exit on failure
# Globals:
#   LOG_DIR
# Arguments:
#   ip - The IP of the Peer
#   port - The port of Peer
#   hostname - The hostname
#   ram_size_in_gb - The key-value store size
#   compute_node_role - The role of the node if part of a compute group
#   compute_group_ID - The compute group id if taking part on master-slave compu
#   class - The class to execute
#   arguments - The arguments
#   condition - The string to wait for
#   vm_options - The VM options
######################################################
check_peer_startup() {
  local ip=$1
  local port=$2
  local hostname=$3
  local ram_size_in_gb=$4
  local compute_node_role=$5
  local compute_group_id=$6
  local class=$7
  local arguments="$8"
  local condition="$9"
  local vm_options="$10"

  local logfile="${LOG_DIR}${hostname}_${port}_peer"

  while true ; do
    local success=`cat "$logfile" 2> /dev/null | sed "s,\x1B\[[0-9;]*[a-zA-Z],,g" | grep "$condition"`
    local fail_init=`cat "$logfile" 2> /dev/null | sed "s,\x1B\[[0-9;]*[a-zA-Z],,g" | grep "^Initializing DXRAM failed.$"`
	# Abort execution after an exception was thrown (every exception but NetworkResponseTimeoutException)
    local fail_error=`cat "$logfile" 2> /dev/null | sed "s,\x1B\[[0-9;]*[a-zA-Z],,g" | grep -i "exception" | grep -v "NetworkResponseTimeoutException"`
    if [ "$success" != "" ] ; then
      echo "Peer ($ip $port $ram_size_in_gb $compute_node_role $compute_group_id $class $arguments $vm_options) started"
      break
    elif [ "$fail_init" != "" ] ; then
      echo "ERROR: Peer ($ip $port $ram_size_in_gb $compute_node_role $compute_group_id $class $arguments $vm_options) could not be started. See log file $logfile"
      close
    elif [ "$fail_error" != "" ] ; then
      echo "ERROR: Peer ($ip $port $ram_size_in_gb $compute_node_role $compute_group_id $class $arguments $vm_options) failed. See log file $logfile"
      close
    fi
    sleep 1.0
  done
}

######################################################
# Compile the java vm options string for a terminal
# Globals:
#   VM_OPTS - Created by function ("return value")
# Arguments:
#   ip - The IP of the terminal
#   port - The port of terminal
#   script - Terminal script to run after start (optional)
######################################################
compile_vm_options_string_terminal() {
  local ip=$1
  local port=$2
  local script=$3

  VM_OPTS=""
  VM_OPTS="$VM_OPTS -Dlog4j.configurationFile=config/log4j.xml"
  VM_OPTS="$VM_OPTS -Ddxram.config=config/dxram.json"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_address.m_ip=$ip"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_address.m_port=$port"
  VM_OPTS="$VM_OPTS -Ddxram.m_engineSettings.m_role=Terminal"

  if [ "$script" ] ; then
    VM_OPTS="$VM_OPTS -Ddxram.m_services[TerminalService].m_autostartScript=script/dxram/$script"
  fi
}

######################################################
# Start a Terminal on a remote node
# Globals:
#   REMOTE_EXEC_PATH
#   DEFAULT_CLASS
# Arguments:
#   ip - The IP of the Peer
#   port - The port of Peer
#   hostname - The hostname
#   script - The script with terminal commands
######################################################
start_remote_terminal() {
  local ip=$1
  local port=$2
  local hostname=$3
  local script=$4

  compile_vm_options_string_terminal $ip $port $script

  echo "Executing terminal on $3 ($ip, $port):"
  ssh $hostname -t "bash -l -c \"cd $REMOTE_EXEC_PATH && java $VM_OPTS -cp $LIBRARIES $DEFAULT_CLASS\""
}

######################################################
# Start a Terminal locally
# Globals:
#   LOCAL_EXEC_PATH
#   DEFAULT_CLASS
#   EXECUTION_DIR
# Arguments:
#   ip - The IP of the Peer
#   port - The port of Peer
#   script - The script with terminal commands
######################################################
start_local_terminal() {
  local ip=$1
  local port=$2
  local script=$3

  compile_vm_options_string_terminal $ip $port $script

  cd "$LOCAL_EXEC_PATH"
  java $VM_OPTS -cp $LIBRARIES $DEFAULT_CLASS
  cd "$EXECUTION_DIR"
}

######################################################
# Start all instances
# Globals:
#   LOCALHOST
#	THIS_HOST
#   LOG_DIR
#   NODES
# Arguments:
#   None
######################################################
execute() {
  local number_of_superpeers=0
  local number_of_peers=0
  local zookeeper_started=false
  local local_config_was_copied=false
  local remote_config_was_copied=false
  
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
      else
	echo "ERROR: More than one ZooKeeper instance defined."
	close
      fi
    elif [ "$role" = "S" ] ; then
      number_of_superpeers=$(($number_of_superpeers + 1))

      local tmp=`echo $node | cut -d ',' -f 5`
      local arg_type=`echo $tmp | cut -d '=' -f 1`
      if [ "$arg_type" = "vmopts" ] ; then
        local vm_options=`echo $tmp | cut -d '=' -f 2`
        vm_options=`echo "-$vm_options" | sed 's/\^/ -/'`
      elif [ "$arg_type" != "" ] ; then
	echo "ERROR: Unknown parameter type $arg_type"
      fi

      if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ] ; then
        local_config_was_copied=`copy_local_configuration "$local_config_was_copied"`
        start_local_superpeer "$ip" "$port" "$vm_options" > "${LOG_DIR}${hostname}_${port}_superpeer" 2>&1 &
        check_superpeer_startup "$ip" "$port" "$hostname" "$vm_options"
      else
        remote_config_was_copied=`copy_remote_configuration "$remote_config_was_copied" "$hostname"`
        start_remote_superpeer "$ip" "$port" "$hostname" "$vm_options" > "${LOG_DIR}${hostname}_${port}_superpeer" 2>&1 &
        check_superpeer_startup "$ip" "$port" "$hostname" "$vm_options"
      fi
    elif [ "$role" = "P" ] ; then
      number_of_peers=$(($number_of_peers + 1))

      iter=5
      while true ; do
        local tmp=`echo $node | cut -d ',' -f $iter`
        local iter=$((iter + 1))

        local arg_type=`echo $tmp | cut -d '=' -f 1`
        if [ "$arg_type" = "" ] ; then
          break
        elif [ "$arg_type" = "vmopts" ] ; then
          local vm_options=`echo $tmp | cut -d '=' -f 2`
          vm_options=`echo "-$vm_options" | sed 's/\^/ -/'`
        elif [ "$arg_type" = "kvss" ] ; then
          local ram_size_in_gb=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "class" ] ; then
          local class=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "args" ] ; then
          local arguments=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "cond" ] ; then
          local condition=`echo $tmp | cut -d '=' -f 2`
          condition="^$condition$"
        elif [ "$arg_type" = "tcond" ] ; then
          local time_condition=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "cnr" ] ; then
          local compute_node_role=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "cgid" ] ; then
          local compute_group_id=`echo $tmp | cut -d '=' -f 2`
        else
	  echo "ERROR: Unknown parameter type $arg_type"
        fi
      done

      if [ "$class" = "" ] ; then
        local class=$DEFAULT_CLASS
      fi
      if [ "$condition" = "" ] ; then
        local condition=$DEFAULT_CONDITION
      fi

      if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ] ; then
        local_config_was_copied=`copy_local_configuration "$local_config_was_copied"`
        start_local_peer "$ip" "$port" "$ram_size_in_gb" "$compute_node_role" "$compute_group_id" "$class" "$arguments" "$vm_options" > "${LOG_DIR}${hostname}_${port}_peer" 2>&1 &
        check_peer_startup "$ip" "$port" "$hostname" "$ram_size_in_gb" "$compute_node_role" "$compute_group_id" "$class" "$arguments" "$condition" "$vm_options"
      else
        remote_config_was_copied=`copy_remote_configuration "$remote_config_was_copied" "$hostname"`
        start_remote_peer "$ip" "$port" "$hostname" "$ram_size_in_gb" "$compute_node_role" "$compute_group_id" "$class" "$arguments" "$vm_options" > "${LOG_DIR}${hostname}_${port}_peer" 2>&1 &
        check_peer_startup "$ip" "$port" "$hostname" "$ram_size_in_gb" "$compute_node_role" "$compute_group_id" "$class" "$arguments" "$condition" "$vm_options"
      fi

      if [ "$time_condition" != "" ] ; then
        echo "Waiting for $time_condition seconds after initialization..."
        sleep "$time_condition"
      fi
    elif [ "$role" = "T" ] ; then
      script=`echo $node | cut -d ',' -f 5`
      echo "Starting terminal ($ip $port $script)"
      echo "Quit with ctrl+c or by typing \"quit\""
      echo "Output:"

      if [ "$ip" = "$LOCALHOST" -o "$ip" = "$THIS_HOST" ] ; then
        local_config_was_copied=`copy_local_configuration "$local_config_was_copied"`
        start_local_terminal "$ip" "$port" "$script"
      else
        remote_config_was_copied=`copy_remote_configuration "$remote_config_was_copied" "$hostname"`
        start_remote_terminal "$ip" "$port" "$hostname" "$script"
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
    ip=`getent hosts $hostname | cut -d ' ' -f 1 | grep -E "[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"`
    if [ "$ip" = "" ] ; then
      echo "ERROR: $hostname could not be identified. Seting default 127.0.0.1"
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

if [ "$1" = "" ] ; then
  echo "Missing parameter: Configuration file"
  echo "  Example: ./deploy.sh SimpleTest.conf"
  exit
fi

node_file="./$1"
if [ "${node_file: -5}" != ".conf" ] ; then
  node_file="${node_file}.conf"
fi

check_programs "$node_file"

# Trim node file
NODES=`cat "$node_file" | grep -v '#' | sed 's/, /,/g' | sed 's/,\t/,/g'`

# Set default values
readonly NFS_MODE=true # Deactivate for copying configuration to every single remote node
readonly LOCALHOST=`resolve "localhost"`
if [ `echo $LOCALHOST | cut -d "." -f 1` != "127" ] ; then
	echo "Illegal loopback device (ip: $LOCALHOST). Exiting..."
	exit
fi
readonly THIS_HOST=`resolve $(hostname)`
readonly DEFAULT_CLASS="de.hhu.bsinfo.dxram.run.DXRAMMain"
readonly LIBRARIES="lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:lib/jline-1.0.jar:DXRAM.jar"
readonly DEFAULT_CONDITION="***!---ooo---!***"
readonly ZOOKEEPER_PORT="2181"

echo "########################################"
echo "Deploying $(echo $1 | cut -d '.' -f 1) on $THIS_HOST"
echo "########################################"
echo ""

# Set execution paths
determine_configurable_paths
readonly EXECUTION_DIR="`pwd`/"
readonly LOG_DIR="${EXECUTION_DIR}logs/"
readonly CONFIG_FILE="${LOCAL_EXEC_PATH}config/dxram.json"
echo -e "\n\n"

clean_up

check_configuration

write_configuration

execute

echo -e "\n\n"
close
