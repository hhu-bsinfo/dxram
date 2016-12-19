#!/bin/bash


#############
# Functions #
#############

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
  echo "$tmp"
  if [ "$tmp" != "" ] ; then
    readonly LOCAL_EXEC_PATH=`echo "$tmp" | cut -d '=' -f 2`
  else
    readonly LOCAL_EXEC_PATH="../../"
  fi
  
  tmp=`echo "$NODES" | grep REMOTE_EXEC_PATH`
  if [ "$tmp" != "" ] ; then
    readonly REMOTE_EXEC_PATH=`echo "$tmp" | cut -d '=' -f 2`
  else
    readonly REMOTE_EXEC_PATH="/home/beineke/dxram/"
  fi
  
  tmp=`echo "$NODES" | grep LOCAL_ZOOKEEPER_PATH`
  if [ "$tmp" != "" ] ; then
    readonly LOCAL_ZOOKEEPER_PATH=`echo "$tmp" | cut -d '=' -f 2`
  else
    readonly LOCAL_ZOOKEEPER_PATH="/mnt/c/Users/kbein/workspace/zookeeper/"
  fi
  
  tmp=`echo "$NODES" | grep REMOTE_ZOOKEEPER_PATH`
  if [ "$tmp" != "" ] ; then
    readonly REMOTE_ZOOKEEPER_PATH=`echo "$tmp" | cut -d '=' -f 2`
  else
    readonly REMOTE_ZOOKEEPER_PATH="/home/beineke/zookeeper/"
  fi

  # Trim node file
  NODES=`echo "$NODES" | grep -v '_EXEC_PATH'`
  readonly NODES=`echo "$NODES" | grep -v '_ZOOKEEPER_PATH'`
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
  rm -f "${EXECUTION_DIR}nohup.out"
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
    java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS > /dev/null 2>&1
    echo -e "File not found: DXRAM configuration file was created\n"
    cd "$EXECUTION_DIR"
  else
    local component_header=`echo $config_content | grep "m_components"`
    local service_header=`echo $config_content | grep "m_services"`
    if [ "$component_header" = "" -o "$service_header" = "" ] ; then
      # Configuration file seems to be corrupted -> start dxram once to create new configuration
      rm "$CONFIG_FILE"
      cd "$LOCAL_EXEC_PATH"
      java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS > /dev/null 2>&1
      echo -e "File corruption: DXRAM configuration file was created\n"
      cd "$EXECUTION_DIR"
    fi
  fi
}

######################################################
# Write DXRAM configuration file with updated node and ZooKeeper information
# Globals:
#   CONFIG_FILE
#   NODES
#   EXECUTION_DIR
# Arguments:
#   None
######################################################
write_configuration() {
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

  local current_config=""
  local config_string=""
  local end=""
  local node=""
  local first_iterartion=true
  while read node || [[ -n "$node" ]]; do
    local node_ip=`echo $node | cut -d ',' -f 1`
    local node_port=`echo $node | cut -d ',' -f 2`
    local role=`echo $node | cut -d ',' -f 3`

    if [ "$role" = "Z" ] ; then
      # Create replacement string for zookeeper configuration
      local zookeeper_config_string="
      \"m_path\": \"/dxram\",
      \"m_connection\": {
        \"m_ip\": \"$node_ip\",
        \"m_port\": $node_port
      },"

      # Replace zookeeper configuration
      current_config=`sed '/ZookeeperBootComponent/q' $CONFIG_FILE`
      current_config="$current_config$zookeeper_config_string"
      end=`sed -ne '/ZookeeperBootComponent/{s///; :a' -e 'n;p;ba' -e '}' $CONFIG_FILE`
      end=`echo "$end" | sed -ne '/},/{s///; :a' -e 'n;p;ba' -e '}'`
      current_config=`echo -e "$current_config\n$end"`
      continue
    elif [ "$role" = "S" ] ; then
      role="SUPERPEER"
    elif [ "$role" = "P" ] ; then
      role="PEER"
    elif [ "$role" = "T" ] ; then
      role="TERMINAL"
    fi
    
    local node_string=`echo "$default_node" | sed "s/IP_TEMPLATE/$node_ip/" | sed "s/PORT_TEMPLATE/$node_port/" | sed "s/ROLE_TEMPLATE/$role/"`
    
    if [ "$first_iterartion" == true ] ; then
      config_string="$config_string$node_string"
      first_iterartion=false
    else
      config_string="$config_string,$node_string"
    fi
  done <<< "$NODES"
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
#   ip - The IP of the remote node
# Return:
#   copied - Whether the local config was copied
######################################################
copy_remote_configuration() {
    local copied=$1
    local ip=$2

    if [ "$NFS_MODE" = false -o "$copied" = false ] ; then
      scp "dxram.json ${ip}:${REMOTE_EXEC_PATH}config/"
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
#   port - The Port of the ZooKeeper server to start
######################################################
start_remote_zookeeper() {
  local ip=$1
  local port=$2

  nohup ssh $ip -n "sed -i \"s/clientPort=[0-9]*/clientPort=$port/g\" \"${REMOTE_ZOOKEEPER_PATH}conf/zoo.cfg\" && ${REMOTE_ZOOKEEPER_PATH}bin/zkServer.sh start"
}

######################################################
# Start ZooKeeper locally
# Globals:
#   LOCAL_ZOOKEEPER_PATH
#   EXECUTION_DIR
# Arguments:
#   port - The Port of the ZooKeeper server to start
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
# Arguments:
#   ip - The IP of the ZooKeeper server
#   port - The Port of the ZooKeeper server
######################################################
check_zookeeper_startup() {
  local ip=$1
  local port=$2

  while true ; do
    local success_started=`cat "${LOG_DIR}log_zookeeper" | grep "STARTED"`
    local success_running=`cat "${LOG_DIR}log_zookeeper" | grep "already running"`
    local fail_pid=`cat "${LOG_DIR}log_zookeeper" | grep "FAILED TO WRITE PID"`
    local fail_started=`cat "${LOG_DIR}log_zookeeper" | grep "SERVER DID NOT START"`
    if [ "$success_started" != "" -o "$success_running" != "" ] ; then
      echo "ZooKeeper ($ip $port) started"
      
      # TODO: Remote

      # Remove all dxram related entries
      cd "$LOCAL_ZOOKEEPER_PATH"
      "bin/zkCli.sh" rmr /dxram > "${LOG_DIR}log_zookeeper_client"
      cd "$EXECUTION_DIR"

      break  
    elif [ "$fail_pid" != "" -o "$fail_started" != "" ] ; then
      echo "ERROR: ZooKeeper ($ip $port) could not be started. See log file ${LOG_DIR}log_zookeeper"
      exit
    fi
    sleep 1.0
  done
}

######################################################
# Start a Superpeer on a remote node
# Globals:
#   REMOTE_EXEC_PATH
#   DEFAULT_CLASS
# Arguments:
#   ip - The IP of the Superpeer
#   port - The Port of Superpeer
######################################################
start_remote_superpeer() {
  local ip=$1
  local port=$2

  nohup ssh $ip -n "cd $REMOTE_EXEC_PATH && java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$ip -Ddxram.m_engineSettings.m_address.m_port=$port -Ddxram.m_engineSettings.m_role=Superpeer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS"
}

######################################################
# Start a Superpeer locally
# Globals:
#   LOCAL_EXEC_PATH
#   DEFAULT_CLASS
#   EXECUTION_DIR
# Arguments:
#   ip - The IP of the Superpeer
#   port - The Port of Superpeer
######################################################
start_local_superpeer() {
  local ip=$1
  local port=$2

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$ip -Ddxram.m_engineSettings.m_address.m_port=$port -Ddxram.m_engineSettings.m_role=Superpeer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS
  cd "$EXECUTION_DIR"
}

######################################################
# Check Superpeer startup, exit on failure
# Globals:
#   LOG_DIR
# Arguments:
#   ip - The IP of the Superpeer
#   port - The Port of Superpeer
#   number_of_superpeers - The number of Superpeers
######################################################
check_superpeer_startup() {
  local node_ip=$1
  local node_port=$2
  local number_of_superpeers=$3

  while true ; do
    local success=`cat "${LOG_DIR}log_superpeer_$number_of_superpeers" | grep "^>>> Superpeer started <<<$"`
    local fail=`cat "${LOG_DIR}log_superpeer_$number_of_superpeers" | grep "^Initializing DXRAM failed.$"`
    if [ "$success" != "" ] ; then
      echo "Superpeer ($node_ip $node_port) started"
     break
    elif [ "$fail" != "" ] ; then
      echo "ERROR: Superpeer ($node_ip $node_port) could not be started. See log file ${LOG_DIR}log_superpeer_$number_of_superpeers"
      exit
    fi
    sleep 1.0
  done
}

######################################################
# Start a Peer on a remote node
# Globals:
#   REMOTE_EXEC_PATH
# Arguments:
#   ip - The IP of the Peer
#   port - The Port of Peer
#   ram_size_in_gb - The key-value store size
#   class - The class to execute
#   arguments - The arguments
######################################################
start_remote_peer() {
  local ip=$1
  local port=$2
  local ram_size_in_gb=$3
  local class=$4
  local arguments="$5"

  nohup ssh $ip -n "cd $REMOTE_EXEC_PATH && java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$ip -Ddxram.m_engineSettings.m_address.m_port=$port -Ddxram.m_engineSettings.m_role=Peer -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=$ram_size_in_gb -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=gb -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $class $arguments"
}

######################################################
# Start a Peer locally
# Globals:
#   LOCAL_EXEC_PATH
# Arguments:
#   ip - The IP of the Peer
#   port - The Port of Peer
#   ram_size_in_gb - The key-value store size
#   class - The class to execute
#   arguments - The arguments
######################################################
start_local_peer() {
  local ip=$1
  local port=$2
  local ram_size_in_gb=$3
  local class=$4
  local arguments="$5"

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$ip -Ddxram.m_engineSettings.m_address.m_port=$port -Ddxram.m_engineSettings.m_role=Peer -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=$ram_size_in_gb -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=gb -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $class $arguments
  cd "$EXECUTION_DIR"
}

######################################################
# Check Peer startup, exit on failure
# Globals:
#   LOG_DIR
# Arguments:
#   ip - The IP of the Peer
#   port - The Port of Peer
#   ram_size_in_gb - The key-value store size
#   class - The class to execute
#   arguments - The arguments
#   condition - The string to wait for
#   number_of_peers - The number of Peers
######################################################
check_peer_startup() {
  local ip=$1
  local port=$2
  local ram_size_in_gb=$3
  local class=$4
  local arguments="$5"
  local condition="$6"
  local number_of_peers=$7

  while true ; do
    local success=`cat "${LOG_DIR}log_peer_$number_of_peers" | grep "^$condition$"`
    local fail_init=`cat "${LOG_DIR}log_peer_$number_of_peers" | grep "^Initializing DXRAM failed.$"`
    local fail_error=`cat "${LOG_DIR}log_peer_$number_of_peers" | grep -i "error"`
    if [ "$success" != "" ] ; then
      echo "Peer ($ip $port $ram_size_in_gb $class $arguments) started"
      break
    elif [ "$fail_init" != "" ] ; then
      echo "ERROR: Peer ($ip $port $ram_size_in_gb $class $arguments) could not be started. See log file ${LOG_DIR}log_peer_$number_of_peers"
      exit
    elif [ "$fail_error" != "" ] ; then
      echo "ERROR: Peer ($ip $port $ram_size_in_gb $class $arguments) failed. See log file ${LOG_DIR}log_peer_$number_of_peers"
      exit
    fi
    sleep 1.0
  done
}

######################################################
# Start a Terminal on a remote node
# Globals:
#   REMOTE_EXEC_PATH
#   DEFAULT_CLASS
# Arguments:
#   ip - The IP of the Peer
#   port - The Port of Peer
#   script - The script with terminal commands
######################################################
start_remote_terminal() {
  local ip=$1
  local port=$2
  local script=$3

  ssh $ip - n "cd $REMOTE_EXEC_PATH && java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$ip -Ddxram.m_engineSettings.m_address.m_port=$port -Ddxram.m_engineSettings.m_role=Terminal -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS < $script"
}

######################################################
# Start a Terminal locally
# Globals:
#   LOCAL_EXEC_PATH
#   DEFAULT_CLASS
#   EXECUTION_DIR
# Arguments:
#   ip - The IP of the Peer
#   port - The Port of Peer
#   script - The script with terminal commands
######################################################
start_local_terminal() {
  local ip=$1
  local port=$2
  local script=$3

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$ip -Ddxram.m_engineSettings.m_address.m_port=$port -Ddxram.m_engineSettings.m_role=Terminal -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS < $script
  cd "$EXECUTION_DIR"
}

######################################################
# Start all instances
# Globals:
#   LOCALHOST
#   LOG_DIR
#   NODES
# Arguments:
#   None
######################################################
execute() {
  local number_of_superpeers=0
  local number_of_peers=0
  local local_config_was_copied=false
  local remote_config_was_copied=false
  
  local node=""
  while read node || [[ -n "$node" ]]; do
    local node_ip=`echo $node | cut -d ',' -f 1`
    local node_port=`echo $node | cut -d ',' -f 2`
    local role=`echo $node | cut -d ',' -f 3`

    if [ "$role" = "Z" ] ; then
      if [ "$node_ip" = "$LOCALHOST" ] ; then
        start_local_zookeeper $node_ip > ${LOG_DIR}log_zookeeper 2>&1 &
        check_zookeeper_startup $node_ip $node_port
      else
        start_remote_zookeeper $node_ip $node_port > ${LOG_DIR}log_peer_zookeeper 2>&1 &
        check_zookeeper_startup $node_ip $node_port
      fi
    elif [ "$role" = "S" ] ; then
      number_of_superpeers=$(($number_of_superpeers + 1))
      if [ "$node_ip" = "$LOCALHOST" ] ; then
        local_config_was_copied=`copy_local_configuration $local_config_was_copied`
        start_local_superpeer $node_ip $node_port > ${LOG_DIR}log_superpeer_$number_of_superpeers 2>&1 &
        check_superpeer_startup $node_ip $node_port $number_of_superpeers
      else
        remote_config_was_copied=`copy_remote_configuration $remote_config_was_copied $node_ip`
        start_remote_superpeer $node_ip $node_port > ${LOG_DIR}log_superpeer_$node_ip 2>&1 &
        check_superpeer_startup $node_ip $node_port $number_of_superpeers
      fi
    elif [ "$role" = "P" ] ; then
      number_of_peers=$(($number_of_peers + 1))

      iter=4
      while true ; do
        local tmp=`echo $node | cut -d ',' -f $iter`
        local iter=$((iter + 1))

        local arg_type=`echo $tmp | cut -d '=' -f 1`
        if [ "$arg_type" = "" ] ; then
          break
        elif [ "$arg_type" = "kvss" ] ; then
          local ram_size_in_gb=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "class" ] ; then
          local class=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "args" ] ; then
          local arguments=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "cond" ] ; then
          local condition=`echo $tmp | cut -d '=' -f 2`
        elif [ "$arg_type" = "tcond" ] ; then
          local time_condition=`echo $tmp | cut -d '=' -f 2`
        fi  
      done

      if [ "$class" = "" ] ; then
        local class=$DEFAULT_CLASS
      fi
      if [ "$condition" = "" ] ; then
        local condition=$DEFAULT_CONDITION
      fi

      if [ "$node_ip" = "$LOCALHOST" ] ; then
        local_config_was_copied=`copy_local_configuration $local_config_was_copied`
        start_local_peer $node_ip $node_port $ram_size_in_gb $class "$arguments" > ${LOG_DIR}log_peer_$number_of_peers 2>&1 &
        check_peer_startup $node_ip $node_port $ram_size_in_gb $class "$arguments" "$condition" $number_of_peers
      else
        remote_config_was_copied=`copy_remote_configuration $remote_config_was_copied $node_ip`
        start_emote_peer $node_ip $node_port $ram_size_in_gb $class "$arguments" > ${LOG_DIR}log_peer_$node_ip 2>&1 &
        check_peer_startup $node_ip $node_port $ram_size_in_gb $class "$arguments" "$condition" $number_of_peers
      fi

      if [ "$time_condition" != "" ] ; then
        echo "Waiting for $time_condition seconds after initialization..."
        sleep "$time_condition"
      fi
    elif [ "$role" = "T" ] ; then
      script=`echo $node | cut -d ',' -f 4`
      echo "Starting terminal ($node_ip $node_port $script)"
      echo "Quit with ctrl+c or by typing \"quit\""
      echo "Output:"

      if [ "$node_ip" = "$LOCALHOST" ] ; then
        local_config_was_copied=`copy_local_configuration $local_config_was_copied`
        start_local_terminal $node_ip $node_port $script
      else
        remote_config_was_copied=`copy_remote_configuration $remote_config_was_copied $node_ip`
        start_remote_terminal $node_ip $node_port $script
      fi
    fi

  done <<< "$NODES"
}

######################################################
# Close all instances
# Globals:
#   NODES
#   LOCALHOST
# Arguments:
#   None
######################################################
close() {
  echo "Closing all dxram instances..."
  local node=""
  while read node || [[ -n "$node" ]]; do
    node_ip=`echo $node | cut -d ',' -f 1`
    node_port=`echo $node | cut -d ',' -f 2`
    role=`echo $node | cut -d ',' -f 3`

    if [ "$role" = "Z" ] ; then
      # Stop ZooKeeper?
      echo "ZooKeeper will stay alive"
    else
      if [ "$node_ip" = "$LOCALHOST" ] ; then
        kill `ps u | grep dxram | grep $node_port | cut -d " " -f 3`
      else
        ssh $node_ip - n "kill `ps u | grep dxram grep $node_port | cut -d " " -f 3`"
      fi
      sleep 1
    fi
  done <<< "$NODES"
}


###############
# Entry point #
###############

# Trim node file
node_file="./$1"
if [ "${node_file: -5}" != ".conf" ] ; then
  node_file="${node_file}.conf"
fi
NODES=`cat "$node_file" | grep -v '#' | sed 's/, /,/g' | sed 's/,\t/,/g'`

# Set default values
readonly NFS_MODE=true # Deactivate for copying configuration to every single remote node
readonly LOCALHOST="127.0.0.1"
readonly DEFAULT_CLASS="de.hhu.bsinfo.dxram.run.DXRAMMain"
readonly DEFAULT_CONDITION="Peer started."

# Set execution paths
determine_configurable_paths
readonly EXECUTION_DIR="`pwd`/"
readonly LOG_DIR="${EXECUTION_DIR}logs/"
readonly CONFIG_FILE="${LOCAL_EXEC_PATH}config/dxram.json"

# Print configuration
echo "##############################"
echo "Deploying $(echo $1 | cut -d '.' -f 1)"
echo "##############################"
echo ""
echo "Local execution path: $LOCAL_EXEC_PATH"
echo "Remote execution path: $REMOTE_EXEC_PATH"
echo "Local ZooKeeper path: $LOCAL_ZOOKEEPER_PATH"
echo "Remote ZooKeeper path: $REMOTE_ZOOKEEPER_PATH"
echo -e "\n\n"

clean_up

check_configuration

write_configuration

execute

echo -e "\n\n"
close
echo "Exiting..."