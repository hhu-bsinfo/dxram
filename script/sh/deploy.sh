#!/bin/bash


#############
# Functions #
#############
determineConfigurablePaths() {
  TMP=`echo "$NODES" | grep LOCAL_EXEC_PATH`
  echo "$TMP"
  if [ "$TMP" != "" ] ; then
    LOCAL_EXEC_PATH=`echo "$TMP" | cut -d '=' -f 2`
  else
    LOCAL_EXEC_PATH="../../"
  fi
  
  TMP=`echo "$NODES" | grep REMOTE_EXEC_PATH`
  if [ "$TMP" != "" ] ; then
    REMOTE_EXEC_PATH=`echo "$TMP" | cut -d '=' -f 2`
  else
    REMOTE_EXEC_PATH="/home/beineke/dxram/"
  fi
  
  TMP=`echo "$NODES" | grep LOCAL_ZOOKEEPER_PATH`
  if [ "$TMP" != "" ] ; then
    LOCAL_ZOOKEEPER_PATH=`echo "$TMP" | cut -d '=' -f 2`
  else
    LOCAL_ZOOKEEPER_PATH="/mnt/c/Users/kbein/workspace/zookeeper/"
  fi
  
  TMP=`echo "$NODES" | grep REMOTE_ZOOKEEPER_PATH`
  if [ "$TMP" != "" ] ; then
    REMOTE_ZOOKEEPER_PATH=`echo "$TMP" | cut -d '=' -f 2`
  else
    REMOTE_ZOOKEEPER_PATH="/home/beineke/zookeeper/"
  fi

  # Trim node file
  NODES=`echo "$NODES" | grep -v '_EXEC_PATH'`
  NODES=`echo "$NODES" | grep -v '_ZOOKEEPER_PATH'`
}

clean-up() {
  rm -f "${EXECUTION_DIR}dxram.json"
  rm -f "${EXECUTION_DIR}nohup.out"
  rm -rf "${EXECUTION_DIR}logs"

  mkdir "$LOG_DIR"
}

writeConfiguration() {
  # Create replacement string for nodes configuration:
  DEFAULT_NODE="
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

  CURRENT_CONFIG=""
  CONFIG_STRING=""
  FIRST_ITERATION=true
  while read NODE || [[ -n "$NODE" ]]; do
    NODE_IP=`echo $NODE | cut -d ',' -f 1`
    NODE_PORT=`echo $NODE | cut -d ',' -f 2`
    ROLE=`echo $NODE | cut -d ',' -f 3`

    if [ "$ROLE" = "Z" ] ; then
      # Create replacement string for zookeeper configuration
      ZOOKEEPER_CONFIG_STRING="
      \"m_path\": \"/dxram\",
      \"m_connection\": {
        \"m_ip\": \"$NODE_IP\",
        \"m_port\": $NODE_PORT
      },"

      # Replace zookeeper configuration
      CURRENT_CONFIG=`sed '/ZookeeperBootComponent/q' $CONFIG_FILE`
      CURRENT_CONFIG="$CURRENT_CONFIG$ZOOKEEPER_CONFIG_STRING"
      END=`sed -ne '/ZookeeperBootComponent/{s///; :a' -e 'n;p;ba' -e '}' $CONFIG_FILE`
      END=`echo "$END" | sed -ne '/},/{s///; :a' -e 'n;p;ba' -e '}'`
      CURRENT_CONFIG=`echo -e "$CURRENT_CONFIG\n$END"`
      continue
    elif [ "$ROLE" = "S" ] ; then
      ROLE="SUPERPEER"
    elif [ "$ROLE" = "P" ] ; then
      ROLE="PEER"
    elif [ "$ROLE" = "T" ] ; then
      ROLE="TERMINAL"
    fi
    
    NODE_STRING=`echo "$DEFAULT_NODE" | sed "s/IP_TEMPLATE/$NODE_IP/" | sed "s/PORT_TEMPLATE/$NODE_PORT/" | sed "s/ROLE_TEMPLATE/$ROLE/"`
    
    if [ "$FIRST_ITERATION" == true ] ; then
      CONFIG_STRING="$CONFIG_STRING$NODE_STRING"
      FIRST_ITERATION=false
    else
      CONFIG_STRING="$CONFIG_STRING,$NODE_STRING"
    fi
  done <<< "$NODES"
  CONFIG_STRING=`echo -e "$CONFIG_STRING\n      ],"`

  # Replace nodes configuration:  
  NEW_CONFIG=`echo "$CURRENT_CONFIG" | sed '/m_nodesConfig/q'`
  NEW_CONFIG="$NEW_CONFIG$CONFIG_STRING"
  END=`echo "$CURRENT_CONFIG" | sed -ne '/m_nodesConfig/{s///; :a' -e 'n;p;ba' -e '}'`
  END=`echo "$END" | sed -ne '/],/{s///; :a' -e 'n;p;ba' -e '}'`
  NEW_CONFIG=`echo -e "$NEW_CONFIG\n$END"`
  
  echo "$NEW_CONFIG" > "${EXECUTION_DIR}dxram.json"
}

copyRemoteConfiguration() {
    local IP=$1
    scp "dxram.json ${IP}:${REMOTE_EXEC_PATH}config/"
}

copyLocalConfiguration() {
  if [ "$LOCAL_CONFIG_WAS_COPIED" = false ] ; then
    cp dxram.json "${LOCAL_EXEC_PATH}config/"
    LOCAL_CONFIG_WAS_COPIED=true
  fi
}

startRemoteZooKeeper() {
  local IP=$1
  local PORT=$2

  nohup ssh $IP -n "sed -i \"s/clientPort=[0-9]*/clientPort=$PORT/g\" \"${REMOTE_ZOOKEEPER_PATH}conf/zoo.cfg\" && ${REMOTE_ZOOKEEPER_PATH}bin/zkServer.sh start"
}

startLocalZooKeeper() {
  local IP=$1 # IP is ignored
  local PORT=$2

  cd "$LOCAL_ZOOKEEPER_PATH"
  sed -i "s/clientPort=[0-9]*/clientPort=$PORT/g" "conf/zoo.cfg"
  "bin/zkServer.sh" start
  cd "$EXECUTION_DIR"
}

checkZooKeeperStartup() {
  local IP=$1
  local PORT=$2

  while true ; do
    SUCCESS1=`cat "${LOG_DIR}log_zookeeper" | grep "STARTED"`
    SUCCESS2=`cat "${LOG_DIR}log_zookeeper" | grep "already running"`
    FAIL1=`cat "${LOG_DIR}log_zookeeper" | grep "FAILED TO WRITE PID"`
    FAIL2=`cat "${LOG_DIR}log_zookeeper" | grep "SERVER DID NOT START"`
    if [ "$SUCCESS1" != "" -o "$SUCCESS2" != "" ] ; then
      echo "ZooKeeper ($IP $PORT) started"
      
      # Remove all dxram related entries  
      cd "$LOCAL_ZOOKEEPER_PATH"
      "bin/zkCli.sh" rmr /dxram > "${LOG_DIR}log_zookeeper_client"
      cd "$EXECUTION_DIR"

      break  
    elif [ "$FAIL1" != "" -o "$FAIL2" != "" ] ; then
      echo "ERROR: ZooKeeper ($IP $PORT) could not be started. See log file ${LOG_DIR}log_zookeeper"
      exit
    fi
    sleep 1.0
  done
}

startRemoteSuperpeer() {
  local IP=$1
  local PORT=$2

  nohup ssh $IP -n "cd $REMOTE_EXEC_PATH && java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Superpeer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS"
}

startLocalSuperpeer() {
  local IP=$1
  local PORT=$2

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Superpeer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS
  cd "$EXECUTION_DIR"
}

checkSuperpeerStartup() {
  local NODE_IP=$1
  local NODE_PORT=$2

  while true ; do
    SUCCESS=`cat "${LOG_DIR}log_superpeer_$NUMBER_OF_SUPERPEERS" | grep "^>>> Superpeer started <<<$"`
    FAIL=`cat "${LOG_DIR}log_superpeer_$NUMBER_OF_SUPERPEERS" | grep "^Initializing DXRAM failed.$"`
    if [ "$SUCCESS" != "" ] ; then
      echo "Superpeer ($NODE_IP $NODE_PORT) started"
     break
    elif [ "$FAIL" != "" ] ; then
      echo "ERROR: Superpeer ($NODE_IP $NODE_PORT) could not be started. See log file ${LOG_DIR}log_superpeer_$NUMBER_OF_SUPERPEERS"
      exit
    fi
    sleep 1.0
  done
}

startRemotePeer() {
  local IP=$1
  local PORT=$2
  local RAM_SIZE_IN_GB=$3
  local CLASS=$4
  local ARGUMENTS="$5"

  nohup ssh $IP -n "cd $REMOTE_EXEC_PATH && java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Peer -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=$RAM_SIZE_IN_GB -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=gb -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $CLASS $ARGUMENTS"
}

startLocalPeer() {
  local IP=$1
  local PORT=$2
  local RAM_SIZE_IN_GB=$3
  local CLASS=$4
  local ARGUMENTS="$5"

  echo "$ARGUMENTS"

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Peer -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=$RAM_SIZE_IN_GB -Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=gb -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $CLASS $ARGUMENTS
  cd "$EXECUTION_DIR"
}

checkPeerStartup() {
  local IP=$1
  local PORT=$2
  local RAM_SIZE_IN_GB=$3
  local CLASS=$4
  local ARGUMENTS="$5"
  local CONDITION="$6"

  while true ; do
    SUCCESS=`cat "${LOG_DIR}log_peer_$NUMBER_OF_PEERS" | grep "^$CONDITION$"`
    FAIL1=`cat "${LOG_DIR}log_peer_$NUMBER_OF_PEERS" | grep "^Initializing DXRAM failed.$"`
    FAIL2=`cat "${LOG_DIR}log_peer_$NUMBER_OF_PEERS" | grep -i "error"`
    if [ "$SUCCESS" != "" ] ; then
      echo "Peer ($NODE_IP $NODE_PORT $RAM_SIZE_IN_GB $CLASS $ARGUMENTS) started"
      break
    elif [ "$FAIL1" != "" ] ; then
      echo "ERROR: Peer ($NODE_IP $NODE_PORT $RAM_SIZE_IN_GB $CLASS $ARGUMENTS) could not be started. See log file ${LOG_DIR}log_peer_$NUMBER_OF_PEERS"
      exit
    elif [ "$FAIL2" != "" ] ; then
      echo "ERROR: Peer ($NODE_IP $NODE_PORT $RAM_SIZE_IN_GB $CLASS $ARGUMENTS) failed. See log file ${LOG_DIR}log_peer_$NUMBER_OF_PEERS"
      exit
    fi
    sleep 1.0
  done
}

startRemoteTerminal() {
  local IP=$1
  local PORT=$2
  local SCRIPT=$3

  ssh $IP - n "cd $REMOTE_EXEC_PATH && java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Terminal -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS < $SCRIPT"
}

startLocalTerminal() {
  local IP=$1
  local PORT=$2
  local SCRIPT=$3

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Terminal -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS < $SCRIPT
  cd "$EXECUTION_DIR"
}

execute() {
  NUMBER_OF_SUPERPEERS=0
  NUMBER_OF_PEERS=0

  LOCAL_CONFIG_WAS_COPIED=false
  while read NODE || [[ -n "$NODE" ]]; do
    NODE_IP=`echo $NODE | cut -d ',' -f 1`
    NODE_PORT=`echo $NODE | cut -d ',' -f 2`
    ROLE=`echo $NODE | cut -d ',' -f 3`

    if [ "$ROLE" = "Z" ] ; then
      if [ "$NODE_IP" = "$LOCALHOST" ] ; then
        startLocalZooKeeper $NODE_IP $NODE_PORT > ${LOG_DIR}log_zookeeper 2>&1 &
        checkZooKeeperStartup $NODE_IP $NODE_PORT
      else
        startRemoteZooKeeper $NODE_IP $NODE_PORT > ${LOG_DIR}log_peer_zookeeper 2>&1 &
        checkZooKeeperStartup $NODE_IP $NODE_PORT
      fi
    elif [ "$ROLE" = "S" ] ; then
      NUMBER_OF_SUPERPEERS=$(($NUMBER_OF_SUPERPEERS + 1))
      if [ "$NODE_IP" = "$LOCALHOST" ] ; then
        copyLocalConfiguration
        startLocalSuperpeer $NODE_IP $NODE_PORT > ${LOG_DIR}log_superpeer_$NUMBER_OF_SUPERPEERS 2>&1 &
        checkSuperpeerStartup $NODE_IP $NODE_PORT
      else
        copyRemoteConfiguration $NODE_IP
        startRemoteSuperpeer $NODE_IP $NODE_PORT > ${LOG_DIR}log_superpeer_$NODE_IP 2>&1 &
        checkSuperpeerStartup $NODE_IP $NODE_PORT
      fi
    elif [ "$ROLE" = "P" ] ; then
      NUMBER_OF_PEERS=$(($NUMBER_OF_PEERS + 1))

      ITER=4
      while true ; do
        TMP=`echo $NODE | cut -d ',' -f $ITER`
        ITER=$((ITER + 1))

        TYPE=`echo $TMP | cut -d '=' -f 1`
        if [ "$TYPE" = "" ] ; then
          break
        elif [ "$TYPE" = "kvss" ] ; then
          RAM_SIZE_IN_GB=`echo $TMP | cut -d '=' -f 2`
        elif [ "$TYPE" = "class" ] ; then
          CLASS=`echo $TMP | cut -d '=' -f 2`
        elif [ "$TYPE" = "args" ] ; then
          ARGUMENTS=`echo $TMP | cut -d '=' -f 2`
        elif [ "$TYPE" = "cond" ] ; then
          CONDITION=`echo $TMP | cut -d '=' -f 2`
        elif [ "$TYPE" = "tcond" ] ; then
          TIME_CONDITION=`echo $TMP | cut -d '=' -f 2`
        fi  
      done

      if [ "$CLASS" = "" ] ; then
        CLASS=$DEFAULT_CLASS
      fi
      if [ "$CONDITION" = "" ] ; then
        CONDITION=$DEFAULT_CONDITION
      fi

      if [ "$NODE_IP" = "$LOCALHOST" ] ; then
        copyLocalConfiguration
        startLocalPeer $NODE_IP $NODE_PORT $RAM_SIZE_IN_GB $CLASS "$ARGUMENTS" > ${LOG_DIR}log_peer_$NUMBER_OF_PEERS 2>&1 &
        checkPeerStartup $NODE_IP $NODE_PORT $RAM_SIZE_IN_GB $CLASS "$ARGUMENTS" "$CONDITION"
      else
        copyRemoteConfiguration $NODE_IP
        startRemotePeer $NODE_IP $NODE_PORT $RAM_SIZE_IN_GB $CLASS "$ARGUMENTS" > ${LOG_DIR}log_peer_$NODE_IP 2>&1 &
        checkPeerStartup $NODE_IP $NODE_PORT $RAM_SIZE_IN_GB $CLASS "$ARGUMENTS" "$CONDITION"
      fi

      if [ "$TIME_CONDITION" != "" ] ; then
        echo "Waiting for $TIME_CONDITION seconds after initialization..."
        sleep "$TIME_CONDITION"
      fi
    elif [ "$ROLE" = "T" ] ; then
      SCRIPT=`echo $NODE | cut -d ',' -f 4`
      echo "Starting terminal ($NODE_IP $NODE_PORT $SCRIPT)"
      echo "Quit with ctrl+c or by typing \"quit\""
      echo "Output:"

      if [ "$NODE_IP" = "$LOCALHOST" ] ; then
        copyLocalConfiguration
        startLocalTerminal $NODE_IP $NODE_PORT $SCRIPT
      else
        copyRemoteConfiguration $NODE_IP
        startRemoteTerminal $NODE_IP $NODE_PORT $SCRIPT
      fi
    fi

  done <<< "$NODES"
}

close() {
  echo "Closing all dxram instances..."
  while read NODE || [[ -n "$NODE" ]]; do
    NODE_IP=`echo $NODE | cut -d ',' -f 1`
    NODE_PORT=`echo $NODE | cut -d ',' -f 2`
    ROLE=`echo $NODE | cut -d ',' -f 3`

    if [ "$ROLE" = "Z" ] ; then
      # Stop ZooKeeper?
      echo "ZooKeeper will stay alive"
    else
      if [ "$NODE_IP" = "$LOCALHOST" ] ; then
        kill `ps u | grep dxram | grep $NODE_PORT | cut -d " " -f 3`
      else
        ssh $IP - n "kill `ps u | grep dxram grep $NODE_PORT | cut -d " " -f 3`"
      fi
      sleep 1
    fi
  done <<< "$NODES"
}


###############
# Entry point #
###############
# Trim node file
NODE_FILE="./$1"
if [ "${NODE_FILE: -5}" != ".conf" ] ; then
  NODE_FILE="${NODE_FILE}.conf"
fi
NODES=`cat "$NODE_FILE" | grep -v '#'`

# Set execution paths
determineConfigurablePaths
EXECUTION_DIR="`pwd`/"
LOG_DIR="${EXECUTION_DIR}logs/"
CONFIG_FILE="$LOCAL_EXEC_PATH/config/dxram.json"

# Set default values
LOCALHOST="127.0.0.1"
DEFAULT_CLASS="de.hhu.bsinfo.dxram.run.DXRAMMain"
DEFAULT_CONDITION="Peer started."

# Print configuration
echo "##############################"
echo "Deploying $(echo $1 | cut -d '.' -f 1)"
echo "##############################"
echo "Local execution path: $LOCAL_EXEC_PATH"
echo "Remote execution path: $REMOTE_EXEC_PATH"
echo "Local ZooKeeper path: $LOCAL_ZOOKEEPER_PATH"
echo "Remote ZooKeeper path: $REMOTE_ZOOKEEPER_PATH"
echo -e "\n\n"

clean-up

writeConfiguration

execute

echo -e "\n\n"
close
echo "Exiting..."