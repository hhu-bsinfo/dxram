#!/bin/bash


echo "##############################"
echo "Running Superpeer Overlay Test"
echo "##############################"

LOCAL_EXEC_PATH="../../"
REMOTE_EXEC_PATH="/home/beineke/dxram/"
LOCAL_ZOOKEEPER_PATH="/home/beineke/workspace/zookeeper/"
REMOTE_ZOOKEEPER_PATH="/home/beineke/zookeeper/"
LOCALHOST="127.0.0.1"
NODE_FILE="./$1.conf"
DEFAULT_CLASS="de.hhu.bsinfo.dxram.run.DXRAMMain"
EXECUTION_DIR=`pwd`

echo "Local execution path: $LOCAL_EXEC_PATH"
echo "Remote execution path: $REMOTE_EXEC_PATH"
echo "Local ZooKeeper path: $LOCAL_ZOOKEEPER_PATH"
echo "Remote ZooKeeper path: $REMOTE_EXEC_PATH"
echo "Localhost: $LOCALHOST"
echo "Configuration: $NODE_FILE"
echo "Default class: $DEFAULT_CLASS"
echo -e "\n\n"

startRemoteZooKeeper() {
  local IP=$1
  local PORT=$2
  nohup ssh $IP -n "sed -i \"s/clientPort=[0-9]*/clientPort=$PORT/g\" \"${REMOTE_ZOOKEEPER_PATH}conf/zoo.cfg\" && ${REMOTE_ZOOKEEPER_PATH}bin/zkServer.sh start"
}

startLocalZooKeeper() {
  local IP=$1 # IP is ignored
  local PORT=$2
  sed -i "s/clientPort=[0-9]*/clientPort=$PORT/g" "${LOCAL_ZOOKEEPER_PATH}conf/zoo.cfg"
  "${LOCAL_ZOOKEEPER_PATH}bin/zkServer.sh" start
}


startRemoteSuperpeer() {
  local IP=$1
  local PORT=$2

  cd "$REMOTE_EXEC_PATH"
  nohup ssh $IP -n "cd $REMOTE_EXEC_PATH && java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Superpeer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS"
  cd "$EXECUTION_DIR"
}

startLocalSuperpeer() {
  local IP=$1
  local PORT=$2

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Superpeer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS
  cd "$EXECUTION_DIR"
}


startRemotePeer() {
  local IP=$1
  local PORT=$2
  local CLASS=$3
  local ARGUMENTS="$4"

  cd "$REMOTE_EXEC_PATH"
  nohup ssh $IP -n "java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Peer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $CLASS $ARGUMENTS"
  cd "$EXECUTION_DIR"
}

startLocalPeer() {
  local IP=$1
  local PORT=$2
  local CLASS=$3
  local ARGUMENTS="$4"

  echo "$ARGUMENTS"

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Peer -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $CLASS $ARGUMENTS
  cd "$EXECUTION_DIR"
}


startRemoteTerminal() {
  local IP=$1
  local PORT=$2
  local SCRIPT=$3

  cd "$REMOTE_EXEC_PATH"
  ssh $IP - n "java -Dlog4j.configurationFile=config/log4j.xml -Ddxram.config=config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Terminal -cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:DXRAM.jar $DEFAULT_CLASS < $SCRIPT"
  cd "$EXECUTION_DIR"
}

startLocalTerminal() {
  local IP=$1
  local PORT=$2
  local SCRIPT=$3

  cd "$LOCAL_EXEC_PATH"
  java -Dlog4j.configurationFile=${LOCAL_EXEC_PATH}config/log4j.xml -Ddxram.config=${LOCAL_EXEC_PATH}config/dxram.json -Ddxram.m_engineSettings.m_address.m_ip=$IP -Ddxram.m_engineSettings.m_address.m_port=$PORT -Ddxram.m_engineSettings.m_role=Terminal -cp ${LOCAL_EXEC_PATH}lib/slf4j-api-1.6.1.jar:${LOCAL_EXEC_PATH}lib/zookeeper-3.4.3.jar:${LOCAL_EXEC_PATH}lib/gson-2.7.jar:${LOCAL_EXEC_PATH}lib/log4j-api-2.7.jar:${LOCAL_EXEC_PATH}lib/log4j-core-2.7.jar:${LOCAL_EXEC_PATH}DXRAM.jar $DEFAULT_CLASS < $SCRIPT
  cd "$EXECUTION_DIR"
}


rm -f nohup.out
rm -f log_*

##################################
NUMBER_OF_SUPERPEERS=0
NUMBER_OF_PEERS=0

while read node; do
  NODE_IP=`echo $node | cut -d ' ' -f 1`
  NODE_PORT=`echo $node | cut -d ' ' -f 2`
  ROLE=`echo $node | cut -d ' ' -f 3`

  if [ "$ROLE" = "S" ] ; then
    NUMBER_OF_SUPERPEERS=$(($NUMBER_OF_SUPERPEERS + 1))
    if [ "$NODE_IP" = "$LOCALHOST" ] ; then
      startLocalSuperpeer $NODE_IP $NODE_PORT > log_superpeer_$NUMBER_OF_SUPERPEERS 2>&1 &
    else
      startRemoteSuperpeer $NODE_IP $NODE_PORT > log_superpeer_$NODE_IP 2>&1 &
    fi
    sleep 2.5
    echo "Superpeer ($NODE_IP $NODE_PORT) started"
  elif [ "$ROLE" = "P" ] ; then
    NUMBER_OF_PEERS=$(($NUMBER_OF_PEERS + 1))

    CLASS=`echo $node | cut -d ' ' -f 4`
    if [ "$CLASS" = "" ] ; then
      CLASS=$DEFAULT_CLASS
    fi

    ARGUMENTS=`echo $node | cut -d \' -f 2`

    if [ "$NODE_IP" = "$LOCALHOST" ] ; then
      startLocalPeer $NODE_IP $NODE_PORT $CLASS "$ARGUMENTS" > log_peer_$NUMBER_OF_PEERS 2>&1 &
    else
      startRemotePeer $NODE_IP $NODE_PORT $CLASS "$ARGUMENTS" > log_peer_$NODE_IP 2>&1 &
    fi
    sleep 2.5
    echo "Peer ($NODE_IP $NODE_PORT $CLASS $ARGUMENTS) started"
  elif [ "$ROLE" = "T" ] ; then
    echo "Number of superpeers: $NUMBER_OF_SUPERPEERS"
    echo "Number of peers: $NUMBER_OF_PEERS"
    echo "Waiting..."

    sleep 10.0

    SCRIPT=`echo $node | cut -d ' ' -f 4`
    echo "Starting terminal ($NODE_IP $NODE_PORT $SCRIPT)..."
    echo "Output:"

    if [ "$NODE_IP" = "$LOCALHOST" ] ; then
      startLocalTerminal $NODE_IP $NODE_PORT $SCRIPT
    else
      startRemoteTerminal $NODE_IP $NODE_PORT $SCRIPT
    fi
  elif [ "$ROLE" = "Z" ] ; then
      if [ "$NODE_IP" = "$LOCALHOST" ] ; then
      startLocalZooKeeper $NODE_IP $NODE_PORT
    else
      startRemoteZooKeeper $NODE_IP $NODE_PORT
    fi
    sleep 5
    echo -e "\nZooKeeper ($NODE_IP $NODE_PORT) started"
  fi

done < $NODE_FILE

echo -e "\nWaiting..."

sleep 10.0

echo "Closing all dxram instances..."
while read node; do
  NODE_IP=`echo $node | cut -d ' ' -f 1`
  NODE_PORT=`echo $node | cut -d ' ' -f 2`
  ROLE=`echo $node | cut -d ' ' -f 3`

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

done < $NODE_FILE

echo "Exiting..."
