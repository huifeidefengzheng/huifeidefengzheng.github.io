#!/bin/sh
for host in node1 node2 node3
do
  ssh $host "source /etc/profile;nohup /export/servers/kafka/bin/kafka-server-stop.sh >/dev/null 2>&1 &" 
  echo "$host kafka is stopping"

done
