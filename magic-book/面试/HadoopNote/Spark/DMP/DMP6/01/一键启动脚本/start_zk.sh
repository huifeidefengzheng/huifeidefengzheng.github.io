#!/bin/sh
for host in node1 node2 node3
do
	ssh $host "source /etc/profile;nohup zkServer.sh start > /dev/null 2>&1 &"
	echo "$host zk is running"

done
