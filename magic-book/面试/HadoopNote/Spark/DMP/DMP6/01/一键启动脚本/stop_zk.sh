#!/bin/sh
for host in node1 node2 node3
do
	echo "$host zk is stopping"
	ssh $host "source /etc/profile;nohup zkServer.sh stop >/dev/null 2>&1 &"

done
