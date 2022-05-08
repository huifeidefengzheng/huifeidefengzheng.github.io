#!/bin/sh
nohup  /export/servers/storm/bin/storm ui >/dev/null 2>&1 &
for host in node1 node2 node3
do
        ssh $host "source /etc/profile;nohup  /export/servers/storm/bin/storm nimbus >/dev/null 2>&1 & nohup  /export/servers/storm/bin/storm supervisor >/dev/null 2>&1 &" 
	echo "$host storm is running"

done
