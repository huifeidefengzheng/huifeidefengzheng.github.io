#!/bin/sh
for host in node01 node02 node03 
do
  ssh $host "service kudu-master stop;service kudu-tserver stop"
  echo "$host is running"
done
