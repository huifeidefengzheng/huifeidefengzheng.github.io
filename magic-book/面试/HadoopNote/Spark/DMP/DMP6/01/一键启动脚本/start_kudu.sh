#!/bin/sh
for host in node01 node02 node03 
do
  echo "$host 正在重启时间同步-启动kudu"
  ssh $host "service ntpd start;service kudu-master start;service kudu-tserver start"
  echo "$host is running"
done
