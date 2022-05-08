#!/bin/sh

echo "在主节点node3上停止以下服务"
ssh node03 service impala-state-store stop
ssh node03 service impala-catalog stop
ssh node03 service impala-server stop
echo "在从节点node1和node2上停止impala-server"
ssh node02 service impala-server stop
ssh node01 service impala-server stop
echo "node01 查看impala进程是否存在"
ssh node01 ps -ef | grep impala
echo "node02 查看impala进程是否存在"
ssh node02 ps -ef | grep impala
echo "node03 查看impala进程是否存在"
ssh node03 ps -ef | grep impala

