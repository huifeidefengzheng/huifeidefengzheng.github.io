#!/bin/sh

echo "在主节点node3上启动以下服务"
ssh node03 service impala-state-store start
ssh node03 service impala-catalog start
ssh node03 service impala-server start
echo "在从节点node1和node2上启动impala-server"
ssh node02 service impala-server start
ssh node01 service impala-server start

echo "node01 查看impala进程是否存在"
ssh node01 ps -ef | grep impala
echo "node02 查看impala进程是否存在"
ssh node02 ps -ef | grep impala
echo "node03 查看impala进程是否存在"
ssh node03 ps -ef | grep impala