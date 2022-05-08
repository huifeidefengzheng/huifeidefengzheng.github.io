---
title: Hadoop高可用+联邦(HA+Federation)
date: 2019/8/4 08:16:25
updated: 2019/8/4 21:52:30
comments: true
tags:
     hadoop
categories: 
     - 项目
     - Hadoop
---

## Hadoop高可用+联邦(HA+Federation)

### 0. 步骤概述

1). 为高可用保存hadoop配置
2). 增加federation配置
3). 首次启动HA+Federation集群part1：启动journalnode和zookeeper，格式化zookeeper集群
4). 首次启动HA+Federation集群part2：格式化第一组的namenode，即hadoop1
5). 首次启动HA+Federation集群part3：格式化第一组的namenode，即hadoop3
6). 首次启动HA+Federation集群part4：启动ZKFC, datanode和yarn
7). 常规启动HA+Federation集群
8). 在HA+Federation集群上测试wordcount程序
9). 为HA+Federation(高可用+联邦)配置viewfs
10). 在HA+Federation+viewFs集群上测试wordcount程序

### 1. 为高可用保存hadoop配置

1. 为高可用保存hadoop配置
1.1 进入`$HADOOP_HOME/etc/`目录

```shell
[root@hadoop1 ~]# cd /opt/test/hadoop-2.6.5/etc
```

1.2 备份hadoop高可用配置，供以后使用

```shell
[root@hadoop1 etc]# cp -r hadoop/ hadoop-ha
```

1.3 查看`$HADOOP_HOME/etc/`目录，备份成功

```shell
[root@hadoop1 etc]# ls
hadoop hadoop-full hadoop-ha
```

## hadoop-full保留了已有配置，接下来高可用的配置继续在hadoop文件夹内修改

### 2. 增加federation配置

2.增加federation配置

2.0 在hadoop1上进入`$HADOOP_HOME/etc/hadoop`目录

```shell
[root@hadoop1 ~]# cd /opt/test/hadoop-2.6.5/etc/hadoop
```

2.1 在hadoop1上修改hdfs-site.xml文件，将原有配置替换如下

```shell
[root@hadoop1 hadoop]# vim hdfs-site.xml
```

```xml
<configuration>
<property>
   <name>dfs.replication</name>
   <value>3</value>
</property>
<!--定义nameservices逻辑名称-->
<property>
  <name>dfs.nameservices</name>
  <value>mycluster,mycluster2</value>
</property>
<!--映射nameservices逻辑名称到namenode逻辑名称-->
<property>
  <name>dfs.ha.namenodes.mycluster</name>
  <value>nn1,nn2</value>
</property>
<property>
  <name>dfs.ha.namenodes.mycluster2</name>
  <value>nn3,nn4</value>
</property>

<!--映射namenode逻辑名称到真实主机名称(RPC) mycluster -->
<property>
  <name>dfs.namenode.rpc-address.mycluster.nn1</name>
  <value>hadoop1:8020</value>
</property>
<property>
  <name>dfs.namenode.rpc-address.mycluster.nn2</name>
  <value>hadoop2:8020</value>
</property>
<!--映射namenode逻辑名称到真实主机名称(RPC) mycluster2 -->
<property>
  <name>dfs.namenode.rpc-address.mycluster2.nn3</name>
  <value>hadoop3:8020</value>
</property>
<property>
  <name>dfs.namenode.rpc-address.mycluster2.nn4</name>
  <value>hadoop4:8020</value>
</property>

<!--映射namenode逻辑名称到真实主机名称(HTTP) mycluster-->
<property>
  <name>dfs.namenode.http-address.mycluster.nn1</name>
  <value>hadoop1:50070</value>
</property>
<property>
  <name>dfs.namenode.http-address.mycluster.nn2</name>
  <value>hadoop2:50070</value>
</property>
<!--映射namenode逻辑名称到真实主机名称(HTTP) mycluster2-->
<property>
  <name>dfs.namenode.http-address.mycluster2.nn3</name>
  <value>hadoop3:50070</value>
</property>
<property>
  <name>dfs.namenode.http-address.mycluster2.nn4</name>
  <value>hadoop4:50070</value>
</property>
<!--配置journalnode集群位置信息及目录-->
<property>
  <name>dfs.namenode.shared.edits.dir</name>
<value>qjournal://hadoop1:8485;hadoop2:8485;hadoop3:8485/mycluster</value>
</property>
<property>
  <name>dfs.journalnode.edits.dir</name>
  <value>/var/test/hadoop/fed/jn</value>
</property>
<!--配置故障切换实现类-->
<property>
  <name>dfs.client.failover.proxy.provider.mycluster</name>
<value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
</property>
<property>
  <name>dfs.client.failover.proxy.provider.mycluster2</name>
<value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
</property>
<!--指定切换方式为SSH免密钥方式-->
<property>
  <name>dfs.ha.fencing.methods</name>
  <value>sshfence</value>
</property>
<property>
  <name>dfs.ha.fencing.ssh.private-key-files</name>
  <value>/root/.ssh/id_dsa</value>
</property>
<!--设置自动切换-->
<property>
   <name>dfs.ha.automatic-failover.enabled.mycluster</name>
   <value>true</value>
</property>
<property>
   <name>dfs.ha.automatic-failover.enabled.mycluster2</name>
   <value>true</value>
</property>
</configuration>
```

2.2 在hadoop1上修改core-site.xml文件，将原有配置替换如下

```shell
[root@hadoop1 hadoop]# vim core-site.xml
```

```xml
<configuration>
<property>
    <name>fs.defaultFS</name>
        <value>hdfs://mycluster</value>
</property>
<!--设置zookeeper数据存放目录-->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/var/test/hadoop/fed</value>
</property>
<!--设置zookeeper位置信息-->
<property>
        <name>ha.zookeeper.quorum</name>
        <value>hadoop2:2181,hadoop3:2181,hadoop4:2181</value>
    </property>
</configuration>
```

2.3 在hadoop1上操作，将更新后的hdfs-site.xml,core-site.xml分发到其他节点

```shell
[root@hadoop1 hadoop]# scp hdfs-site.xml core-site.xml hadoop2:`pwd`
[root@hadoop1 hadoop]# scp hdfs-site.xml core-site.xml hadoop3:`pwd`
[root@hadoop1 hadoop]# scp hdfs-site.xml core-site.xml hadoop4:`pwd`
```

2.4 修改hadoop3上的hdfs-site.xml和core-site.xml文件
2.4.1 进入$HADOOP_HOME/etc/hadoop目录

```shell
[root@hadoop3 ~]# cd /opt/test/hadoop-2.6.5/etc/hadoop
```

2.4.2 在hadoop3上修改hdfs-site.xml文件的dfs.namenode.shared.edits.dir属性

```shell
[root@hadoop3 hadoop]# vim hdfs-site.xml
```

```xml
<!--配置journalnode集群位置信息及目录-->
<property>
  <name>dfs.namenode.shared.edits.dir</name>
<value>qjournal://hadoop1:8485;hadoop2:8485;hadoop3:8485/mycluster2</value>
</property>
```

2.4.3 在haoop3上修改core-site.xml文件的fs.defaultFS属性

```shell
[root@hadoop3 hadoop]# vim core-site.xml
```

```xml
<property>
    <name>fs.defaultFS</name>
        <value>hdfs://mycluster2</value>
</property>
```

### 3. 首次启动HA+Federation集群part1：启动journalnode和zookeeper，格式化zookeeper集群

3.1 在hadoop1,hadoop2,hadoop3上启动journalnode

```shell
[root@hadoop1 ~]# hadoop-daemon.sh start journalnode
[root@hadoop2 ~]# hadoop-daemon.sh start journalnode
[root@hadoop3 ~]# hadoop-daemon.sh start journalnode
```

3.1.1 hadoop1, hadoop2, hadoop3, hadoop4进程显示如下

```shell
[root@hadoop1 ~]# jps
[root@hadoop2 ~]# jps
[root@hadoop3 ~]# jps
1*** JournalNode
1*** Jps

[root@hadoop3 ~]# jps
jps

```

3.2 在hadoop2,hadoop3,hadoop4上分别启动zookeeper

```shell
[root@hadoop2 ~]# zkServer.sh start
[root@hadoop3 ~]# zkServer.sh start
[root@hadoop4 ~]# zkServer.sh start
```

3.3 在hadoop1和hadoop3上格式化zookeeper

```shell
[root@hadoop1 ~]# hdfs zkfc -formatZK
[root@hadoop3 ~]# hdfs zkfc -formatZK
```

3.3.1 格式化zookeeper后在hadoop2,hadoop3,hadoop4查看zookeeper进程

```shell
[root@hadoop2 ~]# zkCli.sh
[root@hadoop2 ~]# zkCli.sh
[root@hadoop2 ~]# zkCli.sh
Connecting to localhost:2181

[zk: localhost:2181(CONNECTED) 0] ls /
[hadoop-ha, zookeeper]
[zk: localhost:2181(CONNECTED) 1] ls /hadoop-ha
[mycluster, mycluster2]
[zk: localhost:2181(CONNECTED) 2]
```

### 4. 首次启动HA+Federation集群part2：格式化第一组的namenode，即hadoop1

4.1 在hadoop1上操作，指定clusterid格式化namenode,
4.1.1 命令

```shell
hadoop namenode -format -clusterid ${CLUSTER_ID}${CLUSTER_ID}为自行指定的clusterID，本例中使用cluster1
```

4.1.2 执行命令

```shell
[root@hadoop1 ~]# hadoop namenode -format -clusterid cluster1
```

4.2 格式化完成后在hadoop1上启动namenode

```shell
[root@hadoop1 ~]# hadoop-daemon.sh start namenode
starting namenode, logging to /opt/test/hadoop-2.6.5/logs/hadoop-root-namenode-hadoop1.out
```

4.3 hadoop1进程显示如下

```shell
[root@hadoop1 ~]# jps
**** Jps
**** JournalNode
**** NameNode
```

4.4 在hadoop2，即另一台namenode上同步hadoop1的CID等信息

```shell
[root@hadoop2 ~]# hdfs namenode -bootstrapStandby
```

4.5 在备用namenode，即hadoop2上启动namenode

```shell
[root@hadoop2 ~]# hadoop-daemon.sh start namenode
```

4.5.1 在hadoop2上查看进程

```shell
[root@hadoop2 ~]# jps
1406 JournalNode
1476 QuorumPeerMain
1710 NameNode
1791 Jps
```

### 5. 首次启动HA+Federation集群part3：格式化第一组的namenode，即hadoop3

备注：步骤4和步骤5除操作的虚拟机不同，过程完全相同
5.1 在hadoop3上操作，指定clusterid格式化namenode,

```shell
[root@hadoop3 ~]# hadoop namenode -format -clusterid cluster1
```

5.2 格式化完成后在hadoop3上启动namenode

```shell
[root@hadoop3 ~]# hadoop-daemon.sh start namenode
starting namenode, logging to /opt/test/hadoop-2.6.5/logs/hadoop-root-namenode-hadoop3.out
```

5.3 hadoop3进程显示如下

```shell
[root@hadoop3 ~]# jps
**** Jps
**** JournalNode
**** NameNode
```

5.4 在hadoop4，即该组另一台namenode上同步hadoop3的CID等信息

```shell
[root@hadoop4 ~]# hdfs namenode -bootstrapStandby
```

5.5 在备用namenode上启动namenode

```shell
[root@hadoop4 ~]# hadoop-daemon.sh start namenode
```

5.5.1 在hadoop4上查看进程

```shell
[root@hadoop4 ~]# jps
1407 QuorumPeerMain
1615 NameNode
1696 Jps
```

### 6. 首次启动HA+Federation集群part4：启动ZKFC, datanode和yarn

6.1 启动zkfc
6.1.1 在hadoop1,hadoop2,hadoop3,hadoop4上启动zkfc

```shell
[root@hadoop1 ~]# hadoop-daemon.sh start zkfc
[root@hadoop2 ~]# hadoop-daemon.sh start zkfc
[root@hadoop3 ~]# hadoop-daemon.sh start zkfc
[root@hadoop4 ~]# hadoop-daemon.sh start zkfc
starting zkfc, logging to /opt/test/hadoop-2.6.5/logs/hadoop-root-zkfc-hadoop*.out
```

6.1.2 启动zkfc后在hadoop2,hadoop3,hadoop4查看已有进程

```shell
[root@hadoop1 ~]# jps1404 JournalNode1727 DFSZKFailoverController1601 NameNode1794 Jps
[root@hadoop2 ~]# jps1668 Jps1406 JournalNode1476 QuorumPeerMain1623 DFSZKFailoverController
[root@hadoop3 ~]# jps1836 Jps1404 JournalNode1474 QuorumPeerMain1664 NameNode1769 DFSZKFailoverController
[root@hadoop4 ~]# jps1407 QuorumPeerMain1546 DFSZKFailoverController1591 Jps
```

6.2 启动datanode
6.2.1 在active的namenode上启动datanode

```shell
[root@hadoop1 ~]# hadoop-daemons.sh start datanode
```

因为hadoop1已经启动了所有datanode，不用在hadoop3上重复启动
6.2.2 启动datanode后查看hadoop1,hadoop2,hadoop3,hadoop4进程

```shell
[root@hadoop1 ~]# jps1404 JournalNode1885 Jps1727 DFSZKFailoverController1601 NameNode
[root@hadoop2 ~]# jps1406 JournalNode1476 QuorumPeerMain2007 Jps1710 NameNode1911 DataNode1623 DFSZKFailoverController
[root@hadoop3 ~]# jps1404 JournalNode1991 Jps1474 QuorumPeerMain1664 NameNode1904 DataNode1769 DFSZKFailoverController
[root@hadoop4 ~]# jps1407 QuorumPeerMain1546 DFSZKFailoverController1615 NameNode1811 DataNode1908 Jps
```

6.3 启动yarn
6.3.1 在hadoop1上启动yarn

```shell
[root@hadoop1 ~]# start-yarn.sh
```

6.3.2 启动yarn后查看hadoop1, hadoop2, hadoop3, hadoop4进程

```shell
[root@hadoop1 ~]# jps


1404 JournalNode
1727 DFSZKFailoverController
1601 NameNode
1947 ResourceManager
2199 Jps

[root@hadoop2 ~]# jps
1406 JournalNode
1476 QuorumPeerMain
2114 Jps
1710 NameNode
1911 DataNode
1623 DFSZKFailoverController
2078 NodeManager

[root@hadoop3 ~]# jps
1404 JournalNode
2035 NodeManager
2068 Jps
1474 QuorumPeerMain
1664 NameNode
1904 DataNode
1769 DFSZKFailoverController

[root@hadoop4 ~]# jps
1407 QuorumPeerMain
2035 Jps
1546 DFSZKFailoverController
1970 NodeManager
1615 NameNode
1811 DataNode
```

6.4 通过web查看集群运行状态,

```text
http://192.168.111.211:50070/dfshealth.html#tab-overview'hadoop1:8020' (active)
http://192.168.111.212:50070/dfshealth.html#tab-overview'hadoop1:8020' (standby)
http://192.168.111.213:50070/dfshealth.html#tab-overview'hadoop1:8020' (active)
http://192.168.111.214:50070/dfshealth.html#tab-overview'hadoop1:8020' (standby)
```

### 7. 常规启动HA+Federation集群

7.1 在hadoop2, hadoop3, hadoop4上启动zookeeper

```shell
[root@hadoop2 ~]# zkServer.sh start
[root@hadoop3 ~]# zkServer.sh start
[root@hadoop4 ~]# zkServer.sh start
JMX enabled by default
Using config: /opt/test/zookeeper-3.4.6/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

7.1.1 在hadoop2, hadoop3, hadoop4上查看进程

```shell
[root@hadoop2 ~]# jps
[root@hadoop3 ~]# jps
[root@hadoop4 ~]# jps
2*** Jps
2*** QuorumPeerMain
```

7.2 在hadoop1上执行start-dfs.sh

```shell
[root@hadoop1 ~]# start-dfs.sh
Starting namenodes on [hadoop1 hadoop2 hadoop3 hadoop4]
hadoop*: starting namenode, logging to /opt/test/hadoop-2.6.5/logs/hadoop-root-namenode-hadoop*.out
hadoop*: starting datanode, logging to /opt/test/hadoop-2.6.5/logs/hadoop-root-datanode-hadoop*.out
Starting journal nodes [hadoop1 hadoop2 hadoop3]
Hadoop*: starting journalnode, logging to /opt/test/hadoop-2.6.5/logs/hadoop-root-journalnode-hadoop*.out
```

7.2.1 在hadoop1, hadoop2, hadoop3, hadoop4上查看进程

```shell
[root@hadoop1 mapreduce]# jps
3243 Jps
2926 NameNode
3121 JournalNode

[root@hadoop2 ~]# jps
2565 Jps
2240 QuorumPeerMain
2305 NameNode
2462 JournalNode
2371 DataNode

[root@hadoop3 ~]# jps
3095 Jps
2767 QuorumPeerMain
2832 NameNode
2989 JournalNode
2901 DataNode

[root@hadoop4 ~]# jps
2389 Jps
2133 QuorumPeerMain
2204 NameNode
2270 DataNode

```

7.3 在hadoop1上启动yarn

```shell
[root@hadoop1 ~]# start-yarn.sh
starting yarn daemons
starting resourcemanager, logging to /opt/test/hadoop-2.6.5/logs/yarn-root-resourcemanager-hadoop1.out
hadoop*: starting nodemanager, logging to /opt/test/hadoop-2.6.5/logs/yarn-root-nodemanager-hadoop*.out
```

7.3.1 在hadoop1,hadoop2,hadoop3,hadoop4上查看进程

```shell
[root@hadoop1 mapreduce]# jps
3588 Jps
2926 NameNode
3121 JournalNode
3313 ResourceManager

[root@hadoop2 ~]# jps
2773 Jps
2240 QuorumPeerMain
2305 NameNode
2462 JournalNode
2371 DataNode
2626 NodeManager

[root@hadoop3 ~]# jps
3308 Jps
2767 QuorumPeerMain
2832 NameNode
2989 JournalNode
2901 DataNode
3155 NodeManager

[root@hadoop4 ~]# jps
2598 Jps
2133 QuorumPeerMain
2204 NameNode
2270 DataNode
2451 NodeManager
```

7.4 在hadoop1,hadoop2,hadoop3,hadoop4上启动zkfc

```shell
[root@hadoop1 ~]# hadoop-daemon.sh start zkfc
[root@hadoop2 ~]# hadoop-daemon.sh start zkfc
[root@hadoop3 ~]# hadoop-daemon.sh start zkfc
[root@hadoop4 ~]# hadoop-daemon.sh start zkfc
starting zkfc, logging to /opt/test/hadoop-2.6.5/logs/hadoop-root-zkfc-hadoop*.out
```

7.4.1 在hadoop1,hadoop2,hadoop3,hadoop4上查看进程

```shell
[root@hadoop1 mapreduce]# jps
3588 Jps
2926 NameNode
3121 JournalNode
3313 ResourceManager
3641 DFSZKFailoverController

[root@hadoop2 ~]# jps
2773 Jps
2240 QuorumPeerMain
2305 NameNode
2462 JournalNode
2371 DataNode
2626 NodeManager
2826 DFSZKFailoverController

[root@hadoop3 ~]# jps
3308 Jps
2767 QuorumPeerMain
2832 NameNode
2989 JournalNode
2901 DataNode
3155 NodeManager
3362 DFSZKFailoverController

[root@hadoop4 ~]# jps
2598 Jps
2133 QuorumPeerMain
2204 NameNode
2270 DataNode
2451 NodeManager
2651 DFSZKFailoverController
```

7.5 通过web查看集群运行状态,

```text
http://192.168.111.211:50070/dfshealth.html#tab-overview'hadoop1:8020' (active)
http://192.168.111.212:50070/dfshealth.html#tab-overview'hadoop1:8020' (standby)
http://192.168.111.213:50070/dfshealth.html#tab-overview'hadoop1:8020' (active)
http://192.168.111.214:50070/dfshealth.html#tab-overview'hadoop1:8020' (standby)
```

### 8. 在HA+Federation集群上测试wordcount程序

8.1 从hadoop1或hadoop3进入$HADOOP_HOME/share/hadoop/mapreduce/
目录，本例选择hadoop1

```shell
[root@hadoop1 ~]# cd /opt/test/hadoop-2.6.5/share/hadoop/mapreduce/
```

8.2 上传test.txt文件到根目录
8.2.1 默认上传

```shell
[root@hadoop1 mapreduce]# hadoop fs -put test.txt /
```

8.2.2 也可以指定blocksize

```shell
[root@hadoop1 mapreduce]# hdfs dfs -D dfs.blocksize=1048576 -put test.txt /
```

8.3 运行wordcount测试程序，输出到/output

```shell
[root@hadoop1 mapreduce]# hadoop jar hadoop-mapreduce-examples-2.6.5.jar wordcount /test.txt /output #运行时会首先看到如下信息
INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
```

8.4 查看mapreduce运行结果

```shell
[root@hadoop1 mapreduce]# hadoop dfs -text /output/part-*
hello    100003
world    200002
“hello    100000
```

### 9. 为HA+Federation(高可用+联邦)配置viewfs

9.1 进入`$HADOOP_HOME/etc/hadoop`目录

```shell
[root@hadoop1 ~]# cd /opt/test/hadoop-2.6.5/etc/hadoop
```

9.2 在hadoop1上修改core-site.xml文件，将原有配置替换如下

```shell
[root@hadoop1 hadoop]# vim core-site.xml
```

```xml
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">
<!--cmt.xml前使用绝对路径-->
<xi:include href="/opt/test/hadoop-2.6.5/etc/hadoop/cmt.xml" />
<property>
        <name>fs.default.name</name>
        <value>viewfs://clusterX</value>
</property>
<!--设置zookeeper数据存放目录-->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/var/test/hadoop/fed</value>
</property>
<!--设置zookeeper位置信息-->
<property>
        <name>ha.zookeeper.quorum</name>
        <value>hadoop2:2181,hadoop3:2181,hadoop4:2181</value>
    </property>
</configuration>
9.3 在hadoop1的/opt/test/hadoop-2.6.5/etc/hadoop目录下新增cmt.xml文件
[root@hadoop1 hadoop]# vim cmt.xml
<configuration>
  <property>
    <name>fs.viewfs.mounttable.clusterX.link./ns1</name>
    <value>hdfs://mycluster</value>
  </property>
  <property>
    <name>fs.viewfs.mounttable.clusterX.link./ns2</name>
    <value>hdfs://mycluster2</value>
  </property>
<property>
    <!-- 指定 /tmp 目录，许多依赖hdfs的组件可能会用到此目录 -->
    <name>fs.viewfs.mounttable.clusterX.link./tmp</name>  
    <value>hdfs://mycluster/tmp</value>
  </property>
</configuration>
```

### 10. 在HA+Federation+viewFs集群上测试wordcount程序

10.1 从hadoop1或hadoop3进入$HADOOP_HOME/share/hadoop/mapreduce/目录，本例选择hadoop1

```shell
[root@hadoop1 ~]# cd /opt/test/hadoop-2.6.5/share/hadoop/mapreduce/
```

10.2上传test.txt文件到根目录
10.2.1 默认上传

```shell
[root@hadoop1 mapreduce]# hadoop fs -put test.txt /
```

10.2.2 也可以指定blocksize

```shell
[root@hadoop1 mapreduce]# hdfs dfs -D dfs.blocksize=1048576 -put test.txt /
```

10.3 运行wordcount测试程序，输出到/output

```shell
[root@hadoop1 mapreduce]#
hadoop jar hadoop-mapreduce-examples-2.6.5.jar wordcount /ns1/test.txt /ns1/output

# 运行时会首先看到如下信息
INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
```

10.4 查看mapreduce运行结果

```shell
[root@hadoop1 mapreduce]# hadoop dfs -text /ns1/output/part-*
hello    100003
world    200002
“hello    100000
```

`
