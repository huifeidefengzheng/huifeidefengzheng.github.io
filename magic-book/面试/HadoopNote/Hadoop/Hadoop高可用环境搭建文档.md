---
title: hadoop分布式环境搭建
date: 2019/8/4 08:16:25
updated: 2019/8/4 21:52:30
comments: true
tags:
     hadoop
categories: 
     - 项目
     - Hadoop
---

### 分布式环境搭建（适用于工作当中正式环境搭建）

使用完全分布式，实现namenode高可用，ResourceManager的高可用

集群运行服务规划

|             | 192.168.174.100 | 192.168.174.110 | 192.168.174.120  |
| ----------- | --------------- | --------------- | ---------------- |
| zookeeper   | zk              | zk              | zk               |
| HDFS        | JournalNode     | JournalNode     | JournalNode      |
| NameNode    | NameNode        |                 |                  |
| ZKFC        | ZKFC            |                 |                  |
| DataNode    | DataNode        | DataNode        |                  |
| YARN        |                 | ResourceManager | ResourceManager  |
| NodeManager | NodeManager     | NodeManager     |                  |
| MapReduce   |                 |                 | JobHistoryServer |

![image-20190805115723549](Hadoop高可用环境搭建文档/image-20190805115723549.png)

#### 安装包解压

停止之前的hadoop集群的所有服务，并删除所有机器的hadoop安装包，然后重新解压hadoop压缩包
解压压缩包

第一台机器执行以下命令进行解压

```shell
cd /export/softwares

tar -zxvf hadoop-2.7.5.tar.gz -C ../servers/
```

#### 配置文件的修改

##### 修改core-site.xml

第一台机器执行以下命令

```shell
#修改core-site.xml
#第一台机器执行以下命令
cd /export/servers/hadoop-2.7.5/etc/hadoop
vim core-site.xml
<configuration>
<!-- 指定NameNode的HA高可用的zk地址  -->
    <property>
        <name>ha.zookeeper.quorum</name>
        <value>node01:2181,node02:2181,node03:2181</value>
    </property>
 <!-- 指定HDFS访问的域名地址  -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://ns</value>
    </property>
 <!-- 临时文件存储目录  -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/export/servers/hadoop-2.7.5/data/tmp</value>
    </property>
     <!-- 开启hdfs垃圾箱机制，指定垃圾箱中的文件七天之后就彻底删掉
            单位为分钟
     -->
    <property>
        <name>fs.trash.interval</name>
        <value>10080</value>
    </property>
</configuration>
```

##### 修改hdfs-site.xml

第一台机器执行以下命令

```shell
#修改hdfs-site.xml
#第一台机器执行以下命令
cd /export/servers/hadoop-2.7.5/etc/hadoop
vim hdfs-site.xml
```

```xml
<configuration>
<!--  指定命名空间  -->
    <property>
        <name>dfs.nameservices</name>
        <value>ns</value>
    </property>
<!--  指定该命名空间下的两个机器作为我们的NameNode  -->
    <property>
        <name>dfs.ha.namenodes.ns</name>
        <value>nn1,nn2</value>
    </property>

    <!-- 配置第一台服务器的namenode通信地址  -->
    <property>
        <name>dfs.namenode.rpc-address.ns.nn1</name>
        <value>node01:8020</value>
    </property>
    <!--  配置第二台服务器的namenode通信地址  -->
    <property>
        <name>dfs.namenode.rpc-address.ns.nn2</name>
        <value>node02:8020</value>
    </property>
    <!-- 所有从节点之间相互通信端口地址 -->
    <property>
        <name>dfs.namenode.servicerpc-address.ns.nn1</name>
        <value>node01:8022</value>
    </property>
    <!-- 所有从节点之间相互通信端口地址 -->
    <property>
        <name>dfs.namenode.servicerpc-address.ns.nn2</name>
        <value>node02:8022</value>
    </property>

    <!-- 第一台服务器namenode的web访问地址  -->
    <property>
        <name>dfs.namenode.http-address.ns.nn1</name>
        <value>node01:50070</value>
    </property>
    <!-- 第二台服务器namenode的web访问地址  -->
    <property>
        <name>dfs.namenode.http-address.ns.nn2</name>
        <value>node02:50070</value>
    </property>
    <!-- journalNode的访问地址，注意这个地址一定要配置 -->
    <property>
        <name>dfs.namenode.shared.edits.dir</name>
        <value>qjournal://node01:8485;node02:8485;node03:8485/ns1</value>
    </property>
    <!--  指定故障自动恢复使用的哪个java类 -->
    <property>
        <name>dfs.client.failover.proxy.provider.ns</name>
        <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
    </property>
    <!-- 故障转移使用的哪种通信机制 -->
    <property>
        <name>dfs.ha.fencing.methods</name>
        <value>sshfence</value>
    </property>

    <!-- 指定通信使用的公钥  -->
    <property>
        <name>dfs.ha.fencing.ssh.private-key-files</name>
        <value>/root/.ssh/id_rsa</value>
    </property>
    <!-- journalNode数据存放地址  -->
    <property>
        <name>dfs.journalnode.edits.dir</name>
        <value>/export/servers/hadoop-2.7.5/data/dfs/jn</value>
    </property>
    <!-- 启用自动故障恢复功能 -->
    <property>
        <name>dfs.ha.automatic-failover.enabled</name>
        <value>true</value>
    </property>
    <!-- namenode产生的文件存放路径 -->
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///export/servers/hadoop-2.7.5/data/dfs/nn/name</value>
    </property>
    <!-- edits产生的文件存放路径 -->
    <property>
        <name>dfs.namenode.edits.dir</name>
        <value>file:///export/servers/hadoop-2.7.5/data/dfs/nn/edits</value>
    </property>
    <!-- dataNode文件存放路径 -->
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///export/servers/hadoop-2.7.5/data/dfs/dn</value>
    </property>
    <!-- 关闭hdfs的文件权限 -->
    <property>
        <name>dfs.permissions</name>
        <value>false</value>
    </property>
    <!-- 指定block文件块的大小 -->
    <property>
        <name>dfs.blocksize</name>
        <value>134217728</value>
    </property>
</configuration>
```

##### 修改yarn-site.xml，注意node03与node02配置不同

第一台机器执行以下命令

```shell
cd /export/servers/hadoop-2.7.5/etc/hadoop
vim yarn-site.xml
```

```xml
<configuration>
<!-- Site specific YARN configuration properties -->
<!-- 是否启用日志聚合.应用程序完成后,日志汇总收集每个容器的日志,这些日志移动到文件系统,例如HDFS. -->
<!-- 用户可以通过配置"yarn.nodemanager.remote-app-log-dir"、"yarn.nodemanager.remote-app-log-dir-suffix"来确定日志移动到的位置 -->
<!-- 用户可以通过应用程序时间服务器访问日志 -->

<!-- 启用日志聚合功能，应用程序完成后，收集各个节点的日志到一起便于查看 -->
    <property>
            <name>yarn.log-aggregation-enable</name>
            <value>true</value>
    </property>

<!--开启resource manager HA,默认为false-->
<property>
        <name>yarn.resourcemanager.ha.enabled</name>
        <value>true</value>
</property>
<!-- 集群的Id，使用该值确保RM不会做为其它集群的active -->
<property>
        <name>yarn.resourcemanager.cluster-id</name>
        <value>mycluster</value>
</property>
<!--配置resource manager  命名-->
<property>
        <name>yarn.resourcemanager.ha.rm-ids</name>
        <value>rm1,rm2</value>
</property>
<!-- 配置第一台机器的resourceManager -->
<property>
        <name>yarn.resourcemanager.hostname.rm1</name>
        <value>node03</value>
</property>
<!-- 配置第二台机器的resourceManager -->
<property>
        <name>yarn.resourcemanager.hostname.rm2</name>
        <value>node02</value>
</property>

<!-- 配置第一台机器的resourceManager通信地址 -->
<property>
        <name>yarn.resourcemanager.address.rm1</name>
        <value>node03:8032</value>
</property>
<property>
        <name>yarn.resourcemanager.scheduler.address.rm1</name>
        <value>node03:8030</value>
</property>
<property>
        <name>yarn.resourcemanager.resource-tracker.address.rm1</name>
        <value>node03:8031</value>
</property>
<property>
        <name>yarn.resourcemanager.admin.address.rm1</name>
        <value>node03:8033</value>
</property>
<property>
        <name>yarn.resourcemanager.webapp.address.rm1</name>
        <value>node03:8088</value>
</property>

<!-- 配置第二台机器的resourceManager通信地址 -->
<property>
        <name>yarn.resourcemanager.address.rm2</name>
        <value>node02:8032</value>
</property>
<property>
        <name>yarn.resourcemanager.scheduler.address.rm2</name>
        <value>node02:8030</value>
</property>
<property>
        <name>yarn.resourcemanager.resource-tracker.address.rm2</name>
        <value>node02:8031</value>
</property>
<property>
        <name>yarn.resourcemanager.admin.address.rm2</name>
        <value>node02:8033</value>
</property>
<property>
        <name>yarn.resourcemanager.webapp.address.rm2</name>
        <value>node02:8088</value>
</property>
<!--开启resourcemanager自动恢复功能-->
<property>
        <name>yarn.resourcemanager.recovery.enabled</name>
        <value>true</value>
</property>
<!--在node1上配置rm1,在node2上配置rm2,注意：一般都喜欢把配置好的文件远程复制到其它机器上，但这个在YARN的另一个机器上一定要修改，其他机器上不配置此项-->
    <property>
        <name>yarn.resourcemanager.ha.id</name>
        <value>rm1</value>
       <description>If we want to launch more than one RM in single node, we need this configuration</description>
    </property>
       <!--用于持久存储的类。尝试开启-->
<property>
        <name>yarn.resourcemanager.store.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore</value>
</property>
<property>
        <name>yarn.resourcemanager.zk-address</name>
        <value>node02:2181,node03:2181,node01:2181</value>
        <description>For multiple zk services, separate them with comma</description>
</property>
<!--开启resourcemanager故障自动切换，指定机器-->
<property>
        <name>yarn.resourcemanager.ha.automatic-failover.enabled</name>
        <value>true</value>
        <description>Enable automatic failover; By default, it is enabled only when HA is enabled.</description>
</property>
<property>
        <name>yarn.client.failover-proxy-provider</name>
        <value>org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider</value>
</property>
<!-- 允许分配给一个任务最大的CPU核数，默认是8 -->
<property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>4</value>
</property>
<!-- 每个节点可用内存,单位MB -->
<property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>512</value>
</property>
<!-- 单个任务可申请最少内存，默认1024MB -->
<property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
</property>
<!-- 单个任务可申请最大内存，默认8192MB -->
<property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>512</value>
</property>
<!--多长时间聚合删除一次日志 此处-->
<property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>2592000</value><!--30 day-->
</property>
<!--时间在几秒钟内保留用户日志。只适用于如果日志聚合是禁用的-->
<property>
        <name>yarn.nodemanager.log.retain-seconds</name>
        <value>604800</value><!--7 day-->
</property>
<!--指定文件压缩类型用于压缩汇总日志-->
<property>
        <name>yarn.nodemanager.log-aggregation.compression-type</name>
        <value>gz</value>
</property>
<!-- nodemanager本地文件存储目录-->
<property>
        <name>yarn.nodemanager.local-dirs</name>
        <value>/export/servers/hadoop-2.7.5/yarn/local</value>
</property>
<!-- resourceManager  保存最大的任务完成个数 -->
<property>
        <name>yarn.resourcemanager.max-completed-applications</name>
        <value>1000</value>
</property>
<!-- 逗号隔开的服务列表，列表名称应该只包含a-zA-Z0-9_,不能以数字开始-->
<property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
</property>

<!--rm失联后重新链接的时间-->
<property>
        <name>yarn.resourcemanager.connect.retry-interval.ms</name>
        <value>2000</value>
</property>
</configuration>

修改mapred-site.xml
cd /export/servers/hadoop-2.7.5/etc/hadoop
vim mapred-site.xml
<configuration>
<!--指定运行mapreduce的环境是yarn -->
<property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
</property>
<!-- MapReduce JobHistory Server IPC host:port -->
<property>
        <name>mapreduce.jobhistory.address</name>
        <value>node03:10020</value>
</property>
<!-- MapReduce JobHistory Server Web UI host:port -->
<property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>node03:19888</value>
</property>
<!-- The directory where MapReduce stores control files.默认 ${hadoop.tmp.dir}/mapred/system -->
<property>
        <name>mapreduce.jobtracker.system.dir</name>
        <value>/export/servers/hadoop-2.7.5/data/system/jobtracker</value>
</property>
<!-- The amount of memory to request from the scheduler for each map task. 默认 1024-->
<property>
        <name>mapreduce.map.memory.mb</name>
        <value>1024</value>
</property>
<!-- <property>
                <name>mapreduce.map.java.opts</name>
                <value>-Xmx1024m</value>
        </property> -->
<!-- The amount of memory to request from the scheduler for each reduce task. 默认 1024-->
<property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>1024</value>
</property>
<!-- <property>
               <name>mapreduce.reduce.java.opts</name>
               <value>-Xmx2048m</value>
        </property> -->
<!-- 用于存储文件的缓存内存的总数量，以兆字节为单位。默认情况下，分配给每个合并流1MB，给个合并流应该寻求最小化。默认值100-->
<property>
        <name>mapreduce.task.io.sort.mb</name>
        <value>100</value>
</property>
<!-- <property>
        <name>mapreduce.jobtracker.handler.count</name>
        <value>25</value>
        </property>-->
<!-- 整理文件时用于合并的流的数量。这决定了打开的文件句柄的数量。默认值10-->
<property>
        <name>mapreduce.task.io.sort.factor</name>
        <value>10</value>
</property>
<!-- 默认的并行传输量由reduce在copy(shuffle)阶段。默认值5-->
<property>
        <name>mapreduce.reduce.shuffle.parallelcopies</name>
        <value>25</value>
</property>
<property>
        <name>yarn.app.mapreduce.am.command-opts</name>
        <value>-Xmx1024m</value>
</property>
<!-- MR AppMaster所需的内存总量。默认值1536-->
<property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>1536</value>
</property>
<!-- MapReduce存储中间数据文件的本地目录。目录不存在则被忽略。默认值${hadoop.tmp.dir}/mapred/local-->
<property>
        <name>mapreduce.cluster.local.dir</name>
        <value>/export/servers/hadoop-2.7.5/data/system/local</value>
</property>
</configuration>
```

##### 修改slaves

第一台机器执行以下命令

```shell
cd /export/servers/hadoop-2.7.5/etc/hadoop
vim slaves
node01
node02
node03
```

##### 修改hadoop-env.sh

第一台机器执行以下命令

```shell
cd /export/servers/hadoop-2.7.5/etc/hadoop
vim hadoop-env.sh
export JAVA_HOME=/export/servers/jdk1.8.0_141
```

#### 集群启动过程

将第一台机器的安装包发送到其他机器上

第一台机器执行以下命令：

```shell
cd /export/servers
scp -r hadoop-2.7.5/ node02:$PWD
scp -r hadoop-2.7.5/ node03:$PWD
```

三台机器上共同创建目录
三台机器执行以下命令

```shell
mkdir -p /export/servers/hadoop-2.7.5/data/dfs/nn/name
mkdir -p /export/servers/hadoop-2.7.5/data/dfs/nn/edits
mkdir -p /export/servers/hadoop-2.7.5/data/dfs/nn/name
mkdir -p /export/servers/hadoop-2.7.5/data/dfs/nn/edits
```

更改node02的rm2

更改node02的rm2
第二台机器执行以下命令

```shell
cd /export/servers/hadoop-2.7.5/etc/hadoop
vim  yarn-site.xml

```

```xml
<!--在node3上配置rm1,在node2上配置rm2,注意：一般都喜欢把配置好的文件远程复制到其它机器上，
但这个在YARN的另一个机器上一定要修改，其他机器上不配置此项 
注意我们现在有两个resourceManager  第三台是rm1   第二台是rm2
这个配置一定要记得去node02上面改好
-->
    <property>
        <name>yarn.resourcemanager.ha.id</name>
        <value>rm2</value>
       <description>If we want to launch more than one RM in single node, we need this configuration</description>
    </property>
```

##### 启动HDFS过程

node01机器执行以下命令

```shell
cd   /export/servers/hadoop-2.7.5
bin/hdfs zkfc -formatZK
sbin/hadoop-daemons.sh start journalnode
bin/hdfs namenode -format
bin/hdfs namenode -initializeSharedEdits -force
sbin/start-dfs.sh
```

node02上面执行

```shell
cd   /export/servers/hadoop-2.7.5
bin/hdfs namenode -bootstrapStandby
sbin/hadoop-daemon.sh start namenode
```

##### 启动yarn过程

启动yarn过程
node03上面执行

```shell
cd   /export/servers/hadoop-2.7.5
sbin/start-yarn.sh
node02上执行
cd   /export/servers/hadoop-2.7.5
sbin/start-yarn.sh
```

##### 查看resourceManager状态

查看resourceManager状态

node03上面执行

```shell
cd   /export/servers/hadoop-2.7.5
bin/yarn rmadmin -getServiceState rm1
```

node02上面执行

```shell
cd   /export/servers/hadoop-2.7.5
bin/yarn rmadmin -getServiceState rm2
```

##### node03启动jobHistory

node03机器执行以下命令启动jobHistory

```shell
cd /export/servers/hadoop-2.7.5
sbin/mr-jobhistory-daemon.sh start historyserver
```

##### hdfs状态查看

hdfs状态查看
node01机器查看hdfs状态
<http://192.168.52.100:50070/dfshealth.html#tab-overview>
node02机器查看hdfs状态
<http://192.168.52.110:50070/dfshealth.html#tab-overview>

##### yarn集群访问查看

yarn集群访问查看
<http://node03:8088/cluster>

##### 历史任务浏览界面

页面访问：
<http://192.168.52.120:19888/jobhistory>
