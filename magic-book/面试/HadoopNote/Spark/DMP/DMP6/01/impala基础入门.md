---
title: impala基础入门
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - DMP-6
---

## impala基础入门

### 1、impala介绍

#### 1.1 impala基本介绍

* ​impala是cloudera提供的一款高效率的sql查询工具，提供实时的查询效果，官方测试性能比hive快10到100倍，其sql查询比sparkSQL还要更加快速，**号称是当前大数据领域最快的查询sql工具**。
* ​impala是参照谷歌的新三篇论文（Caffeine--网络搜索引擎、Pregel--分布式图计算、Dremel--交互式分析工具）当中的Dremel 实现而来，其中旧三篇论文分别是（BigTable，GFS，MapReduce）分别对应我们即将学的HBase和已经学过的HDFS以及MapReduce。
* ​impala是基于hive并使用内存进行计算，兼顾数据仓库，具有实时，批处理，多并发等优点。
* ​Kudu与Apache Impala （孵化）紧密集成，impala天然就支持兼容kudu，允许开发人员使用Impala的SQL语法从Kudu的tablets 插入，查询，更新和删除数据；

#### 1.2 impala与hive的关系

* impala是基于hive的大数据分析查询引擎，直接使用hive的元数据metadata。
* impala元数据都存储在hive的metastore当中，并且impala兼容hive的绝大多数sql语法。
* 安装impala的话，必须先安装hive，保证hive安装成功，并且还需要启动hive的metastore服务。

#### 1.3 impala的优缺点

* 优点
  * 1、 impala比较快，非常快，特别快，因为所有的计算都可以放入内存当中进行完成，只要你内存足够大
  * 2、 摈弃了MR的计算，改用C++来实现，有针对性的硬件优化
  * 3、 具有数据仓库的特性，对hive的原有数据做数据分析
  * 4、多种存储格式可以选择（Parquet, Text, Avro, RCFile, SequenceFile）。
  * 5、支持ODBC，jdbc远程访问
* 缺点
  * 1、基于内存计算，对内存依赖性较大
  * 2、改用C++编写，意味着维护难度增大
  * 3、基于hive，与hive共存亡，紧耦合
  * 4、稳定性不如hive，存在数据丢失的情况
  * 5、不支持用户定义函数UDF

#### 1.4 impala架构和查询过程

![20171105084851619](impala基础入门/20171105084851619.png)

* **impala架构说明**

  * Impala Statestore
    * 检查集群各个节点上Impala daemon的健康状态
    * 同时不间断地将结果反馈给各个Impala daemon
    * 这个服务的物理进程名称是statestored，在整个集群中我们仅需要一个这样的进程即可

    补充说明：
    如果某个Impala节点由于硬件错误、软件错误或者其他原因导致离线，statestore就会通知其他的节点，避免其他节点再向这个离线的节点发送请求。
      由于statestore是当集群节点有问题的时候起通知作用，所以它对Impala集群并不是有关键影响的。
        如果statestore没有运行或者运行失败，其他节点和分布式任务会照常运行，只是说当节点掉线的时候集群会变得没那么健壮。当statestore恢复正常运行时，它就又开始与其他节点通信并进行监控。
  * Impala Catalog
    * 将SQL语句做出的元数据变化通知给集群的各个节点.
    * catalog服务的物理进程名称是catalogd，在整个集群中仅需要一个这样的进程。
    * 由于它的请求会跟statestore daemon交互，所以最好让statestored和catalogd这两个进程在同一节点上.
    补充说明：
      Impala 1.2中加入的catalog服务减少了REFRESH和INVALIDATE METADATA语句的使用。
    在之前的版本中，当在某个节点上执行了CREATE DATABASE、DROP DATABASE、CREATE TABLE、ALTER TABLE、或者DROP TABLE语句之后，需要在其它的各个节点上执行命令INVALIDATE METADATA来确保元数据信息的更新。
      同样的，当你在某个节点上执行了INSERT语句，在其它节点上执行查询时就得先执行REFRESH table_name这个操作，这样才能识别到新增的数据文件。
      需要注意的是，通过Impala执行的操作带来的元数据变化，有了catalog就不需要再执行REFRESH和INVALIDATE METADATA，但如果是通过Hive进行的建表、加载数据，则仍然需要执行REFRESH和INVALIDATE METADATA来通知Impala更新元数据信息。

  * Impala Daemon
    * Impala的核心组件是运行在各个节点上面的impalad这个守护进程（Impala daemon)
    * 它负责读写数据文件，接收从impala-shell、Hue、JDBC、ODBC等接口发送的查询语句，并行化查询语句和分发工作任务到Impala集群的各个节点上。
    * Impala daemon不间断的跟statestore进行通信交流，从而确认哪个节点是健康的能接收新的工作任务。它同时接收catalogd daemon（从Impala 1.2之后支持）传来的广播消息来更新元数据信息，当集群中的任意节点create、alter、drop任意对象、或者执行INSERT、LOAD DATA的时候触发广播消息

![092100494176141](impala基础入门/092100494176141.png)

* **impala查询过程**
  * 0、启动好impala集群，然后impalad服务都会向impala state store注册和订阅，然后state store服务监控每一个impalad的健康状态。
  * 1、客户端提交一个查询请求给impalad
  * 2、impalad根据查询的请求sql语句，然后操作存储在mysql中的元数据，获取到sql语句中表的元数据信息以及要操作表的真实数据地址。
  * 3、解析sql语句，生成查询任务，最后把任务分发到不同的impalad节点进行分布式查询计算
  * 4、把查询到的结果汇总之后，返回给客户端

### 2、impala集群安装部署

#### 2.1 安装规划

| 服务名称           | node1  | node2  | node3 |
| ------------------ | ------ | ------ | ----- |
| impala-catalog     | 不安装 | 不安装 | 安装  |
| impala-state-store | 不安装 | 不安装 | 安装  |
| impala-server      | 安装   | 安装   | 安装  |
| impala             | 安装   | 安装   | 安装  |

#### 2.2 yum源安装

* 主节点node3执行以下命令进行安装

```shell
  yum install impala -y
  yum install impala-server -y
  yum install impala-state-store -y
  yum install impala-catalog -y
  yum install bigtop-utils -y
  yum install impala-shell -y
```

* 从节点node1和node2执行以下命令进行安装

```shell
  yum install impala-server -y
  yum install bigtop-utils -y
```

#### 2.3  修改配置信息

##### 2.3.1 修改 hive-site.xml文件

impala依赖于hive，所以首先需要进行hive的配置修改；
node1机器修改hive-site.xml内容如下:
vim /export/servers/hive-1.1.0-cdh5.14.0/conf/hive-site.xml

```xml
      <property>
          <name>hive.metastore.uris</name>
          <value>thrift://node1:9083</value>
      </property>
      <property>
          <name>hive.metastore.client.socket.timeout</name>
          <value>3600</value>
      </property>
```

##### 2.3.2 将hive的安装目录发送到node2与node3上

* 在node1上执行命令

```shell
  cd /export/servers/
  scp -r hive-1.1.0-cdh5.14.0/ node2:$PWD
  scp -r hive-1.1.0-cdh5.14.0/ node3:$PWD
```

##### 2.3.3 启动hive的metastore服务

* 在node1上启动hive的metastore服务

```shell
  cd /export/servers/hive-1.1.0-cdh5.14.0
  nohup bin/hive --service metastore &
```

* 注意：一定要保证mysql的服务正常启动，否则metastore的服务不能够启动

##### 2.3.4 所有hadoop节点修改hdfs-site.xml文件

* 所有节点创建文件夹
 mkdir -p /var/run/hdfs-sockets
* 修改所有节点的hdfs-site.xml添加以下配置，修改完之后重启hdfs集群生效
  * vim /export/servers/hadoop-2.6.0-cdh5.14.0/etc/hadoop/hdfs-site.xml

```xml
         <!--短路读取--就是允许impala把一些信息存储在本地磁盘上，可以加快计算的速度-->
    <property>
                <name>dfs.client.read.shortcircuit</name>
                <value>true</value>
        </property>
         <!--打开块位置的存储的元数据信息-->
         <property>
                <name>dfs.datanode.hdfs-blocks-metadata.enabled</name>
                <value>true</value>
        </property>
         <!--Datanode和DFSClient之间沟通的Socket的本地文件路径-->
        <property>
                <name>dfs.domain.socket.path</name>
                <value>/var/run/hdfs-sockets/dn</value>
        </property>
          <!--分布式文件系统中并行RPC的超时-->
        <property>
                <name>dfs.client.file-block-storage-locations.timeout.millis</name>
                <value>10000</value>
        </property>
```

##### 2.3.5 重启hdfs

* start-dfs.sh

##### 2.3.6  创建hadoop与hive的配置文件的连接

* impala的配置目录为 /etc/impala/conf
  * 这个路径下面需要把 core-site.xml、hdfs-site.xml、hive-site.xml拷贝到这里来，但是我们这里使用软连接的方式会更好。
  * 所有节点执行以下命令创建链接到impala配置目录下来

```~shell
ln -s /export/servers/hadoop-2.6.0-cdh5.14.0/etc/hadoop/core-site.xml /etc/impala/conf/core-site.xml

ln -s /export/servers/hadoop-2.6.0-cdh5.14.0/etc/hadoop/hdfs-site.xml  /etc/impala/conf/hdfs-site.xml

ln -s /export/servers/hive-1.1.0-cdh5.14.0/conf/hive-site.xml /etc/impala/conf/hive-site.xml
```~

##### 2.3.7 所有节点修改impala默认配置

* 所有节点修改impala默认配置

* vim /etc/default/impala

```shell
  #指定集群的CATALOG_SERVICE和STATE_STORE服务地址
  IMPALA_CATALOG_SERVICE_HOST=node3
  IMPALA_STATE_STORE_HOST=node3
```

##### 2.3.8 所有节点创建mysql的驱动包的软连接

```shell
ln -s /export/servers/hive-1.1.0-cdh5.14.0/lib/mysql-connector-java-5.1.35.jar /usr/share/java/mysql-connector-java.jar
```

##### 2.3.9  所有节点修改bigtop的java路径

* 修改bigtop的java_home路径
  * vim /etc/default/bigtop-utils

```shell
    export JAVA_HOME=/export/servers/jdk
```

### 3、impala集群的启动和停止

#### 3.1 启动

* 1、需要启动HDFS

```~shell
  start-dfs.sh
```~

* 2、启动hive的元数据服务

  * 在node1上执行命令

```shell
  cd /export/servers/hive-1.1.0-cdh5.14.0
  nohup bin/hive --service metastore &
```

* 3、启动impala

  * 在主节点node3上启动以下服务

```shell
    service impala-state-store start
    service impala-catalog start
    service impala-server start
```

* 在从节点node1和node2上启动impala-server

```shell
    service impala-server start
```

* 4、查看impala进程是否存在

```shell
  ps -ef | grep impala
```

  ![1550735579716](impala基础入门/1550735579716.png)

注意：启动之后所有关于impala的日志默认都在 /var/log/impala  这个路径下，node3机器上面应该有三个进
程，node1与node2机器上面只有一个进程，如果进程个数不对，去对应目录下查看报错日志

#### 3.2  停止

* 在主节点node3上关闭以下服务

```shell
  service impala-state-store stop
  service impala-catalog stop
  service impala-server stop
```

* 在从节点node1和node2上关闭impala-server

```shell
  service impala-server stop
```

### 4、impala的web界面访问

* 启动好impala集群之后，可以访问web地址，查看集群相关信息
  * 访问impalad的管理界面<http://node3:25000/>
![1550735908605](impala基础入门/1550735908605.png)
访问statestored的管理界面<http://node3:25010/>
![1550735957214](impala基础入门/1550735957214.png)
* 访问catalogd 的管理界面<http://node3:25020/>
  ![1550736004673](impala基础入门/1550736004673.png)

### 5、impala的使用

#### 5.1 impala-shell的外部命令参数语法

* 不需要进入到impala-shell交互命令行当中即可执行的命令参数
* impala-shell后面执行的时候可以带很多参数：
  * -h  查看帮助文档
    ![1555999818766](impala基础入门/1555999818766.png)
  * -v  查看对应版本
    ![1556000095985](impala基础入门/1556000095985.png)
  * -r  刷新整个元数据，数据量大的时候，比较消耗服务器性能
    ![1556000019262](impala基础入门/1556000019262.png)
  * -B  去格式化，查询大量数据可以提高性能
      --print_header  去格式化显示列名
      --output_delimiter  指定分隔符
  * -f  执行查询文件

```shell
    cd /export/servers
    vim impala-shell.sql
    select * from employee
```

* impala-shell -f impala-shell.sql
  ![1556000424758](impala基础入门/1556000424758.png)
  * -i  连接到impalad
    ​    --impalad  指定impalad去执行任务
  * -o  保存执行结果到文件当中去
    ​    --output_file  指定输出文件名
    * impala-shell -f impala-shell.sql -o hello.txt
    ![1556000574723](impala基础入门/1556000574723.png)

#### 5.2 impala-shell的内部命令参数语法

* 进入impala-shell命令行之后可以执行的语法
  * help
    * 帮助文档
      ![1556030753137](impala基础入门/1556030753137.png)
  * connect
    * connect  hostname 连接到某一台机器上面去执行
      ![1556030802409](impala基础入门/1556030802409.png)
  * refresh
    * 刷新
    * refresh dbname.tablename  增量刷新，刷新某一张表的元数据，主要用于刷新hive当中数据表里面的数据改变的情况
      ![1556030895715](impala基础入门/1556030895715.png)
  * invalidate  metadata
    * invalidate  metadata全量刷新，性能消耗较大，主要用于hive当中新建数据库或者数据库表的时候来进行刷新
      ![1556030978124](impala基础入门/1556030978124.png)
  * explain
    * 用于查看sql语句的执行计划
      * explain select * from default.employee;
      ![1556031055194](impala基础入门/1556031055194.png)
    * explain的值可以设置成0,1,2,3等几个值，其中3级别是最高的，可以打印出最全的信息
      * set explain_level=3;
      ![1556031157316](impala基础入门/1556031157316.png)

注意:
(1) 在hive窗口当中插入的数据或者新建的数据库或者数据库表，在impala当中是不可直接查询到的，需要刷新数据库，使用命令 invalidate metadata;
(2) 在impala-shell当中插入的数据，在impala当中是可以直接查询到的，不需要刷新数据库，其中使用的就是catalog这个服务的功能实现的，catalog是impala1.2版本之后增加的模块功能，主要作用就是同步impala之间的元数据.

#### 5.3 创建数据库

* impala-shell进入到impala的交互窗口

##### 5.3.1 查看所有数据库

* show databases;

##### 5.3.2 创建与删除数据库

* 创建数据库
  * create database if not exists mydb1;
* 删除数据库
  * drop database if exists mydb1;

#### 5.4 创建表

* 创建表的语法跟hive一样

```sql
  内部表：
  create  table  student1(id int ,name string ,age int )  row  format  delimited fields terminated  by  '\t' ;
  
  
  外部表：
  create  external table  student2(id int ,name string ,age int )  row  format  delimited fields terminated  by  '\t' location  '/input/impala/external';
  
```

#### 5.5 向表中加载数据

* insert语句插入数据

```shell
insert into student1 values (1, 'zhangsan', 25 );
insert into student1 values (2, 'lisi', 20 );
insert into student1 values (3, 'xiaozhang', 35 );
insert into student1 values (4, 'laowang', 45 );
```

* 通过load  hdfs的数据到impala表中

```shell
  准备数据student.txt并上传到hdfs的 /user/impala路径下去
  11  zhangsan1  15
  22  zhangsan2  20
  33  zhangsan3  30
  44  zhangsan4  50
  
  加载数据
  load data inpath '/user/impala' into table student1;
```

* 使用insert  into  select 语法

```shell
  insert  into  user1 select * from user2;
```

#### 5.6 查询数据

```shell
select * from student1;
```

#### 5.7 清空表数据

```shell
truncate  student1;
```

#### 5.8 删除表数据

```shell
drop table student1；
```
