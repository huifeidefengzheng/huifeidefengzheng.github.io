---
title: 01_KUDU
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - DMP
---

KUDU

.导读
什么是 `Kudu`
. 操作 `Kudu`
. 如何设计 `Kudu` 的表

## 1. 什么是 Kudu

.导读
`Kudu` 的应用场景是什么?
. `Kudu` 在大数据平台中的位置在哪?
. `Kudu` 用什么样的设计, 才能满足其设计目标?
. `Kudu` 中有什么集群角色?

### 1.1. Kudu 的应用场景

现代大数据的应用场景::
例如现在要做一个类似物联网的项目, 可能是对某个工厂的生产数据进行分析
项目特点:
数据量大

有一个非常重大的挑战, 就是这些设备可能很多, 其所产生的事件记录可能也很大, 所以需要对设备进行数据收集和分析的话, 需要使用一些大数据的组件和功能

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606003709.png)
流式处理

因为数据是事件, 事件是一个一个来的, 并且如果快速查看结果的话, 必须使用流计算来处理这些数据
数据需要存储

最终需要对数据进行统计和分析, 所以数据要先有一个地方存, 后再通过可视化平台去分析和处理

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606004158.png)

对存储层的要求:
这样的一个流计算系统, 需要对数据进行什么样的处理呢?

. 要能够及时的看到最近的数据, 判断系统是否有异常
. 要能够扫描历史数据, 从而改进设备和流程

所以对数据存储层就有可能进行如下的操作

. 逐行插入, 因为数据是一行一行来的, 要想及时看到, 就需要来一行插入一行
. 低延迟随机读取, 如果想分析某台设备的信息, 就需要在数据集中随机读取某一个设备的事件记录
. 快速分析和扫描, 数据分析师需要快速的得到结论, 执行一行 `SQL` 等上十天是不行的

方案一: 使用 `Spark Streaming` 配合 `HDFS` 存储:总结一下需求

* 实时处理, `Spark Streaming`
* 大数据存储, `HDFS`
* 使用 Kafka 过渡数据

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606005650.png)

但是这样的方案有一个非常重大的问题, 就是速度机器之慢, 因为 `HDFS` 不擅长存储小文件, 而通过流处理直接写入 `HDFS` 的话, 会产生非常大量的小文件, 扫描性能十分的差

方案二: `HDFS` + `compaction`:上面方案的问题是大量小文件的查询是非常低效的, 所以可以将这些小文件压缩合并起来

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606023831.png)

但是这样的处理方案也有很多问题

* 一个文件只有不再活跃时才能合并
* 不能将覆盖的结果放回原来的位置

所以一般在流式系统中进行小文件合并的话, 需要将数据放在一个新的目录中, 让 `Hive/Impala` 指向新的位置, 再清理老的位置

方案三: `HBase` + `HDFS`:前面的方案都不够舒服, 主要原因是因为一直在强迫 `HDFS` 做它并不擅长的事情, 对于实时的数据存储, 谁更适合呢? `HBase` 好像更合适一些, 虽然 `HBase` 适合实时的低延迟的数据村醋, 但是对于历史的大规模数据的分析和扫描性能是比较差的, 所以还要结合 `HDFS` 和 `Parquet` 来做这件事

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606025028.png)

因为 `HBase` 不擅长离线数据分析, 所以在一定的条件触发下, 需要将 `HBase` 中的数据写入 `HDFS` 中的 `Parquet` 文件中, 以便支持离线数据分析, 但是这种方案又会产生新的问题

* 维护特别复杂, 因为需要在不同的存储间复制数据
* 难以进行统一的查询, 因为实时数据和离线数据不在同一个地方

这种方案, 也称之为 `Lambda`, 分为实时层和批处理层, 通过这些这么复杂的方案, 其实想做的就是一件事, 流式数据的存储和快速查询

方案四: `Kudu`:`Kudu` 声称在扫描性能上, 媲美 `HDFS` 上的 `Parquet`. 在随机读写性能上, 媲美 `HBase`. 所以将存储存替换为 `Kudu`, 理论上就能解决我们的问题了.

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606025824.png)
总结
对于实时流式数据处理, `Spark`, `Flink`, `Storm` 等工具提供了计算上的支持, 但是它们都需要依赖外部的存储系统, 对存储系统的要求会比较高一些, 要满足如下的特点

* 支持逐行插入
* 支持更新
* 低延迟随机读取
* 快速分析和扫描

### 1.2. Kudu 和其它存储工具的对比

.导读
`OLAP` 和 `OLTP` 行式存储和列式存储
. `Kudu` 和 `MySQL` 的区别
. `Kudu` 和 `HBase` 的区别
`OLAP` 和 `OLTP`:广义来讲, 数据库分为 `OLTP` 和 `OLAP`

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606125557.png)

* `OLTP`

先举个栗子, 在电商网站中, 经常见到一个功能 - "我的订单", 这个功能再查询数据的时候, 是查询的某一个用户的数据, 并不是批量的数据

`OLTP` 需要做的事情是

. 快速插入和更新
. 精确查询

所以 `OLTP` 并不需要对数据进行大规模的扫描和分析, 所以它的扫描性能并不好, 它主要是用于对响应速度和数据完整性很高的在线服务应用中

* `OLAP`

`OLAP` 和 `OLTP` 的场景不同, `OLAP` 主要服务于分析型应用, 其一般是批量加载数据, 如果出错了, 重新查询即可

* 总结

* `OLTP` 随机访问能力比较强, 批量扫描比较差
* `OLAP` 擅长大规模批量数据加载, 对于随机访问的能力则比较差
* 大数据系统中, 往往从 `OLTP` 数据库中 `ETL` 放入 `OLAP` 数据库中, 然后做分析和处理

行式存储和列式存储:行式和列式是不同的存储方式, 其大致如下

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606132236.png)

* 行式存储

行式一般用做于 `OLTP`, 例如我的订单, 那不仅要看到订单, 还要看到收货地址, 付款信息, 派送信息等, 所以 `OLTP` 一般是倾向于获取整行所有列的信息

* 列式存储

而分析平台就不太一样了, 例如分析销售额, 那可能只对销售额这一列感兴趣, 所以按照列存储, 只获取需要的列, 这样能减少数据的读取量

存储模型:
结构:

* `Kudu` 的存储模型是有结构的表
* `OLTP` 中代表性的 `MySQL`, `Oracle` 模型是有结构的表
* `HBase` 是看起来像是表一样的 `Key-Value` 型数据, `Key` 是 `RowKey` 和列簇的组合, `Value` 是具体的值

主键:

* `Kudu` 采用了 `Raft` 协议, 所以 `Kudu` 的表中有唯一主键
* 关系型数据库也有唯一主键
* `HBase` 的 `RowKey` 并不是唯一主键

事务支持:

. `Kudu` 缺少跨行的 `ACID` 事务
. 关系型数据库大多在单机上是可以支持 `ACID` 事务的

性能:

* `Kudu` 的随机读写速度目标是和 `HBase` 相似, 但是这个目标建立在使用 `SSD` 基础之上
* `Kudu` 的批量查询性能目标是比 `HDFS` 上的 `Parquet` 慢两倍以内
硬件需求:* `Hadoop` 的设计理念是尽可能的减少硬件依赖, 使用更廉价的机器, 配置机械硬盘
* `Kudu` 的时代 `SSD` 已经比较常见了, 能够做更多的磁盘操作和内存操作
* `Hadoop` 不太能发挥比较好的硬件的能力, 而 `Kudu` 为了大内存和 `SSD` 而设计, 所以 `Kudu` 对硬件的需求会更大一些

### 1.3. Kudu 的设计和结构

.导读
`Kudu` 是什么
. `Kudu` 的整体设计
. `Kudu` 的角色
. `Kudu` 的概念

`Kudu` 是什么:
`HDFS` 上的数据分析:
`HDFS` 是一种能够非常高效的进行数据分析的存储引擎

* `HDFS` 有很多支持压缩的列式存储的文件格式, 性能很好, 例如 `Parquet` 和 `ORC`
* `HDFS` 本身支持并行

`HBase` 可以进行高效的数据插入和读取:
`HBase` 主要用于完成一些对实时性要求比较高的场景

* `HBase` 能够以极高的吞吐量来进行数据存储, 无论是批量加载, 还是大量 `put`
* `HBase` 能够对主键进行非常高效的扫描, 因为其根据主键进行排序和维护
* 但是对于主键以外的列进行扫描则性能会比较差

`Kudu` 的设计目标:
`Kudu` 最初的目标是成为一个新的存储引擎, 可以进行快速的数据分析, 又可以进行高效的数据随机插入, 这样就能简化数据从源端到 `Hadoop` 中可以用于被分析的过程, 所以有如下的一些设计目标

* 尽可能快速的扫描, 达到 `HDFS` 中 `Parquet` 的二分之一速度
* 尽可能的支持随机读写, 达到 `1ms` 的响应时间
* 列式存储
* 支持 `NoSQL` 样式的 `API`, 例如 `put`, `get`, `delete`, `scan`

总体设计:

* `Kudu` 不支持 `SQL`
`Kudu` 和 `Impala` 都是 `Cloudera` 的项目, 所以 `Kudu` 不打算自己实现 `SQL` 的解析和执行计划, 而是选择放在 `Impala` 中实现, 这两个东西配合来完成任务
`Kudu` 的底层是一个基于表的引擎, 但是提供了 `NoSQL` 的 `API`
* `Kudu` 中存储两类的数据
* `Kudu` 存储自己的元信息, 例如表名, 列名, 列类型
* `Kudu` 当然也有存放表中的数据
这两种数据都存储在 `tablet` 中
* `Master server`
存储元数据的 `tablet` 由 `Master server` 管理
* `Tablet server`
存储表中数据的 `tablet` 由不同的 `Tablet server` 管理
* `tablet`
`Master server` 和 `Tablet server` 都是以 `tablet` 作为存储形式来存储数据的, 一个 `tablet` 通常由一个 `Leader` 和两个 `Follower` 组成, 这些角色分布的不同的服务器中
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607011022.png)

`Master server`:![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607004622.png)

* `Master server` 中存储的其实也就是一个 `tablet`, 这个 `tablet` 中存储系统的元数据, 所以 `Kudu` 无需依赖 `Hive`
* 客户端访问某一张表的某一部分数据时, 会先询问 `Master server`, 获取这个数据的位置, 去对应位置获取或者存储数据
* 虽然 `Master` 比较重要, 但是其承担的职责并不多, 数据量也不大, 所以为了增进效率, 这个 `tablet` 会存储在内存中
* 生产环境中通常会使用多个 `Master server` 来保证可用性

`Tablet server`:![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607010016.png)

* `Tablet server` 中也是 `tablet`, 但是其中存储的是表数据
* `Tablet server` 的任务非常繁重, 其负责和数据相关的所有操作, 包括存储, 访问, 压缩, 其还负责将数据复制到其它机器
* 因为 `Tablet server` 特殊的结构, 其任务过于繁重, 所以有如下的限制
** `Kudu` 最多支持 `300` 个服务器, 建议 `Tablet server` 最多不超过 `100` 个
** 建议每个 `Tablet server` 至多包含 `2000` 个 `tablet` (包含 `Follower`)
** 建议每个表在每个 `Tablet server` 中至多包含 `60` 个 `tablet` (包含 `Follower`)
** 每个 `Tablet server` 至多管理 `8TB` 数据
** 理想环境下, 一个 `tablet leader` 应该对应一个 `CPU` 核心, 以保证最优的扫描性能

`tablet` 的存储结构:![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607021239.png)

在 `Kudu` 中, 为了同时支持批量分析和随机访问, 在整体上的设计一边参考了 `Parquet` 这样的文件格式的设计, 一边参考了 `HBase` 的设计

* `MemRowSet`
这个组件就很像 `HBase` 中的 `MemoryStore`, 是一个缓冲区, 数据来了先放缓冲区, 保证响应速度
* `DiskRowSet`
列存储的好处不仅仅只是分析的时候只 `I/O` 对应的列, 还有一个好处, 就是同类型的数据放在一起, 更容易压缩和编码
`DiskRowSet` 中的数据以列式组织, 类似 `Parquet` 中的方式, 对其中的列进行编码, 通过布隆过滤器增进查询速度
`tablet` 的 `Insert` 流程:![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607022949.png)
* 使用 MemRowSet 作为缓冲, 特定条件下写为多个 DiskRowSet
* 在插入之前, 为了保证主键唯一性, 会已有的 DiskRowSet 和 MemRowSet 进行验证, 如果主键已经存在则报错
`tablet` 的 `Update` 流程:![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607102727.png)

. 查找要更新的数据在哪个 `DiskRowSet` 中
. 数据放入 `DiskRowSet` 所持有的 `DeltaMemStore` 中, 这一步也是暂存
. 特定时机下, `DeltaMemStore` 会将数据溢写到磁盘, 生成 `RedoDeltaFile`, 记录数据的变化
. 定时合并 `RedoDeltaFile`
** 合并策略有三种, 常见的有两种, 一种是 `major`, 会将数据合并到基线数据中, 一种是 `minor`, 只合并 `RedoDeltaFile`

## 2. Kudu 安装和操作

.导读
因为 `Kudu` 经常和 `Impala` 配合使用, 所以我们也要安装 `Impala`, 但是又因为 `Impala` 强依赖于 `CDH`, 所以我们连 `CDH` 一起安装一下, 做一个完整的 `CDH` 集群, 搭建一套新的虚拟机

. 创建虚拟机准备初始环境
. 安装 `Zookeeper`
. 安装 `Hadoop`
. 安装 `MySQL`
. 安装 `Hive`
. 安装 `Kudu`
. 安装 `Impala`

### 2.1. 准备初始环境

.导读
之前的环境中已经安装了太多环境, 所以换一个新的虚拟机, 从头开始安装

. 创建虚拟机
. 安装系统
. 复制三台虚拟机
. 配置时间同步服务
. 配置主机名
. 关闭 `SELinux`
. 关闭防火墙
. 重启
. 配置免密登录
. 安装 `JDK`

`Step 1`: 创建虚拟机:. 在 `VmWare` 中点击创建虚拟机
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608151742.png)

. 打开向导
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608151923.png)

. 设置硬件兼容性
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152010.png)

. 指定系统安装方式
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152054.png)

. 指定系统类型
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152151.png)

. 指定虚拟机位置
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152251.png)

. 处理器配置
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152338.png)

. 内存配置
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152403.png)

. 选择网络类型, 这一步非常重要, 一定要配置正确
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152433.png)

. 选择 `I/O` 类型
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152533.png)

. 选择虚拟磁盘类型
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152738.png)

. 选择磁盘创建方式
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152827.png)

. 创建新磁盘
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153017.png)

. 指定磁盘文件位置
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153103.png)

. 终于, 虚拟机创建好了, 复制图片差点没给我累挂
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153146.png)

`Step 2`: 安装 `CentOS 6`:. 为虚拟机挂载安装盘
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153305.png)

. 选择安装盘
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153349.png)

. 开启虚拟机
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153428.png)

. 进入 CentOS 6 的安装
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153614.png)

. 跳过磁盘选择
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153644.png)

. 选择语言
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153742.png)

. 选择键盘类型
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153836.png)

. 选择存储设备类型
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153905.png)

. 清除数据
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154025.png)

. 主机名
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154051.png)

. 选择时区, 这一步很重要, 一定要选
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154127.png)

. 设置 `root` 账号, 密码最好是统一的, 就 `hadoop` 吧
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154211.png)

. 选择安装类型
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154244.png)

. 选择安装软件的类型
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154410.png)

. 安装完成, 终于不用复制图片了, 开心
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154448.png)

`Step 3`: 集群规划:|===
| `HostName` | `IP`

| cdh01.itcast.cn | 192.168.169.101
| cdh02.itcast.cn | 192.168.169.102
| cdh03.itcast.cn | 192.168.169.103
|===

已经安装好一台虚拟机了, 接下来通过复制的方式创建三台虚拟机

. 复制虚拟机文件夹(Ps. 在创建虚拟机时候选择的路径)
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608155101.png)

. 进入三个文件夹中, 点击 `vmx` 文件, 让 VmWare 加载
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608155152.png)

. 为所有的虚拟机生成新的 `MAC` 地址
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608161145.png)

. 确认 `vmnet8` 的网关地址, 以及这块虚拟网卡的地址

. 修改网卡信息

进入每台机器中, 修改 `70-persistent-net.rules`

```text
vi /etc/udev/rules.d/70-persistent-net.rules
```

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608161751.png)
更改 `IP` 地址, 注意: 1. 网关地址要和 `vmnet8` 的网关地址一致, 2. `IP` 改为 `192.168.169.101`
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608161950.png)
`Step 4`: 配置时间同步服务:在几乎所有的分布式存储系统上, 都需要进行时钟同步, 避免出现旧的数据在同步过程中变为新的数据, 包括 `HBase`, `HDFS`, `Kudu` 都需要进行时钟同步, 所以在一切开始前, 先同步一下时钟, 保证没有问题

时钟同步比较简单, 只需要确定时钟没有太大差异, 然后开启 `ntp` 的自动同步服务即可

```text
yum install -y ntp
service ntpd start
```

同步大概需要 `5 - 10` 分钟, 然后查看是否已经是同步状态即可

```text
ntpstat
```

最后在其余两台节点也要如此配置一下

`Step 5`: 配置主机名:配置主机名是为了在网络内可以通信

. 修改 `/etc/sysconfig/network` 文件, 声明主机名

```text
# 在三个节点上使用不同的主机名
HOSTNAME=cdh01.itcast.cn
```

. 修改 `/etc/hosts` 文件, 确定 `DNS` 的主机名

```text
127.0.0.1 cdh01.itcast.cn localhost cdh01

192.168.169.101 cdh01.itcast.cn cdh01
192.168.169.102 cdh02.itcast.cn cdh02
192.168.169.103 cdh03.itcast.cn cdh03
```

. 在其余的两台机器中也要如此配置

`Step 6`: 关闭 `SELinux`:修改 `/etc/selinus/config` 将 `SELinux` 关闭

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608162634.png)

最后别忘了再其它节点也要如此配置

`Step 7`: 关闭防火墙:执行如下命令做两件事, 关闭防火墙, 关闭防火墙开机启动

```text
service iptables stop
chkconfig iptables off
```

最后别忘了再其它节点也要如此配置

`Step x`: 重启:刚才有一些配置是没有及时生效的, 为了避免麻烦, 在这里可以重启一下, 在三台节点上依次执行命令

```text
reboot -h now
```

`Step 8`: 配置三台节点的免密登录:SSH 有两种登录方式

. 输入密码从而验证登录
. 服务器生成随机字符串, 客户机使用私钥加密, 服务器使用预先指定的公钥解密, 从而验证登录

所以配置免密登录就可以使用第二种方式, 大概步骤就是先在客户机生成密钥对, 然后复制给服务器

```text
# 生成密钥对
ssh-keygen -t rsa

# 拷贝公钥到服务机
ssh-copy-id cdh01
ssh-copy-id cdh02
ssh-copy-id cdh03
```

然后在三台节点上依次执行这些命令

`Step 9`: 安装 `JDK`:安装 `JDK` 之前, 可以先卸载已经默认安装的 `JDK`, 这样可以避免一些诡异问题

. 查看是否有存留的 `JDK`

```text
rpm -qa | grep java
```

. 如果有, 则使用如下命令卸载

```text
rpm -e -nodeps xx
```

. 上传 JDK 包到服务器中

. 解压并拷贝到 `/usr/java` 中

```text
tar xzvf jdk-8u192-linux-x64.tar.gz
mv jdk1.8.0_192 /usr/java/
```

. 修改 `/etc/hosts` 配置环境变量

```text
export JAVA_HOME=/usr/java/jdk1.8.0_192
export PATH=$PATH:$JAVA_HOME/bin
```

. 在剩余两台主机上重复上述步骤

### 2.2. 配置 Yum 源

.导读
下载 CDH 的所有安装包
.

### 2.. 使用 Java 操作 Kudu

.导读

### 2.. 使用 Spark 操作 Kudu

## 4. Kudu 表和模式
