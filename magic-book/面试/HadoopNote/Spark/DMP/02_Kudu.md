---
title: 02_Kudu
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - DMP
---

 Kudu

.导读
什么是 `Kudu`
. 操作 `Kudu`

## 1. 什么是 Kudu

.导读
`Kudu` 的应用场景是什么?
. `Kudu` 在大数据平台中的位置在哪?
. `Kudu` 用什么样的设计, 才能满足其设计目标?
. `Kudu` 中有什么集群角色?

### 1.1. Kudu 的应用场景

现代大数据的应用场景:

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

方案一: 使用 `Spark Streaming` 配合 `HDFS` 存储:

总结一下需求

* 实时处理, `Spark Streaming`
* 大数据存储, `HDFS`
* 使用 Kafka 过渡数据

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606005650.png)

但是这样的方案有一个非常重大的问题, 就是速度机器之慢, 因为 `HDFS` 不擅长存储小文件, 而通过流处理直接写入 `HDFS` 的话, 会产生非常大量的小文件, 扫描性能十分的差

方案二: `HDFS` + `compaction`:

上面方案的问题是大量小文件的查询是非常低效的, 所以可以将这些小文件压缩合并起来

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606023831.png)

但是这样的处理方案也有很多问题

* 一个文件只有不再活跃时才能合并
* 不能将覆盖的结果放回原来的位置

所以一般在流式系统中进行小文件合并的话, 需要将数据放在一个新的目录中, 让 `Hive/Impala` 指向新的位置, 再清理老的位置

方案三: `HBase` + `HDFS`:

前面的方案都不够舒服, 主要原因是因为一直在强迫 `HDFS` 做它并不擅长的事情, 对于实时的数据存储, 谁更适合呢? `HBase` 好像更合适一些, 虽然 `HBase` 适合实时的低延迟的数据村醋, 但是对于历史的大规模数据的分析和扫描性能是比较差的, 所以还要结合 `HDFS` 和 `Parquet` 来做这件事

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606025028.png)

因为 `HBase` 不擅长离线数据分析, 所以在一定的条件触发下, 需要将 `HBase` 中的数据写入 `HDFS` 中的 `Parquet` 文件中, 以便支持离线数据分析, 但是这种方案又会产生新的问题

* 维护特别复杂, 因为需要在不同的存储间复制数据
* 难以进行统一的查询, 因为实时数据和离线数据不在同一个地方

这种方案, 也称之为 `Lambda`, 分为实时层和批处理层, 通过这些这么复杂的方案, 其实想做的就是一件事, 流式数据的存储和快速查询

方案四: `Kudu`:

`Kudu` 声称在扫描性能上, 媲美 `HDFS` 上的 `Parquet`. 在随机读写性能上, 媲美 `HBase`. 所以将存储存替换为 `Kudu`, 理论上就能解决我们的问题了.

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190606025824.png)

.总结
对于实时流式数据处理, `Spark`, `Flink`, `Storm` 等工具提供了计算上的支持, 但是它们都需要依赖外部的存储系统, 对存储系统的要求会比较高一些, 要满足如下的特点

* 支持逐行插入
* 支持更新
* 低延迟随机读取
* 快速分析和扫描

### 1.2. Kudu 和其它存储工具的对比

.导读
`OLAP` 和 `OLTP`
. 行式存储和列式存储
. `Kudu` 和 `MySQL` 的区别
. `Kudu` 和 `HBase` 的区别

`OLAP` 和 `OLTP`:

广义来讲, 数据库分为 `OLTP` 和 `OLAP`

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

行式存储和列式存储:

行式和列式是不同的存储方式, 其大致如下

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

硬件需求:

* `Hadoop` 的设计理念是尽可能的减少硬件依赖, 使用更廉价的机器, 配置机械硬盘
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

`Master server`:

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607004622.png)

* `Master server` 中存储的其实也就是一个 `tablet`, 这个 `tablet` 中存储系统的元数据, 所以 `Kudu` 无需依赖 `Hive`
* 客户端访问某一张表的某一部分数据时, 会先询问 `Master server`, 获取这个数据的位置, 去对应位置获取或者存储数据
* 虽然 `Master` 比较重要, 但是其承担的职责并不多, 数据量也不大, 所以为了增进效率, 这个 `tablet` 会存储在内存中
* 生产环境中通常会使用多个 `Master server` 来保证可用性

`Tablet server`:

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607010016.png)

* `Tablet server` 中也是 `tablet`, 但是其中存储的是表数据
* `Tablet server` 的任务非常繁重, 其负责和数据相关的所有操作, 包括存储, 访问, 压缩, 其还负责将数据复制到其它机器
* 因为 `Tablet server` 特殊的结构, 其任务过于繁重, 所以有如下的限制
** `Kudu` 最多支持 `300` 个服务器, 建议 `Tablet server` 最多不超过 `100` 个
** 建议每个 `Tablet server` 至多包含 `2000` 个 `tablet` (包含 `Follower`)
** 建议每个表在每个 `Tablet server` 中至多包含 `60` 个 `tablet` (包含 `Follower`)
** 每个 `Tablet server` 至多管理 `8TB` 数据
** 理想环境下, 一个 `tablet leader` 应该对应一个 `CPU` 核心, 以保证最优的扫描性能

`tablet` 的存储结构:

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607021239.png)

在 `Kudu` 中, 为了同时支持批量分析和随机访问, 在整体上的设计一边参考了 `Parquet` 这样的文件格式的设计, 一边参考了 `HBase` 的设计

* `MemRowSet`

这个组件就很像 `HBase` 中的 `MemoryStore`, 是一个缓冲区, 数据来了先放缓冲区, 保证响应速度

* `DiskRowSet`

列存储的好处不仅仅只是分析的时候只 `I/O` 对应的列, 还有一个好处, 就是同类型的数据放在一起, 更容易压缩和编码

`DiskRowSet` 中的数据以列式组织, 类似 `Parquet` 中的方式, 对其中的列进行编码, 通过布隆过滤器增进查询速度

`tablet` 的 `Insert` 流程:

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607022949.png)

* 使用 MemRowSet 作为缓冲, 特定条件下写为多个 DiskRowSet
* 在插入之前, 为了保证主键唯一性, 会已有的 DiskRowSet 和 MemRowSet 进行验证, 如果主键已经存在则报错

`tablet` 的 `Update` 流程:

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190607102727.png)

. 查找要更新的数据在哪个 `DiskRowSet` 中
. 数据放入 `DiskRowSet` 所持有的 `DeltaMemStore` 中, 这一步也是暂存
. 特定时机下, `DeltaMemStore` 会将数据溢写到磁盘, 生成 `RedoDeltaFile`, 记录数据的变化
. 定时合并 `RedoDeltaFile`
** 合并策略有三种, 常见的有两种, 一种是 `major`, 会将数据合并到基线数据中, 一种是 `minor`, 只合并 `RedoDeltaFile`

## 2. Kudu 安装和操作

.导读
因为 `Kudu` 经常和 `Impala` 配合使用, 所以我们也要安装 `Impala`

但是又因为 `Impala` 强依赖于 `Hive` 的 `MetaStore`, 所以 `Hive` 也需要安装

又因为 `Hive` 依赖 `HDFS`, 所以 `Hadoop` 也需要安装

并且 `Impala` 是 `CDH` 的 产品, 所以强依赖 `CDH` 版本的 `Hive` 和 `HDFS`, 所以我们需要安装 CDH 版本的 `Hadoop`, `Zookeeper`, `Hive`, 既然都要重新搞, 所以就放弃原来的虚拟机, 重新部署一套新的即可, 这样最节省时间

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

`Step 1`: 创建虚拟机:

. 在 `VmWare` 中点击创建虚拟机
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608151742.png)

. 打开向导
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608151923.png)

. 设置硬件兼容性
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152010.png)

. 指定系统安装方式
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152054.png)

. 指定系统类型
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152151.png)

. 指定虚拟机位置
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152251.png)

. 处理器配置
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152338.png)

. 内存配置
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152403.png)

. 选择网络类型, 这一步非常重要, 一定要配置正确
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152433.png)

. 选择 `I/O` 类型
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152533.png)

. 选择虚拟磁盘类型
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152738.png)

. 选择磁盘创建方式
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608152827.png)

. 创建新磁盘
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153017.png)

. 指定磁盘文件位置
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153103.png)

. 终于, 虚拟机创建好了, 复制图片差点没给我累挂
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153146.png)

`Step 2`: 安装 `CentOS 6`:

. 为虚拟机挂载安装盘
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153305.png)

. 选择安装盘
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153349.png)

. 开启虚拟机
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153428.png)

. 进入 CentOS 6 的安装
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153614.png)

. 跳过磁盘选择
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153644.png)

. 选择语言
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153742.png)

. 选择键盘类型
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153836.png)

. 选择存储设备类型
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608153905.png)

. 清除数据
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154025.png)

. 主机名
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154051.png)

. 选择时区, 这一步很重要, 一定要选
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154127.png)

. 设置 `root` 账号, 密码最好是统一的, 就 `hadoop` 吧
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154211.png)

. 选择安装类型
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154244.png)

. 选择安装软件的类型
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154410.png)

. 安装完成, 终于不用复制图片了, 开心
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608154448.png)

`Step 3`: 集群规划:

|===
| `HostName` | `IP`

| cdh01.itcast.cn | 192.168.169.101
| cdh02.itcast.cn | 192.168.169.102
| cdh03.itcast.cn | 192.168.169.103
|===

已经安装好一台虚拟机了, 接下来通过复制的方式创建三台虚拟机

. 复制虚拟机文件夹(Ps. 在创建虚拟机时候选择的路径)
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608155101.png)

. 进入三个文件夹中, 点击 `vmx` 文件, 让 VmWare 加载
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608155152.png)

. 为所有的虚拟机生成新的 `MAC` 地址
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608161145.png)

. 确认 `vmnet8` 的网关地址, 以及这块虚拟网卡的地址

. 修改网卡信息

进入每台机器中, 修改 `70-persistent-net.rules`

```text
vi /etc/udev/rules.d/70-persistent-net.rules
```

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608161751.png)
更改 `IP` 地址, 修改文件 `vi /etc/sysconfig/network-scripts/ifcfg-eth0`, 注意: 1. 网关地址要和 `vmnet8` 的网关地址一致, 2. `IP` 改为 `192.168.169.101`
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608161950.png)

`Step 4`: 配置时间同步服务:

在几乎所有的分布式存储系统上, 都需要进行时钟同步, 避免出现旧的数据在同步过程中变为新的数据, 包括 `HBase`, `HDFS`, `Kudu` 都需要进行时钟同步, 所以在一切开始前, 先同步一下时钟, 保证没有问题

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

`Step 5`: 配置主机名:

配置主机名是为了在网络内可以通信

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

`Step 6`: 关闭 `SELinux`:

修改 `/etc/selinus/config` 将 `SELinux` 关闭

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190608162634.png)

最后别忘了再其它节点也要如此配置

`Step 7`: 关闭防火墙:

执行如下命令做两件事, 关闭防火墙, 关闭防火墙开机启动

```text
service iptables stop
chkconfig iptables off
```

最后别忘了再其它节点也要如此配置

`Step x`: 重启:

刚才有一些配置是没有及时生效的, 为了避免麻烦, 在这里可以重启一下, 在三台节点上依次执行命令

```text
reboot -h now
```

`Step 8`: 配置三台节点的免密登录:

SSH 有两种登录方式

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

`Step 9`: 安装 `JDK`:

安装 `JDK` 之前, 可以先卸载已经默认安装的 `JDK`, 这样可以避免一些诡异问题

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

### 2.2. 创建本地 Yum 仓库

.导读
创建本地 `Yum` 仓库的目的是因为从远端的 `Yum` 仓库下载东西的速度实在是太渣, 然而 `CDH` 的所有组件几乎都要从 `Yum` 安装, 所以搭建一个本地仓库会加快下载速度

. 下载 `CDH` 的所有安装包
. 生成 `CDH` 的 `Yum` 仓库
. 配置服务器, 在局域网共享仓库

`Step 1`: 下载 `CDH` 的安装包:

创建本地 `Yum` 仓库的原理是将 `CDH` 的安装包下载下来, 提供 `Http` 服务给局域网其它主机(或本机), 让其它主机的 `Yum` 能够通过 `Http` 服务下载 `CDH` 的安装包, 所以需要先下载对应的 `CDH` 安装包

NOTE: 需要注意的是, 这一步可以一点都不做, 因为已经为大家提供了对应的安装包, 在 `DMP` 的目录中, 就能找到 `cloudera-cdh5` 这个目录, 上传到服务器即可

. 下载 `CDH` 的安装包需要使用 `CDH` 的一个工具, 要安装 `CDH` 的这个工具就要先导入 `CDH` 的 `Yum` 源

```text
wget https://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/cloudera-cdh5.repo
mv cloudera-cdh5.repo /etc/yum.repos.d/
```

. 安装 `CDH` 安装包同步工具

```text
yum install -y yum-utils createrepo
```

. 同步 `CDH` 的安装包

```text
reposync -r cloudera-cdh5
```

`Step 2`: 创建本地 `Yum` 仓库服务器:

创建本地 `Yum` 仓库的原理是将 `CDH` 的安装包下载下来, 提供 `Http` 服务给局域网其它主机(或本机), 让其它主机的 `Yum` 能够通过 `Http` 服务下载 `CDH` 的安装包, 所以需要提供 `Http` 服务, 让本机或者其它节点可以通过 `Http` 下载文件, Yum 本质也就是帮助我们从 `Yum` 的软件仓库下载软件

. 安装 `Http` 服务器软件

```text
yum install -y httpd
service httpd start
```

. 创建 `Yum` 仓库的 `Http` 目录

```text
mkdir -p /var/www/html/cdh/5
cp -r cloudera-cdh5/RPMS /var/www/html/cdh/5/
cd /var/www/html/cdh/5
createrepo .
```

. 在三台主机上配置 `Yum` 源

最后一步便是向 `Yum` 增加一个新的源, 指向我们在 `cdh01` 上创建的 `Yum` 仓库, 但是在这个环节的第一步中, 已经下载了一个 `Yum` 的源, 只需要修改这个源的文件, 把 `URL` 替换为 `cdh01` 的地址即可

所以在 `cdh01` 上修改文件 `/etc/yum.repos.d/cloudera-cdh5.repo` 为

```text
baseurl=http://cdh01/cdh/5/
```

在 `cdh02` 和 `cdh03` 上下载这个文件

```text
wget https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/cloudera-cdh5.repo
mv cloudera-cdh5.repo /etc/yum.repos.d/
```

然后在 cdh02 和 cdh03 上修改文件 `/etc/yum.repos.d/cloudera-cdh5.repo`

```text
baseurl=http://cdh01/cdh/5/
```

### 2.3. 安装 Zookeeper

.集群规划
|===
| 主机名 | 是否有 `Zookeeper`

| `cdh01` | 有
| `cdh02` | 有
| `cdh03` | 有
|===

`Step 1`: 安装 `Zookeeper`:

* 和以往不同, `CDH` 版本的 `Zookeeper` 是经过定制的, 所以可以直接通过 `Yum` 来安装, 使用刚才所搭建的 `Yum` 仓库, 在所有节点上执行如下命令

```text
yum install -y zookeeper zookeeper-server
```

* `CDH` 版本的所有工具都会遵循 `Linux` 的习惯放置 `Log` 和 `Data`, 所以需要先创建 `Zookeeper` 的数据目录, 并且所有者指定给 `Zookeeper` 所使用的用户, 如下命令在所有节点执行

```text
mkdir -p /var/lib/zookeeper
chown -R zookeeper /var/lib/zookeeper/
```

`Step 2`: 配置 `Zookeeper`:

* 在使用 `Apache` 版本的 `Zookeeper` 时, 我们需要自己创建 `Myid` 文件, 现在使用 `CDH` 版本的 `Zookeeper` 已经为我们提供了对应的 `Shell` 程序, 在所有节点执行如下命令, 注意 `myid` 参数, 在不同节点要修改 `myid`

```text
service zookeeper-server init --myid=1
```

* `Zookeeper` 想要组成集群的话, 必须要修改配置文件, 配置整个集群的服务器地址, `CDH` 版本的 `Zookeeper` 默认配置文件在 `/etc/zookeeper/conf/zoo.cfg`, 修改这个文件增加服务器地址, 在所有节点上修改 `Zookeeper` 的配置文件增加如下

```text
server.1=cdh01:2888:3888
server.2=cdh02:2888:3888
server.3=cdh03:2888:3888
```

`Step 3`: 在所有节点启动 `Zookeeper` 并检查:

* 启动 `CDH` 版本的 `Zookeeper` 也是通过 `Service` 的方式

```text
service zookeeper-server start
```

* 因为 Zookeeper 的搭建比较复杂, 启动完成后可以通过 CDH 提供的命令, 或者使用 Zookeeper 的四字命令来查看是否状态正常

```text
zookeeper-server status
```

NOTE: `CDH` 版本的组件有一个特点, 默认情况下配置文件在 `/etc` 对应目录, 日志在 `/var/log` 对应目录, 数据在 `/var/lib` 对应目录, 例如说 `Zookeeper`, 配置文件放在 `/etc/zookeeper` 中, 日志在 `/var/log/zookeeper` 中, 其它的组件也遵循这样的规律

### 2.4. 安装 Hadoop

.导读
安装软件包
. 配置 HDFS
. 配置 Yarn 和 MapReduce

.集群规划
|===
| 主机名 | 职责

| `cdh01` | `Yarn ResourceManager`, `HDFS NameNode`, `HDFS SecondaryNamenode`, `MapReduce HistroyServer`, `Hadoop Clients`
| `cdh02` | `Yarn NodeManager`, `HDFS DataNode`
| `cdh03` | `Yarn NodeManager`, `HDFS DataNode`
|===

`Step 1`: 安装 `Hadoop` 软件包:

`CDH` 版本的 `Hadoop` 安装主要思路如下

. 下载软件包
. 配置各个组件
. 启动各个组件

所以第一步, 应该先安装 `Hadoop` 的软件包, 只有软件包已经下载, 才能进行相应组件的配置, 根据集群规划进行安装

根据集群规划, cdh01 中应该如下安装软件包

```text
yum -y install hadoop hadoop-yarn-resourcemanager hadoop-yarn-nodemanager hadoop-hdfs-secondarynamenode hadoop-hdfs-namenode hadoop-hdfs-datanode hadoop-mapreduce hadoop-mapreduce-historyserver hadoop-client
```

根据集群规划, cdh02 和 cdh03 中应该如下安装软件包

```text
yum -y install hadoop hadoop-yarn-nodemanager hadoop-hdfs-datanode hadoop-mapreduce hadoop-client
```

`Step 2`: 配置 `HDFS`:

. 配置文件的思路
在 `CDH` 版本的组件中, 配置文件是可以动态变更的

本质上, `CDH` 各组件的配置文件几乎都分布在 `/etc` 目录中, 例如 `Hadoop` 的配置文件就在 `/etc/hadoop/conf` 中, 这个 `conf` 目录是 `Hadoop` 当前所使用的配置文件目录, 但是这个目录其实是一个软链接, 当希望更改配置的时候, 只需要在 `/etc/hadoop` 中创建一个新的目录, 然后将 `conf` 指向这个新目录即可

但是因为各个组件的 `conf` 目录对应了多个目录, 还需要修改其指向, 管理起来很麻烦, 所以 `CDH` 使用了 `Linux` 一个非常厉害的功能, 可以配置一个目录可以指向的多个目录, 同时可以根据优先级确定某个目录指向谁, 这个工具叫做 `alternatives`, 有如下几个常见操作

* `alternatives --install` 讲一个新目录关联进来, 并指定其 ID 和优先级
* `alternatives --set` 设置其指向哪个目录
* `alternatives --display` 展示其指向哪个目录

. 在所有节点中复制原始配置文件并生成新的配置目录, 让 `Hadoop` 使用使用新的配置目录
这样做的目的是尽可能的保留原始配置文件, 以便日后恢复, *所以在所有节点中执行如下操作*

* 创建新的配置目录

```text
cp -r /etc/hadoop/conf.empty /etc/hadoop/conf.itcast
```

* 链接过去, 让 `Hadoop` 读取新的目录

```text
# 关联新的目录和 conf
alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.itcast 50
# 设置指向
alternatives --set hadoop-conf /etc/hadoop/conf.itcast
# 显式当前指向
alternatives --display hadoop-conf
```

. 在所有节点的新配置目录 `/etc/hadoop/conf.itcast` 中, 修改配置文件

* `vi /etc/hadoop/conf.itcast/core-site.xml`

```xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://cdh01:8020</value>
</property>
```

* `vi /etc/hadoop/conf.itcast/hdfs-site.xml`

```xml
<property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///var/lib/hadoop-hdfs/cache/hdfs/dfs/name</value>
</property>
<property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///var/lib/hadoop-hdfs/cache/hdfs/dfs/data</value>
</property>
<property>
    <name>dfs.permissions.superusergroup</name>
    <value>hadoop</value>
</property>
<property>
    <name>dfs.namenode.http-address</name>
    <value>cdh01:50070</value>
</property>
<property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
</property>
```

. 在所有节点中, 创建配置文件指定的 `HDFS` 的 `NameNode` 和 `DataNode` 存放数据的目录, 并处理权限
如下创建所需要的目录

```text
mkdir -p /var/lib/hadoop-hdfs/cache/hdfs/dfs/name
mkdir -p /var/lib/hadoop-hdfs/cache/hdfs/dfs/data
```

* 因为 `CDH` 比较特殊, 其严格按照 `Linux` 用户来管理和启动各个服务, 所以 `HDFS` 启动的时候使用的是 `hdfs` 用户组下的用户 `hdfs`, 需要创建文件后进行权限配置

```text
chown -R hdfs:hdfs /var/lib/hadoop-hdfs/cache/hdfs/dfs/name
chown -R hdfs:hdfs /var/lib/hadoop-hdfs/cache/hdfs/dfs/data
chmod 700 /var/lib/hadoop-hdfs/cache/hdfs/dfs/name
chmod 700 /var/lib/hadoop-hdfs/cache/hdfs/dfs/data
```

. 格式化 `NameNode`, 当然, 这个命令只能在 `cdh01` 上执行, 只能执行一次

```text
sudo -u hdfs hdfs namenode -format
```

. 启动 `HDFS`
`cdh01` 上和 `HDFS` 有关的服务有 `NameNode`, `SecondaryNameNode`, 使用如下命令启动这两个组件

```text
service hadoop-hdfs-namenode start
service hadoop-hdfs-secondarynamenode start
```

. 在 `cdh02` 和 `cdh03` 上执行如下命令

```text
service hadoop-hdfs-datanode start
```

`Step 3`: 配置 `Yarn` 和 `MapReduce`:

前面已经完成配置目录创建等一系列任务了, 所以在配置 `Yarn` 的时候, 只需要去配置以下配置文件即可

. 在所有节点上, 配置 `Yarn` 和 `MapReduce`
修改 `Yarn` 和 `MapReduce` 配置文件

* `vi /etc/hadoop/conf.itcast/mapred-site.xml`

```text
<property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
</property>
<property>
    <name>mapreduce.jobhistory.address</name>
    <value>cdh01:10020</value>
</property>
<property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>cdh01:19888</value>
</property>
<property>
    <name>hadoop.proxyuser.mapred.groups</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.mapred.hosts</name>
    <value>*</value>
</property>
<property>
    <name>yarn.app.mapreduce.am.staging-dir</name>
    <value>/user</value>
</property>
```

* `vi /etc/hadoop/conf.itcast/yarn-site.xml`

```text
<property>
    <name>yarn.resourcemanager.hostname</name>
    <value>cdh01</value>
</property>
<property>
    <name>yarn.application.classpath</name>
    <value>
        $HADOOP_CONF_DIR,
        $HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,
        $HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,
        $HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,
        $HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*
    </value>
</property>
<property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
</property>
<property>
    <name>yarn.nodemanager.local-dirs</name>
    <value>file:///var/lib/hadoop-yarn/cache/${user.name}/nm-local-dir</value>
</property>
<property>
    <name>yarn.nodemanager.log-dirs</name>
    <value>file:///var/log/hadoop-yarn/containers</value>
</property>
<property>
    <name>yarn.log.aggregation-enable</name>
    <value>true</value>
</property>
<property>
    <name>yarn.nodemanager.remote-app-log-dir</name>
    <value>hdfs:///var/log/hadoop-yarn/apps</value>
</property>
```

. 在所有节点上, 创建配置文件指定的存放数据的目录

* 创建 `Yarn` 所需要的数据目录

```text
mkdir -p /var/lib/hadoop-yarn/cache
mkdir -p /var/log/hadoop-yarn/containers
mkdir -p /var/log/hadoop-yarn/apps
```

* 赋予 `Yarn` 用户这些目录的权限

```text
chown -R yarn:yarn /var/lib/hadoop-yarn/cache /var/log/hadoop-yarn/containers /var/log/hadoop-yarn/apps
```

. 为 `MapReduce` 准备 `HDFS` 上的目录, 因为是操作 `HDFS`, 只需要在一个几点执行即可
大致上是需要两种文件夹, 一种用做于缓存, 一种是用户目录

* 为 `MapReduce` 缓存目录赋权

```text
sudo -u hdfs hadoop fs -mkdir /tmp
sudo -u hdfs hadoop fs -chmod -R 1777 /tmp
sudo -u hdfs hadoop fs -mkdir -p /user/history
sudo -u hdfs hadoop fs -chmod -R 1777 /user/history
sudo -u hdfs hadoop fs -chown mapred:hadoop /user/history
```

* 为 `MapReduce` 创建用户目录

```text
sudo -u hdfs hadoop fs -mkdir /user/$USER
sudo -u hdfs hadoop fs -chown $USER /user/$USER
```

. 启动 `Yarn`

* 在 `cdh01` 上启动 `ResourceManager` 和 `HistoryServer`

```text
service hadoop-yarn-resourcemanager start
service hadoop-mapreduce-historyserver start
```

* 在 `cdh02` 和 `cdh03` 上启动 `NodeManager`

```text
service hadoop-yarn-nodemanager start
```

### 2.4. 安装 MySQL

.导读
安装 `MySQL` 有很多方式, 可以直接准备压缩包上传解压安装, 也可以通过 `Yum` 来安装, 从方便和是否主流两个角度来看, 通过 `Yum` 来安装会比较舒服, `MySQL` 默认是单机的, 所以在一个主机上安装即可, 我们选择在 `cdh01` 上安装, 安装大致就是两个步骤

. 安装
. 配置

`Step 1`: 安装:

因为要从 `Yum` 安装, 但是默认的 `Yum` 源是没有 `MySQL` 的, 需要导入 `Oracle` 的源, 然后再安装

. 下载 `Yum` 源配置

```text
wget http://repo.mysql.com/mysql-community-release-el6-5.noarch.rpm
rpm -ivh mysql-community-release-el6-5.noarch.rpm
```

. 安装 MySQL

```text
yum install -y mysql-server
```

`Step 2`: 启动和配置:

现在 `MySQL` 的安全等级默认很高, 所以要通过一些特殊的方式来进行密码设置, 在启动 `MySQL` 以后要单独的进行配置

. 禁用 `MySQL` 的密码验证插件, 不然就必须要使用强度很高的密码, 生产环境建议不要禁用, 要有密码意识

修改 /etc/my.conf 增加如下内容

```text
validate_password=OFF
```

启动 `MySQL`

```text
service mysqld start
```

. 通过 `MySQL` 提供的工具, 设置 `root` 密码

```text
mysql_secure_installation
```

### 2.5. 安装 Hive

.导读
因为 `Hive` 需要使用 `MySQL` 作为元数据库, 所以需要在 `MySQL` 为 `Hive` 创建用户, 创建对应的表

. 安装 `Hive` 软件包
. 在 `MySQL` 中增加 `Hive` 用户
. 配置 `Hive`
. 初始化 `Hive` 在 `MySQL` 中的表结构
. 启动 `Hive`

因为我们并不需要 `Hive` 的 `HA`, 所以在单机部署 `Hive` 即可

`Step 1`: 安装 `Hive` 软件包:

* 安装 `Hive` 依然使用 `CDH` 的 `Yum` 仓库

```text
yum install -y hive hive-metastore hive-server2
```

* 如果想要 `Hive` 使用 `MySQL` 作为元数据库, 那需要给 `Hive` 一个 `MySQL` 的 `JDBC` 包

```text
yum install -y mysql-connector-java
ln -s /usr/share/java/mysql-connector-java.jar /usr/lib/hive/lib/mysql-connector-java.jar
```

`Step 2`: `MySQL` 中增加 `Hive` 用户:

* 进入 `MySQL`

```text
mysql -u root -p
```

* 为 `Hive` 创建数据库

```text
CREATE DATABASE metastore;
USE metastore;
```

* 创建 `Hive` 用户

```text
CREATE USER 'hive'@'%' IDENTIFIED BY 'hive';
```

* 为 `Hive` 用户赋权

```text
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'hive'@'%';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';
FLUSH PRIVILEGES;
```

`Step 3`: 配置 `Hive`:

在启动 `Hive` 之前, 要配置 `Hive` 一些参数, 例如使用 `MySQL` 作为数据库之类的配置

`Hive` 的配置文件在 `/etc/hive/conf/hive-site.xml`, 修改它为如下内容

```text
<!-- /usr/lib/hive/conf/hive-site.xml -->
<property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://cdh01/metastore</value>
</property>
<property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.jdbc.Driver</value>
</property>
<property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
</property>
<property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
</property>
<property>
    <name>datanucleus.autoCreateSchema</name>
    <value>false</value>
</property>
<property>
    <name>datanucleus.fixedDatastore</name>
    <value>true</value>
</property>
<property>
    <name>datanucleus.autoStartMechanism</name>
    <value>SchemaTable</value>
</property>
<property>
    <name>hive.metastore.uris</name>
    <value>thrift://cdh01:9083</value>
</property>
<property>
    <name>hive.metastore.schema.verification</name>
    <value>true</value>
</property>
<property>
    <name>hive.support.concurrency</name>
    <description>Enable Hive's Table Lock Manager Service</description>
    <value>true</value>
</property>
<property>
    <name>hive.support.concurrency</name>
    <value>true</value>
</property>
<property>
    <name>hive.zookeeper.quorum</name>
    <value>cdh01</value>
</property>
```

`Step 4`: 初始化表结构:

使用 `Hive` 之前, `MySQL` 中还没有任何内容, 所以需要先为 `Hive` 初始化数据库, 创建必备的表和模式. `Hive` 提供了方便的工具, 提供 `MySQL` 的连接信息, 即可帮助我们创建对应的表

```text
/usr/lib/hive/bin/schematool -dbType mysql -initSchema -passWord hive -userName hive -url jdbc:mysql://cdh01/metastore
```

`Step 5`: 启动 `Hive`:

默认版本的 `Hive` 只提供了一个 `Shell` 命令, 通过这一个单独的 `Shell` 命令以指定参数的形式启动服务, 但是 `CDH` 版本将 `Hive` 抽取为两个独立服务, 方便通过服务的形式启动 `Hive`, `hive-metastore` 是元数据库, `hive-server2` 是对外提供连接的服务

```text
service hive-metastore start
service hive-server2 start
```

通过 `beeline` 可以连接 `Hive` 验证是否启动成功, 启动 `beeline` 后, 通过如下字符串连接 `Hive`

```text
!connect jdbc:hive2://cdh01:10000 username password org.apache.hive.jdbc.HiveDriver
```

### 2.6. 安装 Kudu

.导读
安装 `Kudu` 依然使用我们已经配置好的 `Yum` 仓库来进行, 整体步骤非常简单, 但是安装上分为 `Master` 和 `Tablet server`

. 安装 Master server
.. 安装软件包
.. 配置
.. 启动
. 安装 Tablet server
.. 安装软件包
.. 配置
.. 启动

.集群规划
|===
| 节点 | 职责

| `cdh01` | `Master server`
| `cdh02` | `Tablet server`
| `cdh03` | `Tablet server`
|===

`Step 1`: 安装 `Master server` 的软件包:

根据集群规划, 尽量让 `cdh01` 少一些负载, 所以只在 `cdh01` 上安装 `Master server`, 命令如下

```text
yum install -y kudu kudu-master kudu-client0 kudu-client-devel
```

`Step 2`: 配置 `Master server`:

`Kudu` 的 `Master server` 没有太多可以配置的项目, 默认的话日志和数据都会写入到 `/var` 目录下, 只需要修改一下 `BlockManager` 的方式即可, 在虚拟机上使用 `Log` 方式可能会带来一些问题, 改为 `File` 方式

```text
--block_manager=file
```

但是有一点需要注意, 一定确保 `ntp` 服务是开启的, 可以使用 `ntpstat` 来查看, 因为 `Kudu` 对时间同步的要求非常高, `ntp` 必须可以自动同步

```text
# 查看时间是否是同步状态
ntpstat
```

`Step 3`: 运行 `Master server`:

* 运行 `Master server`

```text
service kudu-master start
```

* 查看 `Web ui` 确认 `Master server` 已经启动

```text
http://cdh01:8050/masters
```

`Step 4`: 安装 `Tablet server` 的软件包:

根据集群规划, 在 `cdh02`, `cdh03` 中安装 `Tablet server`, 负责更为繁重的工作

```text
yum install -y kudu kudu-tserver kudu-client0 kudu-client-devel
```

`Step 5`: 配置 `Tablet server`:

`Master server` 相对来说没什么需要配置的, 也无须知道各个 `Tablet server` 的位置, 但是对于 `Tablet server` 来说, 必须配置 `Master server` 的位置, 因为一般都是从向主注册自己

在 `cdh02`, `cdh03` 修改 `/etc/kudu/conf/tserver.gflagfile` 为如下内容, 如果有多个 `Master server` 实例, 用逗号分隔地址即可

```text
--tserver_master_addrs=cdh01:7051
```

同时 `Tablet server` 也需要设置 `BlockManager`

```text
--block_manager=file
```

`Step 6`: 运行 `Tablet server`:

* 启动

```text
service kudu-tserver start
```

* 通过 `Web ui` 查看是否已经启动成功

```text
http://cdh02:8051
```

### 2.7. 安装 Impala

.导读
`Kudu` 没有 `SQL` 解析引擎, 因为 `Cloudera` 准备使用 `Impala` 作为 `Kudu` 的 `SQL` 引擎, 所以既然使用 `Kudu` 了, `Impala` 几乎也是必不可少的, 安装 `Impala` 之前, 先了解以下 `Impala` 中有哪些服务

|===
| 服务 | 作用

| `Catalog` | `Impala` 的元信息仓库, 但是不同的是这个 `Catalog` 强依赖 `Hive` 的 `MetaStore`, 会从 `Hive` 处获取元信息
| `StateStore` | `Impala` 的协调节点, 负责异常恢复
| `ImpalaServer` | `Impala` 是 `MPP` 架构, 这是 `Impala` 处理数据的组件, 会读取 `HDFS`, 所以一般和 `DataNode` 部署在一起, 提升性能, 每个 `DataNode` 配一个 `ImpalaServer`
|===

所以, `cdh01` 上应该有 `Catalog` 和 `StateStore`, 而不应该有 `ImpalaServer`, 因为 `cdh01` 中没有 `DataNode`

. 安装 `cdh01` 中的软件包
. 安装其它节点中所需的软件包
. 对所有节点进行配置
. 启动

.集群规划
|===
| 节点 | 职责

| `cdh01` | `impala-state-store`, `impala-catalog`
| `cdh02` | `impala-server`
| `cdh03` | `impala-server`
|===

`Step 1`: 安装软件包:

* 安装主节点 `cdh01` 所需要的软件包

```text
yum install -y impala impala-state-store impala-catalog impala-shell
```

* 安装其它节点所需要的软件包

```text
yum install -y impala impala-server
```

`Step 2`: 针对所有节点进行配置:

* 软链接 `Impala` 所需要的 `Hadoop` 配置文件, 和 `Hive` 的配置文件

`Impala` 需要访问 `Hive` 的 `MetaStore`, 所以需要 `hive-site.xml` 来读取其位置

`Impala` 需要访问 `HDFS`, 所以需要读取 `hdfs-site.xml` 来获取访问信息, 同时也需要读取 `core-site.xml` 获取一些信息

```text
ln -s /etc/hadoop/conf/core-site.xml /etc/impala/conf/core-site.xml
ln -s /etc/hadoop/conf/hdfs-site.xml /etc/impala/conf/hdfs-site.xml
ln -s /etc/hive/conf/hive-site.xml /etc/impala/conf/hive-site.xml
```

* 配置 `Impala` 的主服务位置, 以供 `ImpalaServer(Impalad)` 访问, 修改 `Impala` 的默认配置文件 `/etc/default/impala`, `/etc/default` 往往放置 `CDH` 版本中各组件的环境变量类的配置文件

```text
IMPALA_CATALOG_SERVICE_HOST=cdh01
IMPALA_STATE_STORE_HOST=cdh01
```

`Step 3`: 启动:

* 启动 cdh01

```text
service impala-state-store start
service impala-catalog start
```

* 启动其它节点

```text
service impala-server start
```

* 通过 Web ui 查看是否启动成功

```text
http://cdh01:25000
```

### 2.8. 安装 Hue

.导读
`Hue` 其实就是一个可视化平台, 主要用于浏览 `HDFS` 的文件, 编写和执行 `Hive` 的 `SQL`, 以及 `Impala` 的 `SQL`, 查看数据库中数据等, 而且 `Hue` 一般就作为 `CDH` 数据平台的入口, 所以装了 `CDH` 而不装 `Hue` 会觉得少了点什么, 面试的时候偶尔也会问 `Hue` 的使用, 所以我们简单安装, 简单使用 `Hue` 让大家了解以下这个可视化工具

. `Hue` 组件安装
. 配置 `Hue`
. 启动 `Hue`

`Hue` 只在 `cdh01` 上安装即可

`Step 1`: `Hue` 组件安装:

使用 `Yum` 即可简单安装

```text
yum -y install hue
```

`Step 2`: 配置 `Hue`:

`Hue` 的配置就会稍微优点复杂, 因为 `Hue` 要整合其它的一些工具, 例如访问 `HDFS`, 所以配置要从两方面说, 一是 `HDFS` 要允许 `Hue` 访问, 二是配置给 `Hue` 如何访问 `HDFS` (以及如何访问其它程序)

* 配置 `HDFS`, 允许 `Hue` 的访问

修改 `hdfs-site.xml` 增加如下内容, 以便让 `Hue` 用户可以访问 `HDFS` 中的文件

```text
<property>
    <name>hadoop.proxyuser.hue.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.hue.groups</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.httpfs.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.httpfs.groups</name>
    <value>*</value>
</property>
```

修改 `httpfs-site.xml` 文件, 增加如下内容, 以便让 `Hue` 可以通过 `HttpFS` 访问

```text
<configuration>
    <!-- Hue HttpFS proxy user setting -->
    <property>
      <name>httpfs.proxyuser.hue.hosts</name>
      <value>*</value>
    </property>
    <property>
      <name>httpfs.proxyuser.hue.groups</name>
      <value>*</value>
    </property>
</configuration>
```

* 配置 `Hue`, 告诉 `Hue` 如何访问其它组件和工具

配置 `Hue` 所占用的 `Web` 端口, 在 `/etc/hue/conf/hue.ini` 中搜索 `http_port` 修改为如下

```text
http_host=cdh01
http_port=8888
```

配置 `Impala` 的访问方式, 在 `/etc/hue/conf/hue.ini` 中搜索 `server_host` 修改为如下

```text
server_host=cdh01
```

配置 `Hive` 的访问方式, 在 `/etc/hue/conf/hue.ini` 中搜索 `hive_server_host` 修改为如下

```text
hive_server_host=cdh01
```

`Step 3`: 启动 `Hue`:

使用如下命令即可启动

```text
service hue start
```

.开机要启动的服务
|===
| 服务 | 命令

| `httpd` | `service httpd start`
| `Zookeeper` | `service zookeeper-server start`
| `hdfs-namenode` | `service hadoop-hdfs-namenode start`
| `hdfs-datanode` | `service hadoop-hdfs-datanode start`
| `hdfs-secondarynamenode` | `service hadoop-hdfs-secondarynamenode start`
| `yarn-resourcemanager` | `service hadoop-yarn-resourcemanager start`
| `mapreduce-historyserver` | `service hadoop-mapreduce-historyserver start`
| `yarn-nodemanager` | `service hadoop-yarn-nodemanager start`
| `hive-metastore` | `service hive-metastore start`
| `hive-server2` | `service hive-server2 start`
| `kudu-master` | `service kudu-master start`
| `kudu-tserver` | `service kudu-tserver start`
| `impala-state-store` | `service impala-state-store start`
| `impala-catalog` | `service impala-catalog start`
| `impala-server` | `service impala-server start`
| `hue` | `service hue start`
|===

### 2.8. 使用 Scala 操作 Kudu

.导读
`Kudu API` 结构
. 导入 `Kudu` 所需要的包
. 创建表
. 插入数据
. 查询数据

`Kudu API` 的结构设计:

[cols="3,~"]
|===
| 对象 | 设计

| `Client` a|

创建: 使用 `Kudu master` 服务器地址列表来创建

作用: `Kudu` 的 `API` 主入口, 通过 `Client` 对象获取 `Table` 后才能操作数据

操作:

* 检查表是否存在
* 提交表的 `DDL` 操作, 如 `create`, `delete`, `alter`, 在对表进行 `DDL` 的时候, 需要如下两个对象
** `Kudu Schema` 定义表的列结构
** `Kudu Partial Row` 指定分区方式
* 创建 `Table` 对象

| `Table` a|

创建: 通过 `Client` 对象开启

作用: 通过 `Table` 对象可以操作表中的数据

操作:

* `insert`, `delete`, `update`, `upsert` 行
* 扫描行

| `Scanner` a|

创建: 通过 `Table` 对象开启扫描

作用: 扫描表数据, 并获取结果

操作:

* `Kudu` 中可以通过读取模式空值该读到哪些数据, 有如下三种读取模式
** `READ_LATEST` 是 `Scanner` 的默认模式, 只会返回已经提交的数据, 类似 `ACID` 中的 `ReadCommitted`
** `READ_AT_SNAPSHOT` 读取某一个时间点的数据, 这个模式效率相对比较低, 因为会等待这个时间点之前的所有事务都提交后, 才会返回响应的数据, 类似 `ACID` 中的 `RepeatableRead`
** `READ_YOUR_WRITES` 这种模式会确保读取到自己已经写入的数据, 并且尽可能的忽略其他人的写入, 会导致读取期间有其它人写入但是没有等待, 所以产生的问题就是每次读取的数据可能是不同的, 当前还是实验性功能, 尽量不要使用
|===

导入 `Kudu` 所需要的包:

`Kudu` 并没有提供 `Scala` 单独的客户端 `SDK`, 但是提供了 `Java` 的 `SDK`, 我们使用 `Scala` 访问 `Kudu` 的时候, 可以使用 `Java` 的 `API`, 可以创建一个新的工程开始 `Kudu Scala` 的学习, 创建工程的方式参照 `Spark` 部分第一天, 创建工程后, 需要先做如下两件事

需要导入如下三个 `Maven` 插件:

* `maven-compile-plugin`

`Maven` 的编译插件其实是自动导入的, 现在需要导入这个插件的目的主要是需要通过其指定 `JDK` 的版本

* `maven-shade-plugin`

一般工程打包的方式有两种

* `uber Jar`
直译过来就是胖 `Jar`, 其中包含了所有依赖的 `Jar` 包, 通常会重命名其中一些类以避免冲突,

* `non-uber Jar`
瘦 `Jar`, 没有包含依赖的 `Jar` 包, 在运行的时候使用环境中已有的库

* `scala-maven-plugin`

引入这个插件的主要作用是编译 `Scala` 代码

NOTE: 举个栗子, 比如说现在在本地引入了 `Spark` 的依赖, 要提交代码去集群运行, 但是集群中必然包含了 `Spark` 相关的所有依赖, 那么此时是否需要再生成 `Uber Jar` 了呢? 明显不需要, 因为 `Spark` 在安装部署集群的时候, `Spark` 的软件包内, 有一个 `lib` 目录, 其中所有的 `Jar` 包在运行的时候都会被加载, 完全不需要 `Uber Jar`

NOTE: 再举个栗子, 比如说现在依然是引入 `Spark` 的依赖, 但是同时引入了一个 `JSON` 解析的包, 但是这个 `JSON` 解析的包在集群中并没有, 那么此时如何解决? 有两种方式, 一种是 `Non-Uber Jar`, 但是将依赖的 `Jar` 包在使用 `spark-submit` 命令提交任务的时候通过 `-jar` 参数一并提交过去. 另外一种是直接生成 `Uber Jar` 包含这个 `JSON` 解析库

需要导入一个 `Kudu` 的依赖包:
根据使用 Hadoop 的版本不同, Kudu 的导入方式有两种, 一种是 CDH 版本的依赖, 一种是 Apache 版本的依赖, 我们当前使用 CDH 版本的依赖, 所以需要导入如下包

```xml
<dependency>
    <groupId>org.apache.kudu</groupId>
    <artifactId>kudu-client</artifactId>
    <version>1.7.0-cdh5.16.1</version>
    <scope>provided</scope>
</dependency>
```

整个 `Maven` 文件如下所示:

```xml
<repositories>
    <repository>
        <id>cdh.repo</id>
        <name>Cloudera Repositories</name>
        <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
</repositories>

<properties>
    <junit.version>4.12</junit.version>
    <maven.version>3.5.1</maven.version>
</properties>

<dependencies>
    <!-- Kudu client -->
    <dependency>
        <groupId>org.apache.kudu</groupId>
        <artifactId>kudu-client</artifactId>
        <version>1.7.0-cdh5.16.1</version>
        <scope>provided</scope>
    </dependency>

    <!-- Logging -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>1.7.12</version>
    </dependency>

    <!-- Unit testing -->
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>${junit.version}</version>
        <scope>provided</scope>
    </dependency>
</dependencies>

<build>
    <sourceDirectory>src/main/scala</sourceDirectory>
    <testSourceDirectory>src/test/scala</testSourceDirectory>

    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>${maven.version}</version>
            <configuration>
                <source>1.8</source>
                <target>1.8</target>
            </configuration>
        </plugin>

        <plugin>
            <groupId>net.alchim31.maven</groupId>
            <artifactId>scala-maven-plugin</artifactId>
            <version>3.2.0</version>
            <executions>
                <execution>
                    <goals>
                        <goal>compile</goal>
                        <goal>testCompile</goal>
                    </goals>
                    <configuration>
                        <args>
                            <arg>-dependencyfile</arg>
                            <arg>${project.build.directory}/.scala_dependencies</arg>
                        </args>
                    </configuration>
                </execution>
            </executions>
        </plugin>

        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>2.4</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

创建表:

在进行如下操作之前, 需要先创建 `Scala` 的类, 通过 `Junit` 的方式编写代码, 当然, 也可以创建 `Object`, 编写 `Main` 方法来运行

. 创建 `KuduClient` 实例
. 创建表的列模式 `Schema`
. 创建表

```scala
@Test
def createTable(): Unit = {
  // Kudu 的 Master 地址
  val KUDU_MASTER = "node01:7051"

  // 创建 KuduClient 入口
  val kuduClient = new KuduClientBuilder(KUDU_MASTER).build()

  // 创建列定义的 List
  val columns = List(
    new ColumnSchemaBuilder("key", Type.STRING).build(),
    new ColumnSchemaBuilder("value", Type.STRING).build()
  )

  // 因为是 Java 的 API, 所以在使用 List 的时候要转为 Java 中的 List
  import scala.collection.JavaConverters._
  val javaColumns = columns.asJava

  // 创建 Schema
  val schema = new Schema(javaColumns)

  // 因为 Kudu 必须要指定分区, 所以先创建一个分区键设置
  val keys = List("key").asJava
  val options = new CreateTableOptions().setRangePartitionColumns(keys)

  // 通过 Schema 创建表
  kuduClient.createTable("simple", schema, options)
}
```

插入数据:

扫描查询数据:

### 2.9. 使用 Spark 操作 Kudu

.导读

### 2.10. 使用 Impala 执行 SQL 语句访问 Hive
