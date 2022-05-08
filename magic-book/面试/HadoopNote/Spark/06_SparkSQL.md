---
title: 06_SparkSQL.md
date: 2019/9/5 08:16:25
updated: 2019/9/5 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - Spark
---

## SparkSQL

### 1. SparkSQL 是什么

目标
对于一件事的理解, 应该分为两个大部分, 第一, 它是什么, 第二, 它解决了什么问题
理解为什么会有 `SparkSQL`
理解 `SparkSQL` 所解决的问题, 以及它的使命

#### 1.1. SparkSQL 的出现契机目标

历史前提
发展过程
重要性数据分析的方式
  数据分析的方式大致上可以划分为 `SQL` 和 命令式两种

命令式
在前面的 `RDD` 部分, 非常明显可以感觉的到是命令式的, 主要特征是通过一个算子, 可以得到一个结果, 通过结果再进行后续计算.

```java
sc.textFile("...")
  .flatMap(_.split(" "))
  .map((_, 1))
  .reduceByKey(_ + _)
  .collect()
```

命令式的优点

* 操作粒度更细, 能够控制数据的每一个处理环节
* 操作更明确, 步骤更清晰, 容易维护
* 支持非结构化数据的操作

命令式的缺点

* 需要一定的代码功底
* 写起来比较麻烦

SQL
对于一些数据科学家, 要求他们为了做一个非常简单的查询, 写一大堆代码, 明显是一件非常残忍的事情, 所以 `SQL on Hadoop` 是一个非常重要的方向.

```sql
SELECT
  name,
  age,
  school
FROM students
WHERE age > 10
```

SQL 的优点
表达非常清晰, 比如说这段 `SQL` 明显就是为了查询三个字段, 又比如说这段 `SQL` 明显能看到是想查询年龄大于 10 岁的条目

SQL 的缺点

* 想想一下 3 层嵌套的 `SQL`, 维护起来应该挺力不从心的吧
* 试想一下, 如果使用 `SQL` 来实现机器学习算法, 也挺为难的吧

`SQL` 擅长数据分析和通过简单的语法表示查询, 命令式操作适合过程式处理和算法性的处理. 在 `Spark` 出现之前, 对于结构化数据的查询和处理, 一个工具一向只能支持 `SQL` 或者命令式, 使用者被迫要使用多个工具来适应两种场景, 并且多个工具配合起来比较费劲.
而 `Spark` 出现了以后, 统一了两种数据处理范式, 是一种革新性的进步.
因为 `SQL` 是数据分析领域一个非常重要的范式, 所以 `Spark` 一直想要支持这种范式, 而伴随着一些决策失误, 这个过程其实还是非常曲折的
![image](06_SparkSQL/7a1cdf107b8636713c2502a99d058061-20190907161711003.png)

Hive
解决的问题
`Hive` 实现了 `SQL on Hadoop`, 使用 `MapReduce` 执行任务
简化了 `MapReduce` 任务
新的问题
`Hive` 的查询延迟比较高, 原因是使用 `MapReduce` 做调度

Shark

解决的问题

* `Shark` 改写 `Hive` 的物理执行计划, 使用 `Spark` 作业代替 `MapReduce` 执行物理计划
* 使用列式内存存储
* 以上两点使得 `Shark` 的查询效率很高

新的问题

* `Shark` 重用了 `Hive` 的 `SQL` 解析, 逻辑计划生成以及优化, 所以其实可以认为 `Shark` 只是把 `Hive` 的物理执行替换为了 `Spark` 作业
* 执行计划的生成严重依赖 `Hive`, 想要增加新的优化非常困难
* `Hive` 使用 `MapReduce` 执行作业, 所以 `Hive` 是进程级别的并行, 而 `Spark` 是线程级别的并行, 所以 `Hive` 中很多线程不安全的代码不适用于 `Spark`

由于以上问题, `Shark` 维护了 `Hive` 的一个分支, 并且无法合并进主线, 难以为继

`SparkSQL`

解决的问题

* `Spark SQL` 使用 `Hive` 解析 `SQL` 生成 `AST` 语法树, 将其后的逻辑计划生成, 优化, 物理计划都自己完成, 而不依赖 `Hive`
* 执行计划和优化交给优化器 `Catalyst`
* 内建了一套简单的 `SQL` 解析器, 可以不使用 `HQL`, 此外, 还引入和 `DataFrame` 这样的 `DSL API`, 完全可以不依赖任何 `Hive` 的组件
* `Shark` 只能查询文件, `Spark SQL` 可以直接降查询作用于 `RDD`, 这一点是一个大进步

新的问题
对于初期版本的 `SparkSQL`, 依然有挺多问题, 例如只能支持 `SQL` 的使用, 不能很好的兼容命令式, 入口不够统一等

`Dataset`

`SparkSQL` 在 2.0 时代, 增加了一个新的 `API`, 叫做 `Dataset`, `Dataset` 统一和结合了 `SQL` 的访问和命令式 `API` 的使用, 这是一个划时代的进步

在 `Dataset` 中可以轻易的做到使用 `SQL` 查询并且筛选数据, 然后使用命令式 `API` 进行探索式分析

.重要性

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/9b1db9d54c796e0eb6769cafd2ef19ac.png)

`SparkSQL` 不只是一个 `SQL` 引擎, `SparkSQL` 也包含了一套对 *结构化数据的命令式 `API`*, 事实上, 所有 `Spark` 中常见的工具, 都是依赖和依照于 `SparkSQL` 的 `API` 设计的
总结: `SparkSQL` 是什么

`SparkSQL` 是一个为了支持 `SQL` 而设计的工具, 但同时也支持命令式的 `API`

#### 1.2. SparkSQL 的适用场景目标

理解 `SparkSQL` 的适用场景

|==
| |定义|特点|举例

| *结构化数据* | 有固定的 `Schema` | 有预定义的 `Schema` | 关系型数据库的表
| *半结构化数据* | 没有固定的 `Schema`, 但是有结构 | 没有固定的 `Schema`, 有结构信息, 数据一般是自描述的 | 指一些有结构的文件格式, 例如 `JSON`
| *非结构化数据* | 没有固定 `Schema`, 也没有结构 | 没有固定 `Schema`, 也没有结构 | 指文档图片之类的格式
|==

结构化数据

一般指数据有固定的 `Schema`, 例如在用户表中, `name` 字段是 `String` 型, 那么每一条数据的 `name` 字段值都可以当作 `String` 来使用

```text
+```+`````````--+``````````````````---+```---+``````-| id | name         | url                       | alexa | country |
+```+`````````--+``````````````````---+```---+``````-| 1  | Google       | https://www.google.cm/    | 1     | USA     |
| 2  | 淘宝          | https://www.taobao.com/   | 13    | CN      |
| 3  | 菜鸟教程      | http://www.runoob.com/    | 4689  | CN      |
| 4  | 微博          | http://weibo.com/         | 20    | CN      |
| 5  | Facebook     | https://www.facebook.com/ | 3     | USA     |
+```+`````````--+``````````````````---+```---+``````-```

半结构化数据

一般指的是数据没有固定的 `Schema`, 但是数据本身是有结构的

​```json
{
     "firstName": "John",
     "lastName": "Smith",
     "age": 25,
     "phoneNumber":
     [
         {
           "type": "home",
           "number": "212 555-1234"
         },
         {
           "type": "fax",
           "number": "646 555-4567"
         }
     ]
 }
```

没有固定 `Schema`
指的是半结构化数据是没有固定的 `Schema` 的, 可以理解为没有显式指定 `Schema` 比如说一个用户信息的 `JSON` 文件, 第一条数据的 `phone_num` 有可能是 `String`, 第二条数据虽说应该也是 `String`, 但是如果硬要指定为 `BigInt`, 也是有可能的 因为没有指定 `Schema`, 没有显式的强制的约束

有结构

虽说半结构化数据是没有显式指定 `Schema` 的, 也没有约束, 但是半结构化数据本身是有有隐式的结构的, 也就是数据自身可以描述自身 例如 `JSON` 文件, 其中的某一条数据是有字段这个概念的, 每个字段也有类型的概念, 所以说 `JSON` 是可以描述自身的, 也就是数据本身携带有元信息

`SparkSQL` 处理什么数据的问题?

* `Spark` 的 `RDD` 主要用于处理 *非结构化数据* 和 *半结构化数据*
* `SparkSQL` 主要用于处理 *结构化数据*

`SparkSQL` 相较于 `RDD` 的优势在哪?

* `SparkSQL` 提供了更好的外部数据源读写支持
 因为大部分外部数据源是有结构化的, 需要在 `RDD` 之外有一个新的解决方案, 来整合这些结构化数据源
* `SparkSQL` 提供了直接访问列的能力
 因为 `SparkSQL` 主要用做于处理结构化数据, 所以其提供的 `API` 具有一些普通数据库的能力总结: `SparkSQL` 适用于什么场景?

`SparkSQL` 适用于处理结构化数据的场景
本章总结

* `SparkSQL` 是一个即支持 `SQL` 又支持命令式数据处理的工具
* `SparkSQL` 的主要适用场景是处理结构化数据

### 2. SparkSQL 初体验目标

. 了解 `SparkSQL` 的 `API` 由哪些部分组成

#### 2.3. RDD 版本的 WordCount

```java
val config = new SparkConf().setAppName("ip_ana").setMaster("local[6]")
val sc = new SparkContext(config)

sc.textFile("hdfs://node01:8020/dataset/wordcount.txt")
  .flatMap(_.split(" "))
  .map((_, 1))
  .reduceByKey(_ + _)
  .collect
```

* `RDD` 版本的代码有一个非常明显的特点, 就是它所处理的数据是基本类型的, 在算子中对整个数据进行处理

#### 2.2. 命令式 API 的入门案例

```text
case class People(name: String, age: Int)

val spark: SparkSession = new sql.SparkSession.Builder()       // <1>
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

import spark.implicits._

val peopleRDD: RDD[People] = spark.sparkContext.parallelize(Seq(People("zhangsan", 9), People("lisi", 15)))
val peopleDS: Dataset[People] = peopleRDD.toDS()               // <2>
val teenagers: Dataset[String] = peopleDS.where('age > 10)     // <3>
  .where('age < 20)
  .select('name)
  .as[String]

/*
+```|name|
+```|lisi|
+```*/
teenagers.show()
```

<1> SparkSQL 中有一个新的入口点, 叫做 SparkSession
<2> SparkSQL 中有一个新的类型叫做 Dataset
<3> SparkSQL 有能力直接通过字段名访问数据集, 说明 SparkSQL 的 API 中是携带 Schema 信息的

.SparkSession
  `SparkContext` 作为 `RDD` 的创建者和入口, 其主要作用有如下两点

* 创建 `RDD`, 主要是通过读取文件创建 `RDD`
* 监控和调度任务, 包含了一系列组件, 例如 `DAGScheduler`, `TaskSheduler`

为什么无法使用 `SparkContext` 作为 `SparkSQL` 的入口?

* `SparkContext` 在读取文件的时候, 是不包含 `Schema` 信息的, 因为读取出来的是 `RDD`
* `SparkContext` 在整合数据源如 `Cassandra`, `JSON`, `Parquet` 等的时候是不灵活的, 而 `DataFrame` 和 `Dataset` 一开始的设计目标就是要支持更多的数据源
* `SparkContext` 的调度方式是直接调度 `RDD`, 但是一般情况下针对结构化数据的访问, 会先通过优化器优化一下

所以 `SparkContext` 确实已经不适合作为 `SparkSQL` 的入口, 所以刚开始的时候 `Spark` 团队为 `SparkSQL` 设计了两个入口点, 一个是 `SQLContext` 对应 `Spark` 标准的 `SQL` 执行, 另外一个是 `HiveContext` 对应 `HiveSQL` 的执行和 `Hive` 的支持.

在 `Spark 2.0` 的时候, 为了解决入口点不统一的问题, 创建了一个新的入口点 `SparkSession`, 作为整个 `Spark` 生态工具的统一入口点, 包括了 `SQLContext`, `HiveContext`, `SparkContext` 等组件的功能

新的入口应该有什么特性?

* 能够整合 `SQLContext`, `HiveContext`, `SparkContext`, `StreamingContext` 等不同的入口点
* 为了支持更多的数据源, 应该完善读取和写入体系
* 同时对于原来的入口点也不能放弃, 要向下兼容
  
.DataFrame & Dataset

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/eca0d2e1e2b5ce678161438d87707b61.png)

`SparkSQL` 最大的特点就是它针对于结构化数据设计, 所以 `SparkSQL` 应该是能支持针对某一个字段的访问的, 而这种访问方式有一个前提, 就是 `SparkSQL` 的数据集中, 要 *包含结构化信息*, 也就是俗称的 `Schema`

而 `SparkSQL` 对外提供的 `API` 有两类, 一类是直接执行 `SQL`, 另外一类就是命令式`SparkSQL` 提供的命令式 `API` 就是 `DataFrame` 和 `Dataset`, 暂时也可以认为 `DataFrame` 就是 `Dataset`, 只是在不同的 `API` 中返回的是 `Dataset` 的不同表现形式

```scala
// RDD
rdd.map { case Person(id, name, age) => (age, 1) }
  .reduceByKey {case ((age, count), (totalAge, totalCount)) => (age, count + totalCount)}

// DataFrame
df.groupBy("age").count("age")
```

通过上面的代码, 可以清晰的看到, `SparkSQL` 的命令式操作相比于 `RDD` 来说, 可以直接通过 `Schema` 信息来访问其中某个字段, 非常的方便

#### 2.2. SQL 版本 WordCount

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

import spark.implicits._

val peopleRDD: RDD[People] = spark.sparkContext.parallelize(Seq(People("zhangsan", 9), People("lisi", 15)))
val peopleDS: Dataset[People] = peopleRDD.toDS()
peopleDS.createOrReplaceTempView("people")

val teenagers: DataFrame = spark.sql("select name from people where age > 10 and age < 20")

/*
+```|name|
+```|lisi|
+``` */
teenagers.show()
```

以往使用 `SQL` 肯定是要有一个表的, 在 `Spark` 中, 并不存在表的概念, 但是有一个近似的概念, 叫做 `DataFrame`, 所以一般情况下要先通过 `DataFrame` 或者 `Dataset` 注册一张临时表, 然后使用 `SQL` 操作这张临时表总结

`SparkSQL` 提供了 `SQL` 和 命令式 `API` 两种不同的访问结构化数据的形式, 并且它们之间可以无缝的衔接

命令式 `API` 由一个叫做 `Dataset` 的组件提供, 其还有一个变形, 叫做 `DataFrame`

### 3. [扩展] Catalyst 优化器目标

. 理解 `SparkSQL` 和以 `RDD` 为代表的 `SparkCore` 最大的区别
. 理解优化器的运行原理和作用

#### 3.1. RDD 和 SparkSQL 运行时的区别

`RDD` 的运行流程

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/1e627dcc1dc31f721933d3e925fa318b.png)

大致运行步骤
先将 `RDD` 解析为由 `Stage` 组成的 `DAG`, 后将 `Stage` 转为 `Task` 直接运行

问题
任务会按照代码所示运行, 依赖开发者的优化, 开发者的会在很大程度上影响运行效率

解决办法
创建一个组件, 帮助开发者修改和优化代码, 但是这在 `RDD` 上是无法实现的

为什么 `RDD` 无法自我优化?

* `RDD` 没有 `Schema` 信息
* `RDD` 可以同时处理结构化和非结构化的数据

`SparkSQL` 提供了什么?

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/72e4d163c029f86fafcfa083e6cf6eda.png)

和 `RDD` 不同, `SparkSQL` 的 `Dataset` 和 `SQL` 并不是直接生成计划交给集群执行, 而是经过了一个叫做 `Catalyst` 的优化器, 这个优化器能够自动帮助开发者优化代码

也就是说, 在 `SparkSQL` 中, 开发者的代码即使不够优化, 也会被优化为相对较好的形式去执行

为什么 `SparkSQL` 提供了这种能力?:
首先, `SparkSQL` 大部分情况用于处理结构化数据和半结构化数据, 所以 `SparkSQL` 可以获知数据的 `Schema`, 从而根据其 `Schema` 来进行优化

#### 3.2. Catalyst

[NOTE]

为了解决过多依赖 `Hive` 的问题, `SparkSQL` 使用了一个新的 `SQL` 优化器替代 `Hive` 中的优化器, 这个优化器就是 `Catalyst`, 整个 `SparkSQL` 的架构大致如下

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/4d025ea8579395f704702eb94572b8de.png)

1`API` 层简单的说就是 `Spark` 会通过一些 `API` 接受 `SQL` 语句
2. 收到 `SQL` 语句以后, 将其交给 `Catalyst`, `Catalyst` 负责解析 `SQL`, 生成执行计划等
3`Catalyst` 的输出应该是 `RDD` 的执行计划
4. 最终交由集群运行

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/67b14d92b21b191914800c384cbed439.png)
Step 1 : 解析 `SQL`, 并且生成 `AST` (抽象语法树)
![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/5c0e91faae9043400c11bf68c20031a2.png)
Step 2 : 在 `AST` 中加入元数据信息, 做这一步主要是为了一些优化, 例如 `col = col` 这样的条件, 下图是一个简略图, 便于理解
![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/02afbb7533249cc6024c2dfc2ee4891e.png)

* `score.id -> id#1#L` 为 `score.id` 生成 `id` 为 1, 类型是 `Long`
* `score.math_score -> math_score#2#L` 为 `score.math_score` 生成 `id` 为 2, 类型为 `Long`
* `people.id -> id#3#L` 为 `people.id` 生成 `id` 为 3, 类型为 `Long`
* `people.age -> age#4#L` 为 `people.age` 生成 `id` 为 4, 类型为 `Long`

Step 3 : 对已经加入元数据的 `AST`, 输入优化器, 进行优化, 从两种常见的优化开始, 简单介绍
![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/07142425c65dc6d921451a8bdec8a29d.png)

* 谓词下推 `Predicate Pushdown`, 将 `Filter` 这种可以减小数据集的操作下推, 放在 `Scan` 的位置, 这样可以减少操作时候的数据量
![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/7b58443ef6ace60d269d704c1f4eae21.png)

* 列值裁剪 `Column Pruning`, 在谓词下推后, `people` 表之上的操作只用到了 `id` 列, 所以可以把其它列裁剪掉, 这样可以减少处理的数据量, 从而优化处理速度

* 还有其余很多优化点, 大概一共有一二百种, 随着 `SparkSQL` 的发展, 还会越来越多, 感兴趣的同学可以继续通过源码了解, 源码在  `org.apache.spark.sql.catalyst.optimizer.Optimizer`

Step 4 : 上面的过程生成的 `AST` 其实最终还没办法直接运行, 这个 `AST` 叫做 `逻辑计划`, 结束后, 需要生成 `物理计划`, 从而生成 `RDD` 来运行

* 在生成`物理计划`的时候, 会经过`成本模型`对整棵树再次执行优化, 选择一个更好的计划
* 在生成`物理计划`以后, 因为考虑到性能, 所以会使用代码生成, 在机器中运行

[NOTE]

可以使用 `queryExecution` 方法查看逻辑执行计划, 使用 `explain` 方法查看物理执行计划
![image](:http://nos.netease.com/knowledge/6dd59b15-d810-4f1e-ab52-c1ecfe0bddcd)
![image](:http://nos.netease.com/knowledge/6281b141-af94-41e7-8953-d33b0a6d04d0)

也可以使用 `Spark WebUI` 进行查看
![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/7884408908284ba4ebc57b0f1360bc03.png)
总结

`SparkSQL` 和 `RDD` 不同的主要点是在于其所操作的数据是结构化的, 提供了对数据更强的感知和分析能力, 能够对代码进行更深层的优化, 而这种能力是由一个叫做 `Catalyst` 的优化器所提供的

`Catalyst` 的主要运作原理是分为三步, 先对 `SQL` 或者 `Dataset` 的代码解析, 生成逻辑计划, 后对逻辑计划进行优化, 再生成物理计划, 最后生成代码到集群中以 `RDD` 的形式运行

### 4. Dataset 的特点目标

. 理解 `Dataset` 是什么
. 理解 `Dataset` 的特性

`Dataset` 是什么?

  ```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

import spark.implicits._

val dataset: Dataset[People] = spark.createDataset(Seq(People("zhangsan", 9), People("lisi", 15)))
// 方式1: 通过对象来处理
dataset.filter(item => item.age > 10).show()
// 方式2: 通过字段来处理
dataset.filter('age > 10).show()
// 方式3: 通过类似SQL的表达式来处理
dataset.filter("age > 10").show()
  ```

问题1: `People` 是什么?:
`People` 是一个强类型的类

问题2: 这个 `Dataset` 中是结构化的数据吗?:
非常明显是的, 因为 `People` 对象中有结构信息, 例如字段名和字段类型

问题3: 这个 `Dataset` 能够使用类似 `SQL` 这样声明式结构化查询语句的形式来查询吗?:
当然可以, 已经演示过了

问题4: `Dataset` 是什么?:
`Dataset` 是一个强类型, 并且类型安全的数据容器, 并且提供了结构化查询 `API` 和类似 `RDD` 一样的命令式 `API`

即使使用 `Dataset` 的命令式 `API`, 执行计划也依然会被优化
  `Dataset` 具有 `RDD` 的方便, 同时也具有 `DataFrame` 的性能优势, 并且 `Dataset` 还是强类型的, 能做到类型安全.

```text
scala> spark.range(1).filter('id =### 0).explain(true)

### Parsed Logical Plan ==
'Filter ('id = 0)
+- Range (0, 1, splits=8)

### Analyzed Logical Plan ==
id: bigint
Filter (id#51L = cast(0 as bigint))
+- Range (0, 1, splits=8)

### Optimized Logical Plan ==
Filter (id#51L = 0)
+- Range (0, 1, splits=8)

### Physical Plan ==
*Filter (id#51L = 0)
+- *Range (0, 1, splits=8)
```

`Dataset` 的底层是什么?
  `Dataset` 最底层处理的是对象的序列化形式, 通过查看 `Dataset` 生成的物理执行计划, 也就是最终所处理的 `RDD`, 就可以判定 `Dataset` 底层处理的是什么形式的数据

```scala
val dataset: Dataset[People] = spark.createDataset(Seq(People("zhangsan", 9), People("lisi", 15)))
val internalRDD: RDD[InternalRow] = dataset.queryExecution.toRdd
```

`dataset.queryExecution.toRdd` 这个 `API` 可以看到 `Dataset` 底层执行的 `RDD`, 这个 `RDD` 中的范型是 `InternalRow`, `InternalRow` 又称之为 `Catalyst Row`, 是 `Dataset` 底层的数据结构, 也就是说, 无论 `Dataset` 的范型是什么, 无论是 `Dataset[Person]` 还是其它的, 其最底层进行处理的数据结构都是 `InternalRow`

所以, `Dataset` 的范型对象在执行之前, 需要通过 `Encoder` 转换为 `InternalRow`, 在输入之前, 需要把 `InternalRow` 通过 `Decoder` 转换为范型对象

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/cc610157b92466cac52248a8bf72b76e.png)

可以获取 `Dataset` 对应的 `RDD` 表示
  在 `Dataset` 中, 可以使用一个属性 `rdd` 来得到它的 `RDD` 表示, 例如 `Dataset[T] -> RDD[T]`

```scala
val dataset: Dataset[People] = spark.createDataset(Seq(People("zhangsan", 9), People("lisi", 15)))

/*
(2) MapPartitionsRDD[3] at rdd at Testing.scala:159 )
 |  MapPartitionsRDD[2] at rdd at Testing.scala:159 )
 |  MapPartitionsRDD[1] at rdd at Testing.scala:159 )
 |  ParallelCollectionRDD[0] at rdd at Testing.scala:159 )
 */
// <1>
println(dataset.rdd.toDebugString) // 这段代码的执行计划为什么多了两个步骤?

/*
(2) MapPartitionsRDD[5] at toRdd at Testing.scala:160 )
 |  ParallelCollectionRDD[4] at toRdd at Testing.scala:160 )
 */
// <2>
println(dataset.queryExecution.toRdd.toDebugString)
```

<1> 使用 `Dataset.rdd` 将 `Dataset` 转为 `RDD` 的形式
<2> `Dataset` 的执行计划底层的 `RDD`

可以看到 `(1)` 对比 `(2)` 对了两个步骤, 这两个步骤的本质就是将 `Dataset` 底层的 `InternalRow` 转为 `RDD` 中的对象形式, 这个操作还是会有点重的, 所以慎重使用 `rdd` 属性来转换 `Dataset` 为 `RDD`
总结

`Dataset` 是一个新的 `Spark` 组件, 其底层还是 `RDD`
`Dataset` 提供了访问对象中某个特定字段的能力, 不用像 `RDD` 一样每次都要针对整个对象做操作
`Dataset` 和 `RDD` 不同, 如果想把 `Dataset[T]` 转为 `RDD[T]`, 则需要对 `Dataset` 底层的 `InternalRow` 做转换, 是一个比价重量级的操作

### 5. DataFrame 的作用和常见操作目标

. 理解 `DataFrame` 是什么
. 理解 `DataFrame` 的常见操作

`DataFrame` 是什么?
  `DataFrame` 是 `SparkSQL` 中一个表示关系型数据库中 `表` 的函数式抽象, 其作用是让 `Spark` 处理大规模结构化数据的时候更加容易. 一般 `DataFrame` 可以处理结构化的数据, 或者是半结构化的数据, 因为这两类数据中都可以获取到 `Schema` 信息. 也就是说 `DataFrame` 中有 `Schema` 信息, 可以像操作表一样操作 `DataFrame`.

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/eca0d2e1e2b5ce678161438d87707b61.png)

`DataFrame` 由两部分构成, 一是 `row` 的集合, 每个 `row` 对象表示一个行, 二是描述 `DataFrame` 结构的 `Schema`.

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/238c241593cd5b0fd06d4d74294680e2.png)

`DataFrame` 支持 `SQL` 中常见的操作, 例如: `select`, `filter`, `join`, `group`, `sort`, `join` 等

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

import spark.implicits._

val peopleDF: DataFrame = Seq(People("zhangsan", 15), People("lisi", 15)).toDF()

/*
+---+```-|age|count|
+---+```-| 15|    2|
+---+```- */
peopleDF.groupBy('age)
  .count()
  .show()
```

通过隐式转换创建 `DataFrame`
  这种方式本质上是使用 `SparkSession` 中的隐式转换来进行的

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

// 必须要导入隐式转换
// 注意: spark 在此处不是包, 而是 SparkSession 对象
import spark.implicits._

val peopleDF: DataFrame = Seq(People("zhangsan", 15), People("lisi", 15)).toDF()
```

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/841503b4240e7a8ecac62d92203e9943.png)

根据源码可以知道, `toDF` 方法可以在 `RDD` 和 `Seq` 中使用

通过集合创建 `DataFrame` 的时候, 集合中不仅可以包含样例类, 也可以只有普通数据类型, 后通过指定列名来创建

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

import spark.implicits._

val df1: DataFrame = Seq("nihao", "hello").toDF("text")

/*
+```-| text|
+```-|nihao|
|hello|
+```- */
df1.show()

val df2: DataFrame = Seq(("a", 1), ("b", 1)).toDF("word", "count")

/*
+```+```-|word|count|
+```+```-|   a|    1|
|   b|    1|
+```+```- */
df2.show()
```

通过外部集合创建 `DataFrame`

  ```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

val df = spark.read
  .option("header", true)
  .csv("dataset/BeijingPM20100101_20151231.csv")
df.show(10)
df.printSchema()
  ```

不仅可以从 `csv` 文件创建 `DataFrame`, 还可以从 `Table`, `JSON`, `Parquet` 等中创建 `DataFrame`, 后续会有单独的章节来介绍

在 `DataFrame` 上可以使用的常规操作

需求: 查看每个月的统计数量

Step 1: 首先可以打印 `DataFrame` 的 `Schema`, 查看其中所包含的列, 以及列的类型:

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

val df = spark.read
  .option("header", true)
  .csv("dataset/BeijingPM20100101_20151231.csv")

df.printSchema()
```

Step 2: 对于大部分计算来说, 可能不会使用所有的列, 所以可以选择其中某些重要的列:

```scala
...

df.select('year, 'month, 'PM_Dongsi)
```

Step 3: 可以针对某些列进行分组, 后对每组数据通过函数做聚合:

```scala
...

df.select('year, 'month, 'PM_Dongsi)
  .where('PM_Dongsi =!= "Na")
  .groupBy('year, 'month)
  .count()
  .show()
```

使用 `SQL` 操作 `DataFrame`
  使用 `SQL` 来操作某个 `DataFrame` 的话, `SQL` 中必须要有一个 `from` 子句, 所以需要先将 `DataFrame` 注册为一张临时表

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

val df = spark.read
  .option("header", true)
  .csv("dataset/BeijingPM20100101_20151231.csv")

df.createOrReplaceTempView("temp_table")

spark.sql("select year, month, count(*) from temp_table where PM_Dongsi != 'NA' group by year, month")
  .show()
```

总结

`DataFrame` 是一个类似于关系型数据库表的函数式组件
`DataFrame` 一般处理结构化数据和半结构化数据
`DataFrame` 具有数据对象的 Schema 信息
. 可以使用命令式的 `API` 操作 `DataFrame`, 同时也可以使用 `SQL` 操作 `DataFrame`
`DataFrame` 可以由一个已经存在的集合直接创建, 也可以读取外部的数据源来创建

### 6. Dataset 和 DataFrame 的异同目标

. 理解 `Dataset` 和 `DataFrame` 之间的关系

`DataFrame` 就是 `Dataset`
  根据前面的内容, 可以得到如下信息

`Dataset` 中可以使用列来访问数据, `DataFrame` 也可以
`Dataset` 的执行是优化的, `DataFrame` 也是
`Dataset` 具有命令式 `API`, 同时也可以使用 `SQL` 来访问, `DataFrame` 也可以使用这两种不同的方式访问

所以这件事就比较蹊跷了, 两个这么相近的东西为什么会同时出现在 `SparkSQL` 中呢?

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/44fb917304a91eab99d131010448331b.png)

确实, 这两个组件是同一个东西, `DataFrame` 是 `Dataset` 的一种特殊情况, 也就是说 `DataFrame` 是 `Dataset[Row]` 的别名

`DataFrame` 和 `Dataset` 所表达的语义不同
  *第一点: `DataFrame` 表达的含义是一个支持函数式操作的 `表`, 而 `Dataset` 表达是是一个类似 `RDD` 的东西, `Dataset` 可以处理任何对象*

第二点: `DataFrame` 中所存放的是 `Row` 对象, 而 `Dataset` 中可以存放任何类型的对象

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

import spark.implicits._

val df: DataFrame = Seq(People("zhangsan", 15), People("lisi", 15)).toDF()       // <1>

val ds: Dataset[People] = Seq(People("zhangsan", 15), People("lisi", 15)).toDS() // <2>
```

<1> DataFrame 就是 Dataset[Row]
<2> Dataset 的范型可以是任意类型

第三点: `DataFrame` 的操作方式和 `Dataset` 是一样的, 但是对于强类型操作而言, 它们处理的类型不同

`DataFrame` 在进行强类型操作时候, 例如 `map` 算子, 其所处理的数据类型永远是 `Row`

```scala
df.map( (row: Row) => Row(row.get(0), row.getAs[Int](1) * 10) )(RowEncoder.apply(df.schema)).show()
```

但是对于 `Dataset` 来讲, 其中是什么类型, 它就处理什么类型

```scala
ds.map( (item: People) => People(item.name, item.age * 10) ).show()
```

第三点: `DataFrame` 只能做到运行时类型检查, `Dataset` 能做到编译和运行时都有类型检查

`DataFrame` 中存放的数据以 `Row` 表示, 一个 `Row` 代表一行数据, 这和关系型数据库类似
`DataFrame` 在进行 `map` 等操作的时候, `DataFrame` 不能直接使用 `Person` 这样的 `Scala` 对象, 所以无法做到编译时检查
`Dataset` 表示的具体的某一类对象, 例如 `Person`, 所以再进行 `map` 等操作的时候, 传入的是具体的某个 `Scala` 对象, 如果调用错了方法, 编译时就会被检查出来

```scala
val ds: Dataset[People] = Seq(People("zhangsan", 15), People("lisi", 15)).toDS()
ds.map(person => person.hello) // <1>
```

<1> 这行代码明显报错, 无法通过编译

`Row` 是什么?
  `Row` 对象表示的是一个 `行`

`Row` 的操作类似于 `Scala` 中的 `Map` 数据类型

```scala
// 一个对象就是一个对象
val p = People(name = "zhangsan", age = 10)

// 同样一个对象, 还可以通过一个 Row 对象来表示
val row = Row("zhangsan", 10)

// 获取 Row 中的内容
println(row.get(1))
println(row(1))

// 获取时可以指定类型
println(row.getAs[Int](1))

// 同时 Row 也是一个样例类, 可以进行 match
row match {
  case Row(name, age) => println(name, age)
}
```

`DataFrame` 和 `Dataset` 之间可以非常简单的相互转换

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

import spark.implicits._

val df: DataFrame = Seq(People("zhangsan", 15), People("lisi", 15)).toDF()
val ds_fdf: Dataset[People] = df.as[People]

val ds: Dataset[People] = Seq(People("zhangsan", 15), People("lisi", 15)).toDS()
val df_fds: DataFrame = ds.toDF()
​```总结

`DataFrame` 就是 `Dataset`, 他们的方式是一样的, 也都支持 `API` 和 `SQL` 两种操作方式
`DataFrame` 只能通过表达式的形式, 或者列的形式来访问数据, 只有 `Dataset` 支持针对于整个对象的操作
`DataFrame` 中的数据表示为 `Row`, 是一个行的概念


## 7. 数据读写目标

. 理解外部数据源的访问框架
. 掌握常见的数据源读写方式


### 7.1. 初识 DataFrameReader

.目标

* 理解 `DataFrameReader` 的整体结构和组成


`SparkSQL` 的一个非常重要的目标就是完善数据读取, 所以 `SparkSQL` 中增加了一个新的框架, 专门用于读取外部数据源, 叫做 `DataFrameReader`

​```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrameReader

val spark: SparkSession = ...

val reader: DataFrameReader = spark.read
```

`DataFrameReader` 由如下几个组件组成

|===
| 组件 | 解释

| `schema` | 结构信息, 因为 `Dataset` 是有结构的, 所以在读取数据的时候, 就需要有 `Schema` 信息, 有可能是从外部数据源获取的, 也有可能是指定的
| `option` | 连接外部数据源的参数, 例如 `JDBC` 的 `URL`, 或者读取 `CSV` 文件是否引入 `Header` 等
| `format` | 外部数据源的格式, 例如 `csv`, `jdbc`, `json` 等
|===

`DataFrameReader` 有两种访问方式, 一种是使用 `load` 方法加载, 使用 `format` 指定加载格式, 还有一种是使用封装方法, 类似 `csv`, `json`, `jdbc` 等

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

val spark: SparkSession = ...

// 使用 load 方法
val fromLoad: DataFrame = spark
  .read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .load("dataset/BeijingPM20100101_20151231.csv")

// Using format-specific load operator
val fromCSV: DataFrame = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("dataset/BeijingPM20100101_20151231.csv")
```

但是其实这两种方式本质上一样, 因为类似 `csv` 这样的方式只是 `load` 的封装

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/e8af7d7e5ec256de27b2e40c8449a906.png)

[NOTE]

如果使用 `load` 方法加载数据, 但是没有指定 `format` 的话, 默认是按照 `Parquet` 文件格式读取

也就是说, `SparkSQL` 默认的读取格式是 `Parquet`
总结

. 使用 `spark.read` 可以获取 SparkSQL 中的外部数据源访问框架 `DataFrameReader`
`DataFrameReader` 有三个组件 `format`, `schema`, `option`
`DataFrameReader` 有两种使用方式, 一种是使用 `load` 加 `format` 指定格式, 还有一种是使用封装方法 `csv`, `json` 等

### 7.2. 初识 DataFrameWriter目标

. 理解 `DataFrameWriter` 的结构

对于 `ETL` 来说, 数据保存和数据读取一样重要, 所以 `SparkSQL` 中增加了一个新的数据写入框架, 叫做 `DataFrameWriter`

```scala
val spark: SparkSession = ...

val df = spark.read
      .option("header", true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

val writer: DataFrameWriter[Row] = df.write
```

`DataFrameWriter` 中由如下几个部分组成

|===
| 组件 | 解释

| `source` | 写入目标, 文件格式等, 通过 `format` 方法设定
| `mode` | 写入模式, 例如一张表已经存在, 如果通过 `DataFrameWriter` 向这张表中写入数据, 是覆盖表呢, 还是向表中追加呢? 通过 `mode` 方法设定
| `extraOptions` | 外部参数, 例如 `JDBC` 的 `URL`, 通过 `options`, `option` 设定
| `partitioningColumns` | 类似 `Hive` 的分区, 保存表的时候使用, 这个地方的分区不是 `RDD` 的分区, 而是文件的分区, 或者表的分区, 通过 `partitionBy` 设定
| `bucketColumnNames` | 类似 `Hive` 的分桶, 保存表的时候使用, 通过 `bucketBy` 设定
| `sortColumnNames` | 用于排序的列, 通过 `sortBy` 设定
|===

`mode` 指定了写入模式, 例如覆盖原数据集, 或者向原数据集合中尾部添加等

|===
| `Scala` 对象表示 | 字符串表示 | 解释

| `SaveMode.ErrorIfExists` | `"error"` | 将 `DataFrame` 保存到 `source` 时, 如果目标已经存在, 则报错
| `SaveMode.Append` | `"append"` | 将 `DataFrame` 保存到 `source` 时, 如果目标已经存在, 则添加到文件或者 `Table` 中
| `SaveMode.Overwrite` | `"overwrite"` | 将 `DataFrame` 保存到 `source` 时, 如果目标已经存在, 则使用 `DataFrame` 中的数据完全覆盖目标
| `SaveMode.Ignore` | `"ignore"` | 将 `DataFrame` 保存到 `source` 时, 如果目标已经存在, 则不会保存 `DataFrame` 数据, 并且也不修改目标数据集, 类似于 `CREATE TABLE IF NOT EXISTS`
|===

`DataFrameWriter` 也有两种使用方式, 一种是使用 `format` 配合 `save`, 还有一种是使用封装方法, 例如 `csv`, `json`, `saveAsTable` 等

```scala
val spark: SparkSession = ...

val df = spark.read
  .option("header", true)
  .csv("dataset/BeijingPM20100101_20151231.csv")

// 使用 save 保存, 使用 format 设置文件格式
df.write.format("json").save("dataset/beijingPM")

// 使用 json 保存, 因为方法是 json, 所以隐含的 format 是 json
df.write.json("dataset/beijingPM1")
```

[NOTE]

默认没有指定 `format`, 默认的 `format` 是 `Parquet`
总结

. 类似 `DataFrameReader`, `Writer` 中也有 `format`, `options`, 另外 `schema` 是包含在 `DataFrame` 中的
`DataFrameWriter` 中还有一个很重要的概念叫做 `mode`, 指定写入模式, 如果目标集合已经存在时的行为
`DataFrameWriter` 可以将数据保存到 `Hive` 表中, 所以也可以指定分区和分桶信息

### 7.3. 读写 Parquet 格式文件目标

. 理解 `Spark` 读写 `Parquet` 文件的语法
. 理解 `Spark` 读写 `Parquet` 文件的时候对于分区的处理

什么时候会用到 `Parquet` ?
  ![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/00a2a56f725d86b5c27463f109c43d8c.png)

在 `ETL` 中, `Spark` 经常扮演 `T` 的职务, 也就是进行数据清洗和数据转换.

为了能够保存比较复杂的数据, 并且保证性能和压缩率, 通常使用 `Parquet` 是一个比较不错的选择.

所以外部系统收集过来的数据, 有可能会使用 `Parquet`, 而 `Spark` 进行读取和转换的时候, 就需要支持对 `Parquet` 格式的文件的支持.

使用代码读写 `Parquet` 文件
  默认不指定 `format` 的时候, 默认就是读写 `Parquet` 格式的文件

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

val df = spark.read
  .option("header", value = true)
  .csv("dataset/911.csv")

// 保存 Parquet 文件
df.write.mode("override").save("dataset/911.parquet")

// 读取 Parquet 文件
val dfFromParquet = spark.read.parquet("dataset/911.parquet")
dfFromParquet.createOrReplaceTempView("911")

spark.sql("select * from 911 where zip > 19000 and zip < 19400").show()
```

写入 `Parquet` 的时候可以指定分区
  `Spark` 在写入文件的时候是支持分区的, 可以像 `Hive` 一样设置某个列为分区列

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

// 从 CSV 中读取内容
val dfFromParquet = spark.read.option("header", value = true).csv("dataset/BeijingPM20100101_20151231.csv")

// 保存为 Parquet 格式文件, 不指定 format 默认就是 Parquet
dfFromParquet.write.partitionBy("year", "month").save("dataset/beijing_pm")
```

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/67314102d7b36b791b04bafeb5d0d3e8.png)

[NOTE]

这个地方指的分区是类似 `Hive` 中表分区的概念, 而不是 `RDD` 分布式分区的含义

分区发现
  在读取常见文件格式的时候, `Spark` 会自动的进行分区发现, 分区自动发现的时候, 会将文件名中的分区信息当作一列. 例如 如果按照性别分区, 那么一般会生成两个文件夹 `gender=male` 和 `gender=female`, 那么在使用 `Spark` 读取的时候, 会自动发现这个分区信息, 并且当作列放入创建的 `DataFrame` 中

使用代码证明这件事可以有两个步骤, 第一步先读取某个分区的单独一个文件并打印其 `Schema` 信息, 第二步读取整个数据集所有分区并打印 `Schema` 信息, 和第一步做比较就可以确定

```scala
val spark = ...

val partDF = spark.read.load("dataset/beijing_pm/year=2010/month=1") // <1>
partDF.printSchema()
```

<1> 把分区的数据集中的某一个区单做一整个数据集读取, 没有分区信息, 自然也不会进行分区发现

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/dbb274b7fcdfd82c3a3922dfa6bfb29e.png)

```scala
val df = spark.read.load("dataset/beijing_pm") // <1>
df.printSchema()
```

<1> 此处读取的是整个数据集, 会进行分区发现, DataFrame 中会包含分去列

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/84353e6ed2cf479b82b4d2e4e2b6c3c2.png)

.`SparkSession` 中有关 `Parquet` 的配置
|===
| 配置 | 默认值 | 含义

| `spark.sql.parquet.binaryAsString` | `false` | 一些其他 `Parquet` 生产系统, 不区分字符串类型和二进制类型, 该配置告诉 `SparkSQL` 将二进制数据解释为字符串以提供与这些系统的兼容性
| `spark.sql.parquet.int96AsTimestamp` | `true` | 一些其他 `Parquet` 生产系统, 将 `Timestamp` 存为 `INT96`, 该配置告诉 `SparkSQL` 将 `INT96` 解析为 `Timestamp`
| `spark.sql.parquet.cacheMetadata` | `true` | 打开 Parquet 元数据的缓存, 可以加快查询静态数据
| `spark.sql.parquet.compression.codec` | `snappy` | 压缩方式, 可选 `uncompressed`, `snappy`, `gzip`, `lzo`
| `spark.sql.parquet.mergeSchema` | `false` | 当为 true 时, Parquet 数据源会合并从所有数据文件收集的 Schemas 和数据, 因为这个操作开销比较大, 所以默认关闭
| `spark.sql.optimizer.metadataOnly` | `true` | 如果为 `true`, 会通过原信息来生成分区列, 如果为 `false` 则就是通过扫描整个数据集来确定
|===总结

`Spark` 不指定 `format` 的时候默认就是按照 `Parquet` 的格式解析文件
`Spark` 在读取 `Parquet` 文件的时候会自动的发现 `Parquet` 的分区和分区字段
`Spark` 在写入 `Parquet` 文件的时候如果设置了分区字段, 会自动的按照分区存储

### 7.4. 读写 JSON 格式文件目标

. 理解 `JSON` 的使用场景
. 能够使用 `Spark` 读取处理 `JSON` 格式文件

什么时候会用到 `JSON` ?
  ![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/00a2a56f725d86b5c27463f109c43d8c.png)

在 `ETL` 中, `Spark` 经常扮演 `T` 的职务, 也就是进行数据清洗和数据转换.

在业务系统中, `JSON` 是一个非常常见的数据格式, 在前后端交互的时候也往往会使用 `JSON`, 所以从业务系统获取的数据很大可能性是使用 `JSON` 格式, 所以就需要 `Spark` 能够支持 JSON 格式文件的读取

读写 `JSON` 文件
  将要 `Dataset` 保存为 `JSON` 格式的文件比较简单, 是 `DataFrameWriter` 的一个常规使用

```scala
val spark: SparkSession = new sql.SparkSession.Builder()
  .appName("hello")
  .master("local[6]")
  .getOrCreate()

val dfFromParquet = spark.read.load("dataset/beijing_pm")

// 将 DataFrame 保存为 JSON 格式的文件
dfFromParquet.repartition(1)        // <1>
  .write.format("json")
  .save("dataset/beijing_pm_json")
```

<1> 如果不重新分区, 则会为 `DataFrame` 底层的 `RDD` 的每个分区生成一个文件, 为了保持只有一个输出文件, 所以重新分区

[NOTE]

保存为 `JSON` 格式的文件有一个细节需要注意, 这个 `JSON` 格式的文件中, 每一行是一个独立的 `JSON`, 但是整个文件并不只是一个 `JSON` 字符串, 所以这种文件格式很多时候被成为 `JSON Line` 文件, 有时候后缀名也会变为 `jsonl`

```json

{"day":"1","hour":"0","season":"1","year":2013,"month":3}
{"day":"1","hour":"1","season":"1","year":2013,"month":3}
{"day":"1","hour":"2","season":"1","year":2013,"month":3}
```

也可以通过 `DataFrameReader` 读取一个 `JSON Line` 文件

```scala
val spark: SparkSession = ...

val dfFromJSON = spark.read.json("dataset/beijing_pm_json")
dfFromJSON.show()
```

`JSON` 格式的文件是有结构信息的, 也就是 `JSON` 中的字段是有类型的, 例如 `"name": "zhangsan"` 这样由双引号包裹的 `Value`, 就是字符串类型, 而 `"age": 10` 这种没有双引号包裹的就是数字类型, 当然, 也可以是布尔型 `"has_wife": true`

`Spark` 读取 `JSON Line` 文件的时候, 会自动的推断类型信息

```scala
val spark: SparkSession = ...

val dfFromJSON = spark.read.json("dataset/beijing_pm_json")

dfFromJSON.printSchema()
```

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/e8a53ef37bbf6675525d1a844f8648f1.png)

`Spark` 可以从一个保存了 `JSON` 格式字符串的 `Dataset[String]` 中读取 `JSON` 信息, 转为 `DataFrame`
  这种情况其实还是比较常见的, 例如如下的流程

![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/da6f1c7f8d98691117a173e03bfdf18f.png)

假设业务系统通过 `Kafka` 将数据流转进入大数据平台, 这个时候可能需要使用 `RDD` 或者 `Dataset` 来读取其中的内容, 这个时候一条数据就是一个 `JSON` 格式的字符串, 如何将其转为 `DataFrame` 或者 `Dataset[Object]` 这样具有 `Schema` 的数据集呢? 使用如下代码就可以

```scala
val spark: SparkSession = ...

import spark.implicits._

val peopleDataset = spark.createDataset(
  """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}"""  Nil)

spark.read.json(peopleDataset).show()
```

总结

`JSON` 通常用于系统间的交互, `Spark` 经常要读取 `JSON` 格式文件, 处理, 放在另外一处
. 使用 `DataFrameReader` 和 `DataFrameWriter` 可以轻易的读取和写入 `JSON`, 并且会自动处理数据类型信息

#### 7.5. 访问 Hive导读

. 整合 `SparkSQL` 和 `Hive`, 使用 `Hive` 的 `MetaStore` 元信息库
. 使用 `SparkSQL` 查询 `Hive` 表
. 案例, 使用常见 `HiveSQL`
. 写入内容到 `Hive` 表

##### 7.5.1. SparkSQL 整合 Hive导读

. 开启 `Hive` 的 `MetaStore` 独立进程
. 整合 `SparkSQL` 和 `Hive` 的 `MetaStore`
和一个文件格式不同, `Hive` 是一个外部的数据存储和查询引擎, 所以如果 `Spark` 要访问 `Hive` 的话, 就需要先整合 `Hive`
整合什么 ?
  如果要讨论 `SparkSQL` 如何和 `Hive` 进行整合, 首要考虑的事应该是 `Hive` 有什么, 有什么就整合什么就可以

* `MetaStore`, 元数据存储
`SparkSQL` 内置的有一个 `MetaStore`, 通过嵌入式数据库 `Derby` 保存元信息, 但是对于生产环境来说, 还是应该使用 `Hive` 的 `MetaStore`, 一是更成熟, 功能更强, 二是可以使用 `Hive` 的元信息

* 查询引擎
`SparkSQL` 内置了 `HiveSQL` 的支持, 所以无需整合
为什么要开启 `Hive` 的 `MetaStore`
  `Hive` 的 `MetaStore` 是一个 `Hive` 的组件, 一个 `Hive` 提供的程序, 用以保存和访问表的元数据, 整个 `Hive` 的结构大致如下
![image](:https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190523011946.png)
由上图可知道, 其实 `Hive` 中主要的组件就三个, `HiveServer2` 负责接受外部系统的查询请求, 例如 `JDBC`, `HiveServer2` 接收到查询请求后, 交给 `Driver` 处理, `Driver` 会首先去询问 `MetaStore` 表在哪存, 后 `Driver` 程序通过 `MR` 程序来访问 `HDFS` 从而获取结果返回给查询请求者
而 `Hive` 的 `MetaStore` 对 `SparkSQL` 的意义非常重大, 如果 `SparkSQL` 可以直接访问 `Hive` 的 `MetaStore`, 则理论上可以做到和 `Hive` 一样的事情, 例如通过 `Hive` 表查询数据
而 Hive 的 MetaStore 的运行模式有三种

* 内嵌 `Derby` 数据库模式
这种模式不必说了, 自然是在测试的时候使用, 生产环境不太可能使用嵌入式数据库, 一是不稳定, 二是这个 `Derby` 是单连接的, 不支持并发

* `Local` 模式
`Local` 和 `Remote` 都是访问 `MySQL` 数据库作为存储元数据的地方, 但是 `Local` 模式的 `MetaStore` 没有独立进程, 依附于 `HiveServer2` 的进程

* `Remote` 模式
和 `Loca` 模式一样, 访问 `MySQL` 数据库存放元数据, 但是 `Remote` 的 `MetaStore` 运行在独立的进程中

我们显然要选择 `Remote` 模式, 因为要让其独立运行, 这样才能让 `SparkSQL` 一直可以访问

`Hive` 开启 `MetaStore`
`Step 1`: 修改 `hive-site.xml`

```xml



```

```shell
[root@node01 conf]# cat hive-site.xml
```

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>root</value>
  </property>
  <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>123456</value>
  </property>
  <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://node03:3306/hive?createDatabaseIfNotExist=true&amp;useSSL=false</value>
  </property>
  <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
  </property>
  <property>
      <name>hive.metastore.schema.verification</name>
      <value>false</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
 </property>
 <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>node03</value>
   </property>
  <!-- 添加以下代码 -->
     <!-- 指定 hive metastore服务请求的 uri地址 -->
    <property>
       <name>hive.metastore.uris</name>
        <value>thrift://node03:9083</value>
    </property>
     <property>
        <name>hive.metastore.client.socket.timeout</name>
        <value>3600</value>
        </property>
</configuration>

<!-- 添加以下内容 -->
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>/user/hive/warehouse</value>
</property>
<property>
  <name>hive.metastore.local</name>
  <value>false</value>
</property>

```

分发到其它两台机器上

```shell
[root@node01 conf]# scp hive-site.xml root@node02:$PWD
hive-site.xml
[root@node01 conf]# scp hive-site.xml root@node03:$PWD
hive-site.xml
```

`Step 2`: 启动 `Hive MetaStore`

```shell
[root@node03 bin]# nohup /export/servers/apache-hive-2.1.1-bin/bin/hive --service metastore 2>&1 >> /var/log.log &
[1] 19614
```

`SparkSQL` 整合 `Hive` 的 `MetaStore`
  即使不去整合 `MetaStore`, `Spark` 也有一个内置的 `MateStore`, 使用 `Derby` 嵌入式数据库保存数据, 但是这种方式不适合生产环境, 因为这种模式同一时间只能有一个 `SparkSession` 使用, 所以生产环境更推荐使用 `Hive` 的 `MetaStore`

`SparkSQL` 整合 `Hive` 的 `MetaStore` 主要思路就是要通过配置能够访问它, 并且能够使用 `HDFS` 保存 `WareHouse`, 这些配置信息一般存在于 `Hadoop` 和 `HDFS` 的配置文件中, 所以可以直接拷贝 `Hadoop` 和 `Hive` 的配置文件到 `Spark` 的配置目录

```shell
## // <1> <2> <3>
[root@node01 servers]# cd /export/servers/hadoop-2.7.5/etc/hadoop/
[root@node01 hadoop]# cp hdfs-site.xml /export/servers/spark/conf/
[root@node01 hadoop]# cp core-site.xml /export/servers/spark/conf/
[root@node01 hadoop]# cd /export/servers/apache-hive-2.1.1-bin/conf/
[root@node01 conf]# cp hive-site.xml /export/servers/spark/conf/
# 分发
[root@node01 spark]# cd /export/servers/spark/
[root@node01 spark]# scp -r conf root@node02:$PWD
[root@node01 spark]# scp -r conf root@node03:$PWD

```

<1> `Spark` 需要 `hive-site.xml` 的原因是, 要读取 `Hive` 的配置信息, 主要是元数据仓库的位置等信息
<2> `Spark` 需要 `core-site.xml` 的原因是, 要读取安全有关的配置
<3> `Spark` 需要 `hdfs-site.xml` 的原因是, 有可能需要在 `HDFS` 中放置表文件, 所以需要 `HDFS` 的配置

如果不希望通过拷贝文件的方式整合 Hive, 也可以在 SparkSession 启动的时候, 通过指定 Hive 的 MetaStore 的位置来访问, 但是更推荐整合的方式

##### 7.5.2. 访问 Hive 表导读

. 在 `Hive` 中创建表
. 使用 `SparkSQL` 访问 `Hive` 中已经存在的表
. 使用 `SparkSQL` 创建 `Hive` 表
. 使用 `SparkSQL` 修改 `Hive` 表中的数据

在 `Hive` 中创建表
  第一步, 需要先将文件上传到集群中, 使用如下命令上传到 `HDFS` 中

```shell
[root@node01 spark]# hdfs dfs -mkdir -p /dataset
[root@node01 spark]# cd /export/data/
# 上传数据到服务器中
[root@node01 data]# hdfs dfs -put studenttab10k /dataset
```

第二步, 使用 `Hive` 或者 `Beeline` 执行如下 `SQL`

```shell
[root@node03 bin]# cd /export/servers/apache-hive-2.1.1-bin/bin
[root@node03 bin]# expect expectlogin.exp
```

```sql
CREATE DATABASE IF NOT EXISTS spark_integrition;
USE spark_integrition;
CREATE EXTERNAL TABLE student
(
  name  STRING,
  age   INT,
  gpa   string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/dataset/hive';

LOAD DATA INPATH '/dataset/studenttab10k' OVERWRITE INTO TABLE student;
```

通过 `SparkSQL` 查询 `Hive` 的表
  查询 `Hive` 中的表可以直接通过 `spark.sql(...)` 来进行, 可以直接在其中访问 `Hive` 的 `MetaStore`, 前提是一定要将 `Hive` 的配置文件拷贝到 `Spark` 的 `conf` 目录

```scala
[root@node03 bin]# cd /export/servers/spark/
[root@node03 spark]#  bin/spark-shell --master local[2]
// 检查是否成功联接数据库
scala> val a=spark.sql("show databases")
a: org.apache.spark.sql.DataFrame = [databaseName: string]

scala> a.show
+-----------------+
|     databaseName|
+-----------------+
|          default|
|           myhive|
|spark_integrition|
|             test|
|             text|
+-----------------+
// 切换到数据库
scala> spark.sql("use spark_integrition")
res1: org.apache.spark.sql.DataFrame = []
// 执行slq 语句
scala> val resultDF = spark.sql("select * from student limit 10")
resultDF: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]

scala> resultDF.show()
+----------------+---+----+
|            name|age| gpa|
+----------------+---+----+
|ulysses thompson| 64|1.90|
|    katie carson| 25|3.65|
|       luke king| 65|0.73|
|  holly davidson| 57|2.43|
|     fred miller| 55|3.77|
|     holly white| 43|0.24|
|  luke steinbeck| 51|1.14|
|  nick underhill| 31|2.46|
|  holly davidson| 59|1.26|
|    calvin brown| 56|0.72|
+----------------+---+----+
```

通过 `SparkSQL` 创建 `Hive` 表
  通过 `SparkSQL` 可以直接创建 `Hive` 表, 并且使用 `LOAD DATA` 加载数据

```scala
val createTableStr =
  """
    |CREATE EXTERNAL TABLE student
    |(
    |  name  STRING,
    |  age   INT,
    |  gpa   string
    |)
    |ROW FORMAT DELIMITED
    |  FIELDS TERMINATED BY '\t'
    |  LINES TERMINATED BY '\n'
    |STORED AS TEXTFILE
    |LOCATION '/dataset/hive'
  """.stripMargin

scala> spark.sql("CREATE DATABASE IF NOT EXISTS spark_integrition1")
res3: org.apache.spark.sql.DataFrame = []

scala> spark.sql("USE spark_integrition1")
res4: org.apache.spark.sql.DataFrame = []

scala> spark.sql(createTableStr)
res5: org.apache.spark.sql.DataFrame = []

scala> spark.sql("LOAD DATA INPATH '/dataset/studenttab10k' OVERWRITE INTO TABLE student")
res6: org.apache.spark.sql.DataFrame = []
scala> spark.sql("select * from student limit 10").show()
+----------------+---+----+
|            name|age| gpa|
+----------------+---+----+
|ulysses thompson| 64|1.90|
|    katie carson| 25|3.65|
|       luke king| 65|0.73|
|  holly davidson| 57|2.43|
|     fred miller| 55|3.77|
|     holly white| 43|0.24|
|  luke steinbeck| 51|1.14|
|  nick underhill| 31|2.46|
|  holly davidson| 59|1.26|
|    calvin brown| 56|0.72|
+----------------+---+----+

```

目前 `SparkSQL` 支持的文件格式有 `sequencefile`, `rcfile`, `orc`, `parquet`, `textfile`, `avro`, 并且也可以指定 `serde` 的名称

使用 `SparkSQL` 处理数据并保存进 Hive 表
  前面都在使用 `SparkShell` 的方式来访问 `Hive`, 编写 `SQL`, 通过 `Spark` 独立应用的形式也可以做到同样的事, 但是需要一些前置的步骤, 如下

Step 1: 导入 `Maven` 依赖

```scala
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hive_2.11</artifactId>
    <version>${spark.version}</version>
</dependency>
```

Step 2: 配置 `SparkSession`
如果希望使用 `SparkSQL` 访问 `Hive` 的话, 需要做两件事

. 开启 `SparkSession` 的 `Hive` 支持
经过这一步配置, `SparkSQL` 才会把 `SQL` 语句当作 `HiveSQL` 来进行解析

. 设置 `WareHouse` 的位置
虽然 `hive-stie.xml` 中已经配置了 `WareHouse` 的位置, 但是在 `Spark 2.0.0` 后已经废弃了 `hive-site.xml` 中设置的 `hive.metastore.warehouse.dir`, 需要在 `SparkSession` 中设置 `WareHouse` 的位置

. 设置 `MetaStore` 的位置

```scala
val spark = SparkSession
  .builder()
  .appName("hive example")
  .master("local[6]")
  .config("spark.sql.warehouse.dir", "hdfs://node01:8020/dataset/hive")  // <1>
  .config("hive.metastore.uris", "thrift://node03:9083")                 // <2>
  .enableHiveSupport()                                                   // <3>
  .getOrCreate()
```

<1> 设置 `WareHouse` 的位置
<2> 设置 `MetaStore` 的位置
<3> 开启 `Hive` 支持

配置好了以后, 就可以通过 `DataFrame` 处理数据, 后将数据结果推入 `Hive` 表中了, 在将结果保存到 `Hive` 表的时候, 可以指定保存模式

```scala
val schema = StructType(
  List(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("gpa", FloatType)
  )
)

val studentDF = spark.read
  .option("delimiter", "\t")
  .schema(schema)
  .csv("dataset/studenttab10k")

val resultDF = studentDF.where("age < 50")

resultDF.write.mode(SaveMode.Overwrite).saveAsTable("spark_integrition1.student") // <1>
```

<1> 通过 `mode` 指定保存模式, 通过 `saveAsTable` 保存数据到 `Hive`

### 7.6. JDBC导读

. 通过 `SQL` 操作 `MySQL` 的表
. 将数据写入 `MySQL` 的表中

准备 `MySQL` 环境
  在使用 `SparkSQL` 访问 `MySQL` 之前, 要对 `MySQL` 进行一些操作, 例如说创建用户, 表和库等

Step 1: 连接 `MySQL` 数据库
在 `MySQL` 所在的主机上执行如下命令

```text
[root@node03 spark]# mysql -u root -p123456
mysql> show datablses;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| azkaban_two_server |
| hive               |
| hue                |
| metastore          |
| mysql              |
| oozie              |
| performance_schema |
| sys                |
| test               |
| text50             |
| userdb             |
+--------------------+
12 rows in set (0.00 sec)
```

 Step 2: 创建 `Spark` 使用的用户
登进 `MySQL` 后, 需要先创建用户

```sql
mysql> CREATE USER 'spark'@'%' IDENTIFIED BY 'Spark123!';
GRANT ALL ON spark_test.* TO 'spark'@'%';
Query OK, 0 rows affected (0.05 sec)

mysql> GRANT ALL ON spark_test.* TO 'spark'@'%';
Query OK, 0 rows affected (0.00 sec)
```

* Step 3: 创建库和表

```sql
CREATE DATABASE spark_test;
USE spark_test;
CREATE TABLE IF NOT EXISTS `student`(
`id` INT AUTO_INCREMENT,
`name` VARCHAR(100) NOT NULL,
`age` INT NOT NULL,
`gpa` FLOAT,
PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

使用 `SparkSQL` 向 `MySQL` 中写入数据
其实在使用 `SparkSQL` 访问 `MySQL` 是通过 `JDBC`, 那么其实所有支持 `JDBC` 的数据库理论上都可以通过这种方式进行访问
在使用 `JDBC` 访问关系型数据的时候, 其实也是使用 `DataFrameReader`, 对 `DataFrameReader` 提供一些配置, 就可以使用 `Spark` 访问 `JDBC`, 有如下几个配置可用

| 属性 | 含义|
|----|----|
| `url` | 要连接的 `JDBC URL`|
| `dbtable` | 要访问的表, 可以使用任何 `SQL` 语句中 `from` 子句支持的语法|
| `fetchsize` | 数据抓取的大小(单位行), 适用于读的情况|
| `batchsize` | 数据传输的大小(单位行), 适用于写的情况|
| `isolationLevel` | 事务隔离级别, 是一个枚举, 取值 `NONE`, `READ_COMMITTED`, `READ_UNCOMMITTED`, `REPEATABLE_READ`, `SERIALIZABLE`, 默认为 `READ_UNCOMMITTED`|

读取数据集, 处理过后存往 `MySQL` 中的代码如下

```scala
val spark = SparkSession
  .builder()
  .appName("hive example")
  .master("local[6]")
  .getOrCreate()

val schema = StructType(
  List(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("gpa", FloatType)
  )
)
//读取数据
val studentDF = spark.read
  .option("delimiter", "\t")
  .schema(schema)
  .csv("dataset/studenttab10k")
//往指定的数据库写数据
studentDF.write.format("jdbc").mode(SaveMode.Overwrite)
  .option("url", "jdbc:mysql://node03:3306/spark_test")
  .option("dbtable", "student")
  .option("user", "spark")
  .option("password", "Spark123!")
  .save()
```

运行程序

如果是在本地运行, 需要导入 `Maven` 依赖

```xml
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.47</version>
</dependency>
```

如果使用 `Spark submit` 或者 `Spark shell` 来运行任务, 需要通过 `--jars` 参数提交 `MySQL` 的 `Jar` 包, 或者指定 `--packages` 从 `Maven` 库中读取

```text
bin/spark-shell --packages  mysql:mysql-connector-java:5.1.47 --repositories http://maven.aliyun.com/nexus/content/groups/public/
```

从 `MySQL` 中读取数据
  读取 `MySQL` 的方式也非常的简单, 只是使用 `SparkSQL` 的 `DataFrameReader` 加上参数配置即可访问

```scala
spark.read.format("jdbc")
  .option("url", "jdbc:mysql://node01:3306/spark_test")
  .option("dbtable", "student")
  .option("user", "spark")
  .option("password", "Spark123!")
  .load()
  .show()
```

默认情况下读取 `MySQL` 表时, 从 `MySQL` 表中读取的数据放入了一个分区, 拉取后可以使用 `DataFrame` 重分区来保证并行计算和内存占用不会太高, 但是如果感觉 `MySQL` 中数据过多的时候, 读取时可能就会产生 `OOM`, 所以在数据量比较大的场景, 就需要在读取的时候就将其分发到不同的 `RDD` 分区

| 属性 | 含义|
|----|----|
| `partitionColumn` | 指定按照哪一列进行分区, 只能设置类型为数字的列, 一般指定为 `ID`|
| `lowerBound`, `upperBound` | 确定步长的参数, `lowerBound - upperBound` 之间的数据均分给每一个分区, 小于 `lowerBound` 的数据分给第一个分区, 大于 `upperBound` 的数据分给最后一个分区|
| `numPartitions` | 分区数量|

```scala
spark.read.format("jdbc")
  .option("url", "jdbc:mysql://node01:3306/spark_test")
  .option("dbtable", "student")
  .option("user", "spark")
  .option("password", "Spark123!")
  .option("partitionColumn", "age")
  .option("lowerBound", 1)
  .option("upperBound", 60)
  .option("numPartitions", 10)
  .load()
  .show()
```

有时候可能要使用非数字列来作为分区依据, `Spark` 也提供了针对任意类型的列作为分区依据的方法

```scala
val predicates = Array(
  "age < 20",
  "age >= 20, age < 30",
  "age >= 30"
)

val connectionProperties = new Properties()
connectionProperties.setProperty("user", "spark")
connectionProperties.setProperty("password", "Spark123!")

spark.read
  .jdbc(
    url = "jdbc:mysql://node01:3306/spark_test",
    table = "student",
    predicates = predicates,
    connectionProperties = connectionProperties
  )
  .show()
```

`SparkSQL` 中并没有直接提供按照 `SQL` 进行筛选读取数据的 `API` 和参数, 但是可以通过 `dbtable` 来曲线救国, `dbtable` 指定目标表的名称, 但是因为 `dbtable` 中可以编写 `SQL`, 所以使用子查询即可做到

```scala
spark.read.format("jdbc")
  .option("url", "jdbc:mysql://node01:3306/spark_test")
  .option("dbtable", "(select name, age from student where age > 10 and age < 20) as stu")
  .option("user", "spark")
  .option("password", "Spark123!")
  .option("partitionColumn", "age")
  .option("lowerBound", 1)
  .option("upperBound", 60)
  .option("numPartitions", 10)
  .load()
  .show()
```

## 示例

### spark-warehouse

#### scala

#### cn

#### xhchen

#### spark

#### rdd

##### AccessLogAgg.scala

src/main/scala/cn/xhchen/spark/rdd/AccessLogAgg.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class AccessLogAgg {

  @Test
  def ipAgg(): Unit = {
    // 1. 创建 SparkContext
    val conf = new SparkConf().setMaster("local[6]").setAppName("ip_agg")
    val sc = new SparkContext(conf)

    // 2. 读取文件, 生成数据集
    val sourceRDD = sc.textFile("dataset/access_log_sample.txt")

    // 3. 取出IP, 赋予出现次数为1
    val ipRDD = sourceRDD.map(item => (item.split(" ")(0), 1))

    // 4. 简单清洗
    //     4.1. 去掉空的数据
    //     4.2. 去掉非法的数据
    //     4.3. 根据业务再规整一下数据
    val cleanRDD = ipRDD.filter(item => StringUtils.isNotEmpty(item._1))

    // 5. 根据IP出现的次数进行聚合
    val ipAggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg )

    // 6. 根据IP出现的次数进行排序
    val sortedRDD = ipAggRDD.sortBy(item => item._2, ascending = false)

    // 7. 取出结果, 打印结果
    sortedRDD.take(10).foreach(item => println(item))
  }

}
```

##### ActionOp.scala

src/main/scala/cn/xhchen/spark/rdd/ActionOp.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class ActionOp {
  val conf = new SparkConf().setMaster("local[6]").setAppName("transformation_op")
  val sc = new SparkContext(conf)

  /**
    * 需求, 最终生成 ("结果", price)
    *
    * 注意点:
    * 1. 函数中传入的 curr 参数, 并不是 Value, 而是一整条数据
    * 2. reduce 整体上的结果, 只有一个
    */
  @Test
  def reduce(): Unit = {
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    val result: (String, Double) = rdd.reduce((curr, agg) => ("总价", curr._2 + agg._2) )
    println(result)
  }

  @Test
  def foreach(): Unit = {
    val rdd = sc.parallelize(Seq(1, 2, 3))
    rdd.foreach(item => println(item))
  }

  /**
    * count 和 countByKey 的结果相距很远很远, 每次调用 Action 都会生成一个 job, job 会运行获取结果
    * 所以在两个 job 中间有大量的 Log 打出, 其实就是在启动 job
    *
    * countByKey 的运算结果是 Map(Key, Value -> Key 的count)
    *
    * 数据倾斜, 如果要解决数据倾斜的问题, 是不是要先知道谁倾斜, 通过 countByKey 是不是可以查看 Key 对应的数据总数, 从而解决倾斜问题
    */
  @Test
  def count(): Unit = {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("c", 3), ("c", 4)))
    println(rdd.count())
    println(rdd.countByKey())
  }

  /**
    * take 和 takeSample 都是获取数据, 一个是直接获取, 一个是采样获取
    * first: 一般情况下, action 会从所有分区获取数据, 相对来说速度就比较慢, first 只是获取第一个元素, 所以first 只会处理第一个分区, 所以速度很快, 无序处理所有数据
    */
  @Test
  def take(): Unit = {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
    rdd.take(3).foreach(item => println(item))
    println(rdd.first())
    rdd.takeSample(withReplacement = false, num = 3).foreach(item => println(item))
  }

  /**
    * 除了这四个支持以外, 还有其它很多特殊的支持
    * 这些对于数字类型的支持, 都是Action
    */
  @Test
  def numberic(): Unit = {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 10, 20, 30, 50, 100))
    println(rdd.max()) // 100
    println(rdd.min()) // 1
    println(rdd.mean()) // ...
    println(rdd.sum())
  }

}
```

##### CacheOp.scala

src/main/scala/cn/xhchen/spark/rdd/CacheOp.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class CacheOp {

  /**
    * 1. 创建sc
    * 2. 读取文件
    * 3. 取出IP, 赋予初始频率
    * 4. 清洗
    * 5. 统计IP出现的次数
    * 6. 统计出现次数最少的IP
    * 7. 统计出现次数最多的IP
    */
  @Test
  def prepare(): Unit = {
    // 1. 创建 SC
    val conf = new SparkConf().setAppName("cache_prepare").setMaster("local[6]")
    val sc = new SparkContext(conf)

    // 2. 读取文件
    val source = sc.textFile("dataset/access_log_sample.txt")

    // 3. 取出IP, 赋予初始频率
    val countRDD = source.map( item => (item.split(" ")(0), 1) )

    // 4. 数据清洗
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )

    // 5. 统计IP出现的次数(聚合)
    val aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg )

    // 6. 统计出现次数最少的IP(得出结论)
    val lessIp = aggRDD.sortBy(item => item._2, ascending = true).first()

    // 7. 统计出现次数最多的IP(得出结论)
    val moreIp = aggRDD.sortBy(item => item._2, ascending = false).first()

    println((lessIp, moreIp))
  }
}
```

##### SourceAnalysis.scala

src/main/scala/cn/xhchen/spark/rdd/SourceAnalysis.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class SourceAnalysis {

  @Test
  def wordCount(): Unit = {
    // 1. 创建 sc 对象
    val conf = new SparkConf().setMaster("local[6]").setAppName("wordCount_source")
    val sc = new SparkContext(conf)

    // 2. 创建数据集
    // textFile 算子作用是创建一个 HadoopRDD
    val textRDD = sc.textFile("...")

    // 3. 数据处理
    //     1. 拆词
    val splitRDD = textRDD.flatMap( _.split(" ") )
    //     2. 赋予初始词频
    val tupleRDD = splitRDD.map( item => (item, 1) )
    //     3. 聚合统计词频
    val reduceRDD = tupleRDD.reduceByKey( _ + _ )
    //     4. 将结果转为字符串
    val strRDD = reduceRDD.map( item => s"${item._1}, ${item._2}" )

    // 4. 结果获取
//    strRDD.collect().foreach( println(_) )
    println(strRDD.toDebugString)

    // 5. 关闭sc, 执行
    sc.stop()
  }

  @Test
  def narrowDependency(): Unit = {
    // 需求: 求得两个 RDD 之间的笛卡尔积

    // 1. 生成 RDD
    val conf = new SparkConf().setMaster("local[6]").setAppName("cartesian")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
    val rdd2 = sc.parallelize(Seq("a", "b", "c"))

    // 2. 计算
    val resultRDD = rdd1.cartesian(rdd2)

    // 3. 结果获取
    resultRDD.collect().foreach(println(_))

    sc.stop()
  }
}
```

##### StagePractice.scala

src/main/scala/cn/xhchen/spark/rdd/StagePractice.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class StagePractice {

  @Test
  def pmProcess(): Unit = {
    // 1. 创建sc对象
    val conf = new SparkConf().setMaster("local[6]").setAppName("stage_practice")
    val sc = new SparkContext(conf)

    // 2. 读取文件
    val source = sc.textFile("dataset/BeijingPM20100101_20151231_noheader.csv")

    // 3. 通过算子处理数据
    //    1. 抽取数据, 年, 月, PM, 返回结果: ((年, 月), PM)
    //    2. 清洗, 过滤掉空的字符串, 过滤掉 NA
    //    3. 聚合
    //    4. 排序
    val resultRDD = source.map( item => ((item.split(",")(1), item.split(",")(2)), item.split(",")(6)) )
      .filter( item => StringUtils.isNotEmpty(item._2) && ! item._2.equalsIgnoreCase("NA") )
      .map( item => (item._1, item._2.toInt) )
      .reduceByKey( (curr, agg) => curr + agg )
      .sortBy( item => item._2, ascending = false)

    // 4. 获取结果
    resultRDD.take(10).foreach(item => println(item))

    // 5. 运行测试
    sc.stop()
  }
}
```

##### TransformationOp.scala

src/main/scala/cn/xhchen/spark/rdd/TransformationOp.scala

```scala
package cn.xhchen.spark.rdd

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class TransformationOp {
  val conf = new SparkConf().setMaster("local[6]").setAppName("transformation_op")
  val sc = new SparkContext(conf)

  /**
    * mapPartitions 和 map 算子是一样的, 只不过 map 是针对每一条数据进行转换, mapPartitions 针对一整个分区的数据进行转换
    * 所以:
    * 1. map 的 func 参数是单条数据, mapPartitions 的 func 参数是一个集合(一个分区整个所有的数据)
    * 2. map 的 func 返回值也是单条数据, mapPartitions 的 func 返回值是一个集合
    */
  @Test
  def mapPartitions(): Unit = {
    // 1. 数据生成
    // 2. 算子使用
    // 3. 获取结果
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
      .mapPartitions(iter => {
        iter.foreach(item => println(item))
        iter
      })
      .collect()
  }

  @Test
  def mapPartitions2(): Unit = {
    // 1. 数据生成
    // 2. 算子使用
    // 3. 获取结果
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
      .mapPartitions(iter => {
        // 遍历 iter 其中每一条数据进行转换, 转换完成以后, 返回这个 iter
        // iter 是 scala 中的集合类型
        iter.map(item => item * 10)
      })
      .collect()
      .foreach(item => println(item))
  }

  /**
    * mapPartitionsWithIndex 和 mapPartitions 的区别是 func 中多了一个参数, 是分区号
    */
  @Test
  def mapPartitionsWithIndex(): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
      .mapPartitionsWithIndex( (index, iter) => {
        println("index: " + index)
        iter.foreach(item => println(item))
        iter
      } )
      .collect()
  }

  /**
    * filter 可以过滤掉数据集中一部分元素
    * filter 中接收的函数, 参数是 每一个元素, 如果这个函数返回 ture, 当前元素就会被加入新数据集, 如果返回 flase, 当前元素会被过滤掉
    */
  @Test
  def filter(): Unit = {
    // 1. 定义集合
    // 2. 过滤数据
    // 3. 收集结果
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .filter( item => item % 2 == 0 )
      .collect()
      .foreach(item => println(item))
  }

  /**
    * 作用: 把大数据集变小, 尽可能的减少数据集规律的损失
    * withReplacement: 指定为True的情况下, 可能重复, 如果是Flase, 无重复
    */
  @Test
  def sample(): Unit = {
    // 1. 定义集合
    // 2. 过滤数据
    // 3. 收集结果
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rdd2 = rdd1.sample(true, 0.6)
    val result = rdd2.collect()
    result.foreach(item => println(item))
  }

  /**
    * mapValue 也是 map, 只不过map作用于整条数据, mapValue 作用于 Value
    */
  @Test
  def mapValues(): Unit = {
    sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      .mapValues( item => item * 10 )
      .collect()
      .foreach(println(_))
  }

  /**
    * 交集
    */
  @Test
  def intersection(): Unit = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.intersection(rdd2)
      .collect()
      .foreach(println(_))
  }

  /**
    * 并集
    */
  @Test
  def union(): Unit = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.union(rdd2)
      .collect()
      .foreach(println(_))
  }

  /**
    * 差集
    */
  @Test
  def subtract(): Unit = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.subtract(rdd2)
      .collect()
      .foreach(println(_))
  }

  /**
    * groupByKey 运算结果的格式: (K, (value1, value2))
    * reduceByKey 能不能在 Map 端做 Combiner: 1. 能不能减少 IO
    * groupByKey 在 map 端做 Combiner 有没有意义? 没有的...
    */
  @Test
  def groupByKey(): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 1), ("a", 1), ("b", 1)))
    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val rdd2: Array[(String, Iterable[Int])] = rdd1.collect()
    rdd2.foreach(println(_))
  }

  /**
    * CombineByKey 这个算子中接收三个参数:
    * 转换数据的函数(初始函数, 作用于第一条数据, 用于开启整个计算), 在分区上进行聚合, 把所有分区的聚合结果聚合为最终结果
    */
  @Test
  def combineByKey(): Unit = {
    // 1. 准备集合
    val rdd: RDD[(String, Double)] = sc.parallelize(Seq(
      ("zhangsan", 99.0),
      ("zhangsan", 96.0),
      ("lisi", 97.0),
      ("lisi", 98.0),
      ("zhangsan", 97.0))
    )

    // 2. 算子操作
    //   2.1. createCombiner 转换数据
    //   2.2. mergeValue 分区上的聚合
    //   2.3. mergeCombiners 把所有分区上的结果再次聚合, 生成最终结果
    val combineResult = rdd.combineByKey(
      createCombiner = (curr: Double) => (curr, 1),
      mergeValue = (curr: (Double, Int), nextValue: Double) => (curr._1 + nextValue, curr._2 + 1),
      mergeCombiners = (curr: (Double, Int), agg: (Double, Int)) => (curr._1 + agg._1, curr._2 + agg._2)
    )
    // ("zhangsan", (99 + 96 + 97, 3))
    val resultRDD = combineResult.map( item => (item._1, item._2._1 / item._2._2) )

    // 3. 获取结果, 打印结果
    resultRDD.collect().foreach(println(_))
  }

  /**
    * foldByKey 和 Spark 中的 reduceByKey 的区别是可以指定初始值
    * foldByKey 和 Scala 中的 foldLeft 或者 foldRight 区别是, 这个初始值作用于每一个数据
    */
  @Test
  def foldByKey(): Unit = {
    sc.parallelize(Seq(("a", 1), ("a", 1), ("b", 1)))
      .foldByKey(10)((curr, agg) => curr + agg)
      .collect()
      .foreach(println(_))
  }

  /**
    * aggregateByKey(zeroValue)(seqOp, combOp)
    * zeroValue : 指定初始值
    * seqOp : 作用于每一个元素, 根据初始值, 进行计算
    * combOp : 将 seqOp 处理过的结果进行聚合
    *
    * aggregateByKey 特别适合针对每个数据要先处理, 后聚合
    */
  @Test
  def aggregateByKey(): Unit = {
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    rdd.aggregateByKey(0.8)((zeroValue, item) => item * zeroValue, (curr, agg) => curr + agg)
      .collect()
      .foreach(println(_))
  }

  @Test
  def join(): Unit = {
    val rdd1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1)))
    val rdd2 = sc.parallelize(Seq(("a", 10), ("a", 11), ("a", 12)))
    rdd1.join(rdd2)
      .collect()
      .foreach(println(_))
  }

  /**
    * sortBy 可以作用于任何类型数据的RDD, sortByKey 只有 KV 类型数据的RDD中才有
    * sortBy 可以按照任何部分来排序, sortByKey 只能按照 Key 来排序
    * sortByKey 写法简单, 不用编写函数了
    */
  @Test
  def sort(): Unit = {
    val rdd1 = sc.parallelize(Seq(2, 4, 1, 5, 1, 8))
    val rdd2 = sc.parallelize(Seq(("a", 1), ("b", 3), ("c", 2)))

    rdd1.sortBy(item => item)

    rdd2.sortBy(item => item._2)
    rdd2.sortByKey()
    rdd2.map(item => (item._2, item._1)).sortByKey().map(item => (item._2, item._1)).collect().foreach(println(_))
  }

  /**
    * repartition 进行重分区的时候, 默认是 Shuffle 的
    * coalesce 进行重分区的时候, 默认是不 Shuffle 的, coalesce 默认不能增大分区数
    */
  @Test
  def partitioning(): Unit = {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 2)

    // repartition
//    println(rdd.repartition(5).partitions.size)
//    println(rdd.repartition(1).partitions.size)

    // coalesce
    println(rdd.coalesce(5, shuffle = true).partitions.size)
  }
}
```

##### WordCount.scala

src/main/scala/cn/xhchen/spark/rdd/WordCount.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class WordCount {

  def main(args: Array[String]): Unit = {
    // 1. 创建SparkContext
    val conf = new SparkConf().setAppName("word_count")
    val sc = new SparkContext(conf)

    // 2. 加载文件
    //     1. 准备文件
    //     2. 读取文件

    // RDD 特点:
    // 1. RDD是数据集
    // 2. RDD是编程模型
    // 3. RDD相互之间有依赖关系
    // 4. RDD是可以分区的
    val rdd1: RDD[String] = sc.textFile("hdfs:///data/wordcount.txt")

    // 3. 处理
    //     1. 把整句话拆分为多个单词
    val rdd2: RDD[String] = rdd1.flatMap(item => item.split(" ") )
    //     2. 把每个单词指定一个词频1
    val rdd3: RDD[(String, Int)] = rdd2.map(item => (item, 1) )
    //     3. 聚合
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((curr, agg) => curr + agg )

    // 4. 得到结果
    val result: Array[(String, Int)] = rdd4.collect()
    result.foreach(item => println(item))
  }

  @Test
  def sparkContext(): Unit = {
    // 1. Spark Context 如何编写
    //     1. 创建 SparkConf
    val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
    //     2. 创建 SparkContext
    val sc = new SparkContext(conf)

    // SparkContext身为大入口API, 应该能够创建 RDD, 并且设置参数, 设置Jar包...
//    sc....

    // 2. 关闭 SparkContext, 释放集群资源
  }

  val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
  val sc = new SparkContext(conf)

  // 从本地集合创建
  @Test
  def rddCreationLocal(): Unit = {
    val seq = Seq(1, 2, 3)
    val rdd1: RDD[Int] = sc.parallelize(seq, 2)
    sc.parallelize(seq)
    val rdd2: RDD[Int] = sc.makeRDD(seq, 2)
  }

  // 从文件创建
  @Test
  def rddCreationFiles(): Unit = {
    sc.textFile("file:///...")

    // 1. textFile 传入的是什么
    //    * 传入的是一个 路径, 读取路径
    //    * hdfs://  file://   /.../...(这种方式分为在集群中执行还是在本地执行, 如果在集群中, 读的是hdfs, 本地读的是文件系统)
    // 2. 是否支持分区?
    //    * 假如传入的path是 hdfs:///....
    //    * 分区是由HDFS中文件的block决定的
    // 3. 支持什么平台
    //    * 支持aws和阿里云
  }

  // 从RDD衍生
  @Test
  def rddCreateFromRDD(): Unit = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3))
    // 通过在rdd上执行算子操作, 会生成新的 rdd
    // 原地计算
    // str.substr 返回新的字符串, 非原地计算
    // 和字符串中的方式很像, 字符串是可变的吗?
    // RDD可变吗?不可变
    val rdd2: RDD[Int] = rdd1.map(item => item)
  }

  @Test
  def mapTest(): Unit = {
    // 1. 创建 RDD
    val rdd1 = sc.parallelize(Seq(1, 2, 3))
    // 2. 执行 map 操作
    val rdd2 = rdd1.map( item => item * 10 )
    // 3. 得到结果
    val result: Array[Int] = rdd2.collect()
    result.foreach(item => println(item))
  }

  @Test
  def flatMapTest(): Unit = {
    // 1. 创建 RDD
    val rdd1 = sc.parallelize(Seq("Hello lily", "Hello lucy", "Hello tim"))
    // 2. 处理数据
    val rdd2 = rdd1.flatMap( item => item.split(" ") )
    // 3. 得到结果
    val result = rdd2.collect()
    result.foreach(item => println(item))
    // 4. 关闭sc
    sc.stop()
  }

  @Test
  def reduceByKeyTest(): Unit = {
    // 1. 创建 RDD
    val rdd1 = sc.parallelize(Seq("Hello lily", "Hello lucy", "Hello tim"))
    // 2. 处理数据
    val rdd2 = rdd1.flatMap( item => item.split(" ") )
      .map( item => (item, 1) )
      .reduceByKey( (curr, agg) => curr + agg )
    // 3. 得到结果
    val result = rdd2.collect()
    result.foreach(item => println(item))
    // 4. 关闭sc
    sc.stop()
  }

}
```

#### sql

##### HiveAccess.scala

src/main/scala/cn/xhchen/spark/sql/HiveAccess.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object HiveAccess {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hive access1")
      .enableHiveSupport()
       .master("local[6]")// 需要配置本地运行属行，不然会报错
      .config("hive.metastore.uris", "thrift://node03:9083")
      .config("spark.sql.warehouse.dir", "/dataset/hive")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据
    //    1. 上传 HDFS, 因为要在集群中执行, 没办法保证程序在哪个机器中执行
    //        所以, 要把文件上传到所有的机器中, 才能读取本地文件
    //        上传到 HDFS 中就可以解决这个问题, 所有的机器都可以读取 HDFS 中的文件
    //        它是一个外部系统
    //    2. 使用 DF 读取数据

    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val dataframe = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv("hdfs://node01:8020/dataset/hive/studenttab10k")//路径注意

    val resultDF = dataframe.where('age > 50)

    // 3. 写入数据, 使用写入表的 API, saveAsTable
    resultDF.write.mode(SaveMode.Overwrite).saveAsTable("spark_integrition1.student")
  }
}

//A master URL must be set in your configuration 如果报这个错。
//解决问题
//解决办法：在IDE中点击Run -> Edit Configuration，在右侧VM options中输入“-Dspark.master=local”，指示本程序本地单线程运行
```

##### Intro.scala

src/main/scala/cn/xhchen/spark/sql/Intro.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.junit.Test

class Intro {

  @Test
  def rddIntro(): Unit = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("rdd intro")
    val sc = new SparkContext(conf)

    sc.textFile("dataset/wordcount.txt")
      .flatMap( _.split(" ") )
      .map( (_, 1) )
      .reduceByKey( _ + _ )
      .collect()
      .foreach( println(_) )
  }

  @Test
  def dsIntro(): Unit = {
    val spark = new SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))

    val personDS: Dataset[Person] = sourceRDD.toDS()

    val resultDS = personDS.where( 'age > 10 )
      .where( 'age < 20 )
      .select( 'name )
      .as[String]

    resultDS.show()
  }

  @Test
  def dfIntro(): Unit = {
    val spark = new SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))

    val df = sourceRDD.toDF()
    df.createOrReplaceTempView("person")

    val resultDF = spark.sql("select name from person where age > 10 and age < 20")

    resultDF.show()
  }

  @Test
  def dataset1(): Unit = {
    // 1. 创建 SparkSession
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 2. 导入隐式转换
    import spark.implicits._

    // 3. 演示
    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val dataset = sourceRDD.toDS()

    // Dataset 支持强类型的 API
    dataset.filter( item => item.age > 10 ).show()
    // Dataset 支持弱类型 API
    dataset.filter( 'age > 10 ).show()
    dataset.filter( $"age" > 10 ).show()
    // Dataset 可以直接编写 SQL 表达式
    dataset.filter("age > 10").show()
  }

  @Test
  def dataset2(): Unit = {
    // 1. 创建 SparkSession
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 2. 导入隐式转换
    import spark.implicits._

    // 3. 演示
    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val dataset = sourceRDD.toDS()

//    dataset.explain(true)
    // 无论Dataset中放置的是什么类型的对象, 最终执行计划中的RDD上都是 InternalRow
    val executionRdd: RDD[InternalRow] = dataset.queryExecution.toRdd
  }

  @Test
  def dataset3(): Unit = {
    // 1. 创建 SparkSession
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 2. 导入隐式转换
    import spark.implicits._

    // 3. 演示
//    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
//    val dataset = sourceRDD.toDS()
    val dataset: Dataset[Person] = spark.createDataset(Seq(Person("zhangsan", 10), Person("lisi", 15)))

    //    dataset.explain(true)
    // 无论Dataset中放置的是什么类型的对象, 最终执行计划中的RDD上都是 InternalRow
    // 直接获取到已经分析和解析过的 Dataset 的执行计划, 从中拿到 RDD
    val executionRdd: RDD[InternalRow] = dataset.queryExecution.toRdd

    // 通过将 Dataset 底层的 RDD[InternalRow] 通过 Decoder 转成了和 Dataset 一样的类型的 RDD
    val typedRdd: RDD[Person] = dataset.rdd

    println(executionRdd.toDebugString)
    println()
    println()
    println(typedRdd.toDebugString)
  }

  @Test
  def dataframe1(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("dataframe1")
      .master("local[6]")
      .getOrCreate()

    // 2. 创建 DataFrame
    import spark.implicits._

    val dataFrame: DataFrame = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDF()

    // 3. 看看 DataFrame 可以玩出什么花样
    // select name from ... t where t.age > 10
    dataFrame.where('age > 10)
      .select('name)
      .show()
  }

  @Test
  def dataframe2(): Unit = {
    val spark = SparkSession.builder()
      .appName("dataframe1")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val personList = Seq(Person("zhangsan", 15), Person("lisi", 20))

    // 1. toDF
    val df1 = personList.toDF()
    val df2 = spark.sparkContext.parallelize(personList).toDF()

    // 2. createDataFrame
    val df3 = spark.createDataFrame(personList)

    // 3. read
    val df4 = spark.read.csv("dataset/BeijingPM20100101_20151231_noheader.csv")
    df4.show()
  }

  @Test
  def dataframe3(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("pm analysis")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据集
    val sourceDF: DataFrame = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    // 查看 DataFrame 的 Schema 信息, 要意识到 DataFrame 中是有结构信息的, 叫做 Schema
    sourceDF.printSchema()

    // 3. 处理
    //     1. 选择列
    //     2. 过滤掉 NA 的 PM 记录
    //     3. 分组 select year, month, count(PM_Dongsi) from ... where PM_Dongsi != NA group by year, month
    //     4. 聚合
    // 4. 得出结论
//    sourceDF.select('year, 'month, 'PM_Dongsi)
//      .where('PM_Dongsi =!= "NA")
//      .groupBy('year, 'month)
//      .count()
//      .show()

    // 是否能直接使用 SQL 语句进行查询
    // 1. 将 DataFrame 注册为临表
    sourceDF.createOrReplaceTempView("pm")

    // 2. 执行查询
    val resultDF = spark.sql("select year, month, count(PM_Dongsi) from pm where PM_Dongsi != 'NA' group by year, month")

    resultDF.show()

    spark.stop()
  }

  @Test
  def dataframe4(): Unit = {
    val spark = SparkSession.builder()
      .appName("dataframe1")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val personList = Seq(Person("zhangsan", 15), Person("lisi", 20))

    // DataFrame 是弱类型的
    val df: DataFrame = personList.toDF()
    df.map( (row: Row) => Row(row.get(0), row.getAs[Int](1) * 2) )(RowEncoder.apply(df.schema))
      .show()

    // DataFrame 所代表的弱类型操作是编译时不安全
//    df.groupBy("name, school")

    // Dataset 是强类型的
    val ds: Dataset[Person] = personList.toDS()
    ds.map( (person: Person) => Person(person.name, person.age * 2) )
      .show()

    // Dataset 所代表的操作, 是类型安全的, 编译时安全的
//    ds.filter( person => person.school )
  }

  @Test
  def row(): Unit = {
    // 1. Row 如何创建, 它是什么
    // row 对象必须配合 Schema 对象才会有 列名
    val p = Person("zhangsan", 15)
    val row = Row("zhangsan", 15)

    // 2. 如何从 Row 中获取数据
    row.getString(0)
    row.getInt(1)

    // 3. Row 也是样例类
    row match {
      case Row(name, age) => println(name, age)
    }

  }

}

case class Person(name: String, age: Int)
```

##### MySQLWrite.scala

src/main/scala/cn/xhchen/spark/sql/MySQLWrite.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

/**
  * MySQL 的访问方式有两种: 使用本地运行, 提交到集群中运行
  *
  * 写入 MySQL 数据时, 使用本地运行, 读取的时候使用集群运行
  */
object MySQLWrite {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("mysql write")
      .getOrCreate()

    // 2. 读取数据创建 DataFrame
    //    1. 拷贝文件
    //    2. 读取
    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val df = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv("dataset/studenttab10k")

    // 3. 处理数据
    val resultDF = df.where("age < 30")

    // 4. 落地数据
    resultDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://node01:3306/spark02")
      .option("dbtable", "student")
      .option("user", "spark03")
      .option("password", "Spark03!")
      .mode(SaveMode.Overwrite)
      .save()
  }

}
```

##### ReadWrite.scala

src/main/scala/cn/xhchen/spark/sql/ReadWrite.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.{DataFrameReader, SaveMode, SparkSession}
import org.junit.Test

class ReadWrite {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("reader1")
    .getOrCreate()

  @Test
  def reader1(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 2. 框架在哪
    val reader: DataFrameReader = spark.read
  }

  /**
    * 初体验 Reader
    */
  @Test
  def reader2(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 2. 第一种形式
    spark.read
      .format("csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load("dataset/BeijingPM20100101_20151231.csv")
      .show(10)

    // 3. 第二种形式
    spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")
      .show()
  }

  @Test
  def writer1(): Unit = {
    // 2. 读取数据集
    val df = spark.read.option("header", true).csv("dataset/BeijingPM20100101_20151231.csv")

    // 3. 写入数据集
    df.write.json("dataset/beijing_pm.json")

    df.write.format("json").save("dataset/beijing_pm2.json")
  }

  @Test
  def parquet(): Unit = {
    // 1. 读取 CSV 文件的数据
    val df = spark.read.option("header", true).csv("dataset/BeijingPM20100101_20151231.csv")

    // 2. 把数据写为 Parquet 格式
    // 写入的时候, 默认格式就是 parquet
    // 写入模式, 报错, 覆盖, 追加, 忽略
    df.write
      .mode(SaveMode.Overwrite)
      .save("dataset/beijing_pm3")

    // 3. 读取 Parquet 格式文件
    // 默认格式是否是 paruet? 是
    // 是否可能读取文件夹呢? 是
    spark.read
      .load("dataset/beijing_pm3")
      .show()
  }

  /**
    * 表分区的概念不仅在 parquet 上有, 其它格式的文件也可以指定表分区
    */
  @Test
  def parquetPartitions(): Unit = {
    // 1. 读取数据
//    val df = spark.read
//      .option("header", value = true)
//      .csv("dataset/BeijingPM20100101_20151231.csv")

    // 2. 写文件, 表分区
//    df.write
//      .partitionBy("year", "month")
//      .save("dataset/beijing_pm4")

    // 3. 读文件, 自动发现分区
    // 写分区表的时候, 分区列不会包含在生成的文件中
    // 直接通过文件来进行读取的话, 分区信息会丢失
    // spark sql 会进行自动的分区发现
    spark.read
      .parquet("dataset/beijing_pm4")
      .printSchema()
  }

  @Test
  def json(): Unit = {
    val df = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

//    df.write
//      .json("dataset/beijing_pm5.json")

    spark.read
      .json("dataset/beijing_pm5.json")
      .show()
  }

  /**
    * toJSON 的场景:
    * 处理完了以后, DataFrame中如果是一个对象, 如果其他的系统只支持 JSON 格式的数据
    * SParkSQL 如果和这种系统进行整合的时候, 就需要进行转换
    */
  @Test
  def json1(): Unit = {
    val df = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    df.toJSON.show()
  }

  /**
    * 从消息队列中取出JSON格式的数据, 需要使用 SparkSQL 进行处理
    */
  @Test
  def json2(): Unit = {
    val df = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    val jsonRDD = df.toJSON.rdd

    spark.read.json(jsonRDD).show()
  }
}
```

##### dependency-reduced-pom.xml

dependency-reduced-pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>cn.xhchen</groupId>
  <artifactId>spark</artifactId>
  <version>0.1.0</version>
  <build>
    <sourceDirectory>src/main/scala</sourceDirectory>
    <testSourceDirectory>src/test/scala</testSourceDirectory>
    <plugins>
      <plugin>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.0</version>
        <configuration>
          <source>1.8</source>
          <target>1.8</target>
          <encoding>UTF-8</encoding>
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
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.1.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <filters>
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                  </excludes>
                </filter>
              </filters>
              <transformers>
                <transformer>
                  <mainClass />
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
  <dependencies>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.10</version>
      <scope>provided</scope>
      <exclusions>
        <exclusion>
          <artifactId>hamcrest-core</artifactId>
          <groupId>org.hamcrest</groupId>
        </exclusion>
      </exclusions>
    </dependency>
  </dependencies>
  <properties>
    <slf4j.version>1.7.16</slf4j.version>
    <scala.version>2.11.8</scala.version>
    <log4j.version>1.2.17</log4j.version>
    <spark.version>2.2.0</spark.version>
  </properties>
</project>
```

##### pom.xml

pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>cn.xhchen</groupId>
    <artifactId>spark</artifactId>
    <version>0.1.0</version>

    <properties>
        <scala.version>2.11.8</scala.version>
        <spark.version>2.2.0</spark.version>
        <slf4j.version>1.7.16</slf4j.version>
        <log4j.version>1.2.17</log4j.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.11</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.47</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>jcl-over-slf4j</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>${log4j.version}</version>
        </dependency>

        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.10</version>
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
                <version>3.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
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
                <version>3.1.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass></mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
