---
title: 11_Structured Streaming.md
date: 2019/9/5 08:16:25
updated: 2019/9/5 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - Spark
---

Structured Streaming

全天目标:
. 回顾和展望
. 入门案例
. `Stuctured Streaming` 的体系和结构

#### 1. 回顾和展望

本章目标:

`Structured Streaming` 是 `Spark Streaming` 的进化版, 如果了解了 `Spark` 的各方面的进化过程, 有助于理解 `Structured Streaming` 的使命和作用

本章过程:
. `Spark` 的 `API` 进化过程
. `Spark` 的序列化进化过程
. `Spark Streaming` 和 `Structured Streaming`

##### 1.1. Spark 编程模型的进化过程

目标和过程
目标:
`Spark` 的进化过程中, 一个非常重要的组成部分就是编程模型的进化, 通过编程模型可以看得出来内在的问题和解决方案
过程:
. 编程模型 `RDD` 的优点和缺陷
. 编程模型 `DataFrame` 的优点和缺陷
. 编程模型 `Dataset` 的优点和缺陷
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190625103618.png)

| 编程模型 | 解释

| `RDD` a|

```scala
rdd.flatMap(_.split(" "))
   .map((_, 1))
   .reduceByKey(_ + _)
   .collect
```

* 针对自定义数据对象进行处理, 可以处理任意类型的对象, 比较符合面向对象
* `RDD` 无法感知到数据的结构, 无法针对数据结构进行编程

| `DataFrame` a|

```scala
spark.read
     .csv("...")
     .where($"name" =!= "")
     .groupBy($"name")
     .show()
```

* `DataFrame` 保留有数据的元信息, `API` 针对数据的结构进行处理, 例如说可以根据数据的某一列进行排序或者分组
* `DataFrame` 在执行的时候会经过 `Catalyst` 进行优化, 并且序列化更加高效, 性能会更好
* `DataFrame` 只能处理结构化的数据, 无法处理非结构化的数据, 因为 `DataFrame` 的内部使用 `Row` 对象保存数据
* `Spark` 为 `DataFrame` 设计了新的数据读写框架, 更加强大, 支持的数据源众多

| `Dataset` a|

```scala
spark.read
     .csv("...")
     .as[Person]
     .where(_.name != "")
     .groupByKey(_.name)
     .count()
     .show()
```

* `Dataset` 结合了 `RDD` 和 `DataFrame` 的特点, 从 `API` 上即可以处理结构化数据, 也可以处理非结构化数据
* `Dataset` 和 `DataFrame` 其实是一个东西, 所以 `DataFrame` 的性能优势, 在 `Dataset` 上也有

总结
`RDD` 的优点:
. 面向对象的操作方式
. 可以处理任何类型的数据
`RDD` 的缺点:
. 运行速度比较慢, 执行过程没有优化
. `API` 比较僵硬, 对结构化数据的访问和操作没有优化
`DataFrame` 的优点:
. 针对结构化数据高度优化, 可以通过列名访问和转换数据
. 增加 `Catalyst` 优化器, 执行过程是优化的, 避免了因为开发者的原因影响效率
`DataFrame` 的缺点:
. 只能操作结构化数据
. 只有无类型的 `API`, 也就是只能针对列和 `SQL` 操作数据, `API` 依然僵硬
`Dataset` 的优点:
. 结合了 `RDD` 和 `DataFrame` 的 `API`, 既可以操作结构化数据, 也可以操作非结构化数据
. 既有有类型的 `API` 也有无类型的 `API`, 灵活选择

##### 1.2. Spark 的 序列化 的进化过程

目标和过程
目标:
`Spark` 中的序列化过程决定了数据如何存储, 是性能优化一个非常重要的着眼点, `Spark` 的进化并不只是针对编程模型提供的 `API`, 在大数据处理中, 也必须要考虑性能
过程:
. 序列化和反序列化是什么
. `Spark` 中什么地方用到序列化和反序列化
. `RDD` 的序列化和反序列化如何实现
. `Dataset` 的序列化和反序列化如何实现
Step 1: 什么是序列化和序列化:
在 `Java` 中, 序列化的代码大概如下

```java
public class JavaSerializable implements Serializable {
  NonSerializable ns = new NonSerializable();
}

public class NonSerializable {

}

public static void main(String[] args) throws IOException {
  // 序列化
  JavaSerializable serializable = new JavaSerializable();
  ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("/tmp/obj.ser"));
  // 这里会抛出一个 "java.io.NotSerializableException: cn.xhchen.NonSerializable" 异常
  objectOutputStream.writeObject(serializable);
  objectOutputStream.flush();
  objectOutputStream.close();

  // 反序列化
  FileInputStream fileInputStream = new FileInputStream("/tmp/obj.ser");
  ObjectInputStream objectOutputStream = new ObjectInputStream(fileInputStream);
  JavaSerializable serializable1 = objectOutputStream.readObject();
}
```

序列化是什么:

* 序列化的作用就是可以将对象的内容变成二进制, 存入文件中保存
* 反序列化指的是将保存下来的二进制对象数据恢复成对象
序列化对对象的要求:
* 对象必须实现 `Serializable` 接口
* 对象中的所有属性必须都要可以被序列化, 如果出现无法被序列化的属性, 则序列化失败
限制:
* 对象被序列化后, 生成的二进制文件中, 包含了很多环境信息, 如对象头, 对象中的属性字段等, 所以内容相对较大
* 因为数据量大, 所以序列化和反序列化的过程比较慢
序列化的应用场景:
* 持久化对象数据
* 网络中不能传输 `Java` 对象, 只能将其序列化后传输二进制数据

Step 2: 在 `Spark` 中的序列化和反序列化的应用场景:

* `Task` 分发
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627194356.png)
`Task` 是一个对象, 想在网络中传输对象就必须要先序列化
* `RDD` 缓存

```scala
val rdd1 = rdd.flatMap(_.split(" "))
   .map((_, 1))
   .reduceByKey(_ + _)

rdd1.cache

rdd1.collect
```

* `RDD` 中处理的是对象, 例如说字符串, `Person` 对象等
* 如果缓存 `RDD` 中的数据, 就需要缓存这些对象
* 对象是不能存在文件中的, 必须要将对象序列化后, 将二进制数据存入文件
* 广播变量
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627195544.png)
* 广播变量会分发到不同的机器上, 这个过程中需要使用网络, 对象在网络中传输就必须先被序列化
* `Shuffle` 过程
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627200225.png)
* `Shuffle` 过程是由 `Reducer` 从 `Mapper` 中拉取数据, 这里面涉及到两个需要序列化对象的原因
`RDD` 中的数据对象需要在 `Mapper` 端落盘缓存, 等待拉取
`Mapper` 和 `Reducer` 要传输数据对象
* `Spark Streaming` 的 `Receiver`
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627200730.png)
* `Spark Streaming` 中获取数据的组件叫做 `Receiver`, 获取到的数据也是对象形式, 在获取到以后需要落盘暂存, 就需要对数据对象进行序列化
* 算子引用外部对象

```scala
class Unserializable(i: Int)

rdd.map(i => new Unserializable(i))
   .collect
   .foreach(println)
```

* 在 `Map` 算子的函数中, 传入了一个 `Unserializable` 的对象
* `Map` 算子的函数是会在整个集群中运行的, 那 `Unserializable` 对象就需要跟随 `Map` 算子的函数被传输到不同的节点上
* 如果 `Unserializable` 不能被序列化, 则会报错

Step 3: `RDD` 的序列化:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627202022.png)
`RDD` 的序列化:
RDD 的序列化只能使用 Java 序列化器, 或者 Kryo 序列化器
为什么?:

* RDD 中存放的是数据对象, 要保留所有的数据就必须要对对象的元信息进行保存, 例如对象头之类的
* 保存一整个对象, 内存占用和效率会比较低一些
`Kryo` 是什么:
* `Kryo` 是 `Spark` 引入的一个外部的序列化工具, 可以增快 `RDD` 的运行速度
* 因为 `Kryo` 序列化后的对象更小, 序列化和反序列化的速度非常快
* 在 `RDD` 中使用 `Kryo` 的过程如下

```scala
val conf = new SparkConf()
  .setMaster("local[2]")
  .setAppName("KyroTest")

conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.registerKryoClasses(Array(classOf[Person]))

val sc = new SparkContext(conf)

rdd.map(arr => Person(arr(0), arr(1), arr(2)))
```

Step 4: `DataFrame` 和 `Dataset` 中的序列化:
历史的问题:
`RDD` 中无法感知数据的组成, 无法感知数据结构, 只能以对象的形式处理数据
`DataFrame` 和 `Dataset` 的特点:
`DataFrame` 和 `Dataset` 是为结构化数据优化的
在 `DataFrame` 和 `Dataset` 中, 数据和数据的 `Schema` 是分开存储的

```scala
spark.read
     .csv("...")
     .where($"name" =!= "")
     .groupBy($"name")
     .map(row: Row => row)
     .show()
```

* `DataFrame` 中没有数据对象这个概念, 所有的数据都以行的形式存在于 `Row` 对象中, `Row` 中记录了每行数据的结构, 包括列名, 类型等
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627214134.png)
* `Dataset` 中上层可以提供有类型的 `API`, 用以操作数据, 但是在内部, 无论是什么类型的数据对象 `Dataset` 都使用一个叫做 `InternalRow` 的类型的对象存储数据

```scala
val dataset: Dataset[Person] = spark.read.csv(...).as[Person]
```

优化点 1: 元信息独立:
. `RDD` 不保存数据的元信息, 所以只能使用 `Java Serializer` 或者 `Kyro Serializer` 保存 *整个对象*
. `DataFrame` 和 `Dataset` 中保存了数据的元信息, 所以可以把元信息独立出来分开保存
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627233424.png)
. 一个 `DataFrame` 或者一个 `Dataset` 中, 元信息只需要保存一份, 序列化的时候, 元信息不需要参与
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627233851.png)
. 在反序列化 ( `InternalRow -> Object` ) 时加入 `Schema` 信息即可
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190627234337.png)
元信息不再参与序列化, 意味着数据存储量的减少, 和效率的增加
优化点 2: 使用堆外内存:

* `DataFrame` 和 `Dataset` 不再序列化元信息, 所以内存使用大大减少. 同时新的序列化方式还将数据存入堆外内存中, 从而避免 `GC` 的开销.
* 堆外内存又叫做 `Unsafe`, 之所以叫不安全的, 因为不能使用 `Java` 的垃圾回收机制, 需要自己负责对象的创建和回收, 性能很好, 但是不建议普通开发者使用, 毕竟不安全
总结
. 当需要将对象缓存下来的时候, 或者在网络中传输的时候, 要把对象转成二进制, 在使用的时候再将二进制转为对象, 这个过程叫做序列化和反序列化
. 在 `Spark` 中有很多场景需要存储对象, 或者在网络中传输对象
.. `Task` 分发的时候, 需要将任务序列化, 分发到不同的 `Executor` 中执行
.. 缓存 `RDD` 的时候, 需要保存 `RDD` 中的数据
.. 广播变量的时候, 需要将变量序列化, 在集群中广播
.. `RDD` 的 `Shuffle` 过程中 `Map` 和 `Reducer` 之间需要交换数据
.. 算子中如果引入了外部的变量, 这个外部的变量也需要被序列化
. `RDD` 因为不保留数据的元信息, 所以必须要序列化整个对象, 常见的方式是 `Java` 的序列化器, 和 `Kyro` 序列化器
. `Dataset` 和 `DataFrame` 中保留数据的元信息, 所以可以不再使用 `Java` 的序列化器和 `Kyro` 序列化器, 使用 `Spark` 特有的序列化协议, 生成 `UnsafeInternalRow` 用以保存数据, 这样不仅能减少数据量, 也能减少序列化和反序列化的开销, 其速度大概能达到 `RDD` 的序列化的 `20` 倍左右

##### 1.3. Spark Streaming 和 Structured Streaming

目标和过程
目标:
理解 `Spark Streaming` 和 `Structured Streaming` 之间的区别, 是非常必要的, 从这点上可以理解 `Structured Streaming` 的过去和产生契机
过程:
. `Spark Streaming` 时代
. `Structured Streaming` 时代
. `Spark Streaming` 和 `Structured Streaming`
`Spark Streaming` 时代:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628010204.png)
`Spark Streaming` 其实就是 `RDD` 的 `API` 的流式工具, 其本质还是 `RDD`, 存储和执行过程依然类似 `RDD`
`Structured Streaming` 时代:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628010542.png)
`Structured Streaming` 其实就是 `Dataset` 的 `API` 的流式工具, `API` 和 `Dataset` 保持高度一致
`Spark Streaming` 和 `Structured Streaming`:

* `Structured Streaming` 相比于 `Spark Streaming` 的进步就类似于 `Dataset` 相比于 `RDD` 的进步
* 另外还有一点, `Structured Streaming` 已经支持了连续流模型, 也就是类似于 `Flink` 那样的实时流, 而不是小批量, 但在使用的时候仍然有限制, 大部分情况还是应该采用小批量模式
在 `2.2.0` 以后 `Structured Streaming` 被标注为稳定版本, 意味着以后的 `Spark` 流式开发不应该在采用 `Spark Streaming` 了

#### 2. Structured Streaming 入门案例

目标:
了解 `Structured Streaming` 的编程模型, 为理解 `Structured Streaming` 时候是什么, 以及核心体系原理打下基础
步骤:
. 需求梳理
. `Structured Streaming` 代码实现
. 运行
. 验证结果

##### 2.1. 需求梳理

目标和过程
目标:
理解接下来要做的案例, 有的放矢
步骤:
. 需求
. 整体结构
. 开发方式
需求:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628144128.png)

* 编写一个流式计算的应用, 不断的接收外部系统的消息
* 对消息中的单词进行词频统计
* 统计全局的结果
整体结构:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628131804.png)
. `Socket Server` 等待 `Structured Streaming` 程序连接
. `Structured Streaming` 程序启动, 连接 `Socket Server`, 等待 `Socket Server` 发送数据
. `Socket Server` 发送数据, `Structured Streaming` 程序接收数据
. `Structured Streaming` 程序接收到数据后处理数据
. 数据处理后, 生成对应的结果集, 在控制台打印
开发方式和步骤:
`Socket server` 使用 `Netcat nc` 来实现
`Structured Streaming` 程序使用 `IDEA` 实现, 在 `IDEA` 中本地运行
. 编写代码
. 启动 `nc` 发送 `Socket` 消息
. 运行代码接收 `Socket` 消息统计词频
总结

* 简单来说, 就是要进行流式的词频统计, 使用 `Structured Streaming`

##### 2.2. 代码实现

目标和过程
目标:
实现 `Structured Streaming` 部分的代码编写
步骤:
. 创建文件
. 创建 `SparkSession`
. 读取 `Socket` 数据生成 `DataFrame`
. 将 `DataFrame` 转为 `Dataset`, 使用有类型的 `API` 处理词频统计
. 生成结果集, 并写入控制台

```scala
object SocketProcessor {

  def main(args: Array[String]): Unit = {

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("socket_processor")
      .getOrCreate()
//<1> 调整 `Log` 级别, 避免过多的 `Log` 影响视线
    spark.sparkContext.setLogLevel("ERROR")   // <1>

    import spark.implicits._

    // 2. 读取外部数据源, 并转为 Dataset[String]
    val source = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 9999)
      .load()
      .as[String]                             // <2>
//<2> 默认 `readStream` 会返回 `DataFrame`, 但是词频统计更适合使用 `Dataset` 的有类型 `API`
    // 3. 统计词频
    val words = source.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey(_._1)
      .count()

    // 4. 输出结果

    words.writeStream
      .outputMode(OutputMode.Complete())      //<3> 统计全局结果, 而不是一个批次
      .format("console")                      // <4><4> 将结果输出到控制台
      .start()                                // <5><5> 开始运行流式应用
      .awaitTermination()                     // <6><6> 阻塞主线程, 在子线程中不断获取数据
  }
}
```

总结

* `Structured Streaming` 中的编程步骤依然是先读, 后处理, 最后落地
* `Structured Streaming` 中的编程模型依然是 `DataFrame` 和 `Dataset`
* `Structured Streaming` 中依然是有外部数据源读写框架的, 叫做 `readStream` 和 `writeStream`
* `Structured Streaming` 和 `SparkSQL` 几乎没有区别, 唯一的区别是, `readStream` 读出来的是流, `writeStream` 是将流输出, 而 `SparkSQL` 中的批处理使用 `read` 和 `write`

##### 2.3. 运行和结果验证

目标和过程
目标:
代码已经编写完毕, 需要运行, 并查看结果集, 因为从结果集的样式中可以看到 `Structured Streaming` 的一些原理
步骤:
. 开启 `Socket server`
. 运行程序
. 查看数据集
开启 `Socket server` 和运行程序:
. 在虚拟机 `node01` 中运行 `nc -lk 9999`
. 在 IDEA 中运行程序
. 在 `node01` 中输入以下内容

```text
hello world
hello spark
hello hadoop
hello spark
hello spark
```

查看结果集:

```text
1
```

从结果集中可以观察到以下内容

* `Structured Streaming` 依然是小批量的流处理
* `Structured Streaming` 的输出是类似 `DataFrame` 的, 也具有 `Schema`, 所以也是针对结构化数据进行优化的
* 从输出的时间特点上来看, 是一个批次先开始, 然后收集数据, 再进行展示, 这一点和 `Spark Streaming` 不太一样
总结
. 运行的时候需要先开启 `Socket server`
. `Structured Streaming` 的 API 和运行也是针对结构化数据进行优化过的

#### 3. Stuctured Streaming 的体系和结构

目标:
了解 `Structured Streaming` 的体系结构和核心原理, 有两点好处, 一是需要了解原理才好进行性能调优, 二是了解原理后, 才能理解代码执行流程, 从而更好的记忆, 也做到知其然更知其所以然
步骤:
. `WordCount` 的执行原理
. `Structured Streaming` 的体系结构

##### 3.1. 无限扩展的表格

目标和过程
目标:
`Structured Streaming` 是一个复杂的体系, 由很多组件组成, 这些组件之间也会进行交互, 如果无法站在整体视角去观察这些组件之间的关系, 也无法理解 `Structured Streaming` 的全局
步骤:
. 了解 `Dataset` 这个计算模型和流式计算的关系
. 如何使用 `Dataset` 处理流式数据?
. `WordCount` 案例的执行过程和原理
`Dataset` 和流式计算:
可以理解为 `Spark` 中的 `Dataset` 有两种, 一种是处理静态批量数据的 `Dataset`, 一种是处理动态实时流的 `Dataset`, 这两种 `Dataset` 之间的区别如下

* 流式的 `Dataset` 使用 `readStream` 读取外部数据源创建, 使用 `writeStream` 写入外部存储
* 批式的 `Dataset` 使用 `read` 读取外部数据源创建, 使用 `write` 写入外部存储

如何使用 `Dataset` 这个编程模型表示流式计算?:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628191649.png)

* 可以把流式的数据想象成一个不断增长, 无限无界的表
* 无论是否有界, 全都使用 `Dataset` 这一套 `API`
* 通过这样的做法, 就能完全保证流和批的处理使用完全相同的代码, 减少这两种处理方式的差异

`WordCount` 的原理:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628232818.png)

* 整个计算过程大致上分为如下三个部分
. `Source`, 读取数据源
. `Query`, 在流式数据上的查询
. `Result`, 结果集生成
* 整个的过程如下
. 随着时间段的流动, 对外部数据进行批次的划分
. 在逻辑上, 将缓存所有的数据, 生成一张无限扩展的表, 在这张表上进行查询
. 根据要生成的结果类型, 来选择是否生成基于整个数据集的结果
总结
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628235321.png)

* `Dataset` 不仅可以表达流式数据的处理, 也可以表达批量数据的处理
* `Dataset` 之所以可以表达流式数据的处理, 因为 `Dataset` 可以模拟一张无限扩展的表, 外部的数据会不断的流入到其中

##### 3.2. 体系结构

目标和过程
目标:
`Structured Streaming` 是一个复杂的体系, 由很多组件组成, 这些组件之间也会进行交互, 如果无法站在整体视角去观察这些组件之间的关系, 也无法理解 `Structured Streaming` 的核心原理
步骤:
. 体系结构
. `StreamExecution` 的执行顺序
体系结构:

* 在 `Structured Streaming` 中负责整体流程和执行的驱动引擎叫做 `StreamExecution`
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190629111018.png)
`StreamExecution` 在流上进行基于 `Dataset` 的查询, 也就是说, `Dataset` 之所以能够在流上进行查询, 是因为 `StreamExecution` 的调度和管理
* `StreamExecution` 如何工作?
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190629100439.png)
`StreamExecution` 分为三个重要的部分
* `Source`, 从外部数据源读取数据
* `LogicalPlan`, 逻辑计划, 在流上的查询计划
* `Sink`, 对接外部系统, 写入结果
`StreamExecution` 的执行顺序:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190629113627.png)
. 根据进度标记, 从 `Source` 获取到一个由 `DataFrame` 表示的批次, 这个 `DataFrame` 表示数据的源头

```scala
val source = spark.readStream
  .format("socket")
  .option("host", "127.0.0.1")
  .option("port", 9999)
  .load()
  .as[String]
```

这一点非常类似 `val df = spark.read.csv()` 所生成的 `DataFrame`, 同样都是表示源头
根据源头 `DataFrame` 生成逻辑计划

```scala
val words = source.flatMap(_.split(" "))
  .map((_, 1))
  .groupByKey(_._1)
  .count()
```

上述代码表示的就是数据的查询, 这一个步骤将这样的查询步骤生成为逻辑执行计划
优化逻辑计划最终生成物理计划
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/67b14d92b21b191914800c384cbed439.png)
这一步其实就是使用 `Catalyst` 对执行计划进行优化, 经历基于规则的优化和基于成本模型的优化
执行物理计划将表示执行结果的 `DataFrame / Dataset` 交给 `Sink`
整个物理执行计划会针对每一个批次的数据进行处理, 处理后每一个批次都会生成一个表示结果的 `Dataset`
`Sink` 可以将每一个批次的结果 `Dataset` 落地到外部数据源
执行完毕后, 汇报 `Source` 这个批次已经处理结束, `Source` 提交并记录最新的进度
增量查询:

* 核心问题
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190628232818.png)
上图中清晰的展示了最终的结果生成是全局的结果, 而不是一个批次的结果, 但是从 `StreamExecution` 中可以看到, 针对流的处理是按照一个批次一个批次来处理的
那么, 最终是如何生成全局的结果集呢?
* 状态记录
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190629115459.png)
在 `Structured Streaming` 中有一个全局范围的高可用 `StateStore`, 这个时候针对增量的查询变为如下步骤
. 从 `StateStore` 中取出上次执行完成后的状态
. 把上次执行的结果加入本批次, 再进行计算, 得出全局结果
. 将当前批次的结果放入 `StateStore` 中, 留待下次使用
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190629123847.png)
总结

* `StreamExecution` 是整个 `Structured Streaming` 的核心, 负责在流上的查询
* `StreamExecution` 中三个重要的组成部分, 分别是 `Source` 负责读取每个批量的数据, `Sink` 负责将结果写入外部数据源, `Logical Plan` 负责针对每个小批量生成执行计划
* `StreamExecution` 中使用 `StateStore` 来进行状态的维护

#### 4. Source

目标:
流式计算一般就是通过数据源读取数据, 经过一系列处理再落地到某个地方, 所以这一小节先了解一下如何读取数据, 可以整合哪些数据源
过程:

. 从 `HDFS` 中读取数据
. 从 `Kafka` 中读取数据
. `Source` 的原理和结构

##### 4.1. 从 HDFS 中读取数据

目标:
在数据处理的时候, 经常会遇到这样的场景
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190630160310.png)
有时候也会遇到这样的场景
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190630160448.png)
以上两种场景有两个共同的特点

* 会产生大量小文件在 `HDFS` 上
* 数据需要处理
* 使用 `Structured Streaming` 可以满足以上两点, 接下来就一起了解以下如何使用 `Structured Streaming` 如何整合 `HDFS`

步骤:
. 案例结构
. 案例编写
. 参数配置

###### 4.1.1. 案例结构

###### 4.1.2. 案例编写

###### 4.1.3. 参数配置

Mark

## 示例

### log4j.properties

streaming/conf/log4j.properties

```properties
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Define some default values that can be overridden by system properties
hadoop.root.logger=INFO,console
hadoop.log.dir=.
hadoop.log.file=hadoop.log

# Define the root logger to the system property "hadoop.root.logger".
log4j.rootLogger=${hadoop.root.logger}, EventCounter

# Logging Threshold
log4j.threshold=ALL

# Null Appender
log4j.appender.NullAppender=org.apache.log4j.varia.NullAppender

#
# Rolling File Appender - cap space usage at 5gb.
#
hadoop.log.maxfilesize=256MB
hadoop.log.maxbackupindex=20
log4j.appender.RFA=org.apache.log4j.RollingFileAppender
log4j.appender.RFA.File=${hadoop.log.dir}/${hadoop.log.file}

log4j.appender.RFA.MaxFileSize=${hadoop.log.maxfilesize}
log4j.appender.RFA.MaxBackupIndex=${hadoop.log.maxbackupindex}

log4j.appender.RFA.layout=org.apache.log4j.PatternLayout

# Pattern format: Date LogLevel LoggerName LogMessage
log4j.appender.RFA.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
# Debugging Pattern format
#log4j.appender.RFA.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n


#
# Daily Rolling File Appender
#

log4j.appender.DRFA=org.apache.log4j.DailyRollingFileAppender
log4j.appender.DRFA.File=${hadoop.log.dir}/${hadoop.log.file}

# Rollver at midnight
log4j.appender.DRFA.DatePattern=.yyyy-MM-dd

# 30-day backup
#log4j.appender.DRFA.MaxBackupIndex=30
log4j.appender.DRFA.layout=org.apache.log4j.PatternLayout

# Pattern format: Date LogLevel LoggerName LogMessage
log4j.appender.DRFA.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
# Debugging Pattern format
#log4j.appender.DRFA.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n


#
# console
# Add "console" to rootlogger above if you want to use this
#

log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM HH:mm:ss} %p %c{2}: %m%n

#
# TaskLog Appender
#

#Default values
hadoop.tasklog.taskid=null
hadoop.tasklog.iscleanup=false
hadoop.tasklog.noKeepSplits=4
hadoop.tasklog.totalLogFileSize=100
hadoop.tasklog.purgeLogSplits=true
hadoop.tasklog.logsRetainHours=12

log4j.appender.TLA=org.apache.hadoop.mapred.TaskLogAppender
log4j.appender.TLA.taskId=${hadoop.tasklog.taskid}
log4j.appender.TLA.isCleanup=${hadoop.tasklog.iscleanup}
log4j.appender.TLA.totalLogFileSize=${hadoop.tasklog.totalLogFileSize}

log4j.appender.TLA.layout=org.apache.log4j.PatternLayout
log4j.appender.TLA.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n

#
# HDFS block state change log from block manager
#
# Uncomment the following to suppress normal block state change
# messages from BlockManager in NameNode.
#log4j.logger.BlockStateChange=WARN

#
#Security appender
#
hadoop.security.logger=INFO,NullAppender
hadoop.security.log.maxfilesize=256MB
hadoop.security.log.maxbackupindex=20
log4j.category.SecurityLogger=${hadoop.security.logger}
hadoop.security.log.file=SecurityAuth-${user.name}.audit
log4j.appender.RFAS=org.apache.log4j.RollingFileAppender
log4j.appender.RFAS.File=${hadoop.log.dir}/${hadoop.security.log.file}
log4j.appender.RFAS.layout=org.apache.log4j.PatternLayout
log4j.appender.RFAS.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
log4j.appender.RFAS.MaxFileSize=${hadoop.security.log.maxfilesize}
log4j.appender.RFAS.MaxBackupIndex=${hadoop.security.log.maxbackupindex}

#
# Daily Rolling Security appender
#
log4j.appender.DRFAS=org.apache.log4j.DailyRollingFileAppender
log4j.appender.DRFAS.File=${hadoop.log.dir}/${hadoop.security.log.file}
log4j.appender.DRFAS.layout=org.apache.log4j.PatternLayout
log4j.appender.DRFAS.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
log4j.appender.DRFAS.DatePattern=.yyyy-MM-dd

#
# hadoop configuration logging
#

# Uncomment the following line to turn off configuration deprecation warnings.
# log4j.logger.org.apache.hadoop.conf.Configuration.deprecation=WARN

#
# hdfs audit logging
#
hdfs.audit.logger=INFO,NullAppender
hdfs.audit.log.maxfilesize=256MB
hdfs.audit.log.maxbackupindex=20
log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit=${hdfs.audit.logger}
log4j.additivity.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit=false
log4j.appender.RFAAUDIT=org.apache.log4j.RollingFileAppender
log4j.appender.RFAAUDIT.File=${hadoop.log.dir}/hdfs-audit.log
log4j.appender.RFAAUDIT.layout=org.apache.log4j.PatternLayout
log4j.appender.RFAAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
log4j.appender.RFAAUDIT.MaxFileSize=${hdfs.audit.log.maxfilesize}
log4j.appender.RFAAUDIT.MaxBackupIndex=${hdfs.audit.log.maxbackupindex}

#
# mapred audit logging
#
mapred.audit.logger=INFO,NullAppender
mapred.audit.log.maxfilesize=256MB
mapred.audit.log.maxbackupindex=20
log4j.logger.org.apache.hadoop.mapred.AuditLogger=${mapred.audit.logger}
log4j.additivity.org.apache.hadoop.mapred.AuditLogger=false
log4j.appender.MRAUDIT=org.apache.log4j.RollingFileAppender
log4j.appender.MRAUDIT.File=${hadoop.log.dir}/mapred-audit.log
log4j.appender.MRAUDIT.layout=org.apache.log4j.PatternLayout
log4j.appender.MRAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
log4j.appender.MRAUDIT.MaxFileSize=${mapred.audit.log.maxfilesize}
log4j.appender.MRAUDIT.MaxBackupIndex=${mapred.audit.log.maxbackupindex}

# Custom Logging levels

#log4j.logger.org.apache.hadoop.mapred.JobTracker=DEBUG
#log4j.logger.org.apache.hadoop.mapred.TaskTracker=DEBUG
#log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit=DEBUG

# Jets3t library
log4j.logger.org.jets3t.service.impl.rest.httpclient.RestS3Service=ERROR

# AWS SDK & S3A FileSystem
log4j.logger.com.amazonaws=ERROR
log4j.logger.com.amazonaws.http.AmazonHttpClient=ERROR
log4j.logger.org.apache.hadoop.fs.s3a.S3AFileSystem=WARN

#
# Event Counter Appender
# Sends counts of logging messages at different severity levels to Hadoop Metrics.
#
log4j.appender.EventCounter=org.apache.hadoop.log.metrics.EventCounter

#
# Job Summary Appender
#
# Use following logger to send summary to separate file defined by
# hadoop.mapreduce.jobsummary.log.file :
# hadoop.mapreduce.jobsummary.logger=INFO,JSA
#
hadoop.mapreduce.jobsummary.logger=${hadoop.root.logger}
hadoop.mapreduce.jobsummary.log.file=hadoop-mapreduce.jobsummary.log
hadoop.mapreduce.jobsummary.log.maxfilesize=256MB
hadoop.mapreduce.jobsummary.log.maxbackupindex=20
log4j.appender.JSA=org.apache.log4j.RollingFileAppender
log4j.appender.JSA.File=${hadoop.log.dir}/${hadoop.mapreduce.jobsummary.log.file}
log4j.appender.JSA.MaxFileSize=${hadoop.mapreduce.jobsummary.log.maxfilesize}
log4j.appender.JSA.MaxBackupIndex=${hadoop.mapreduce.jobsummary.log.maxbackupindex}
log4j.appender.JSA.layout=org.apache.log4j.PatternLayout
log4j.appender.JSA.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n
log4j.logger.org.apache.hadoop.mapred.JobInProgress$JobSummary=${hadoop.mapreduce.jobsummary.logger}
log4j.additivity.org.apache.hadoop.mapred.JobInProgress$JobSummary=false

#
# Yarn ResourceManager Application Summary Log
#
# Set the ResourceManager summary log filename
yarn.server.resourcemanager.appsummary.log.file=rm-appsummary.log
# Set the ResourceManager summary log level and appender
yarn.server.resourcemanager.appsummary.logger=${hadoop.root.logger}
#yarn.server.resourcemanager.appsummary.logger=INFO,RMSUMMARY

# To enable AppSummaryLogging for the RM,
# set yarn.server.resourcemanager.appsummary.logger to
# <LEVEL>,RMSUMMARY in hadoop-env.sh

# Appender for ResourceManager Application Summary Log
# Requires the following properties to be set
#    - hadoop.log.dir (Hadoop Log directory)
#    - yarn.server.resourcemanager.appsummary.log.file (resource manager app summary log filename)
#    - yarn.server.resourcemanager.appsummary.logger (resource manager app summary log level and appender)

log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary=${yarn.server.resourcemanager.appsummary.logger}
log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary=false
log4j.appender.RMSUMMARY=org.apache.log4j.RollingFileAppender
log4j.appender.RMSUMMARY.File=${hadoop.log.dir}/${yarn.server.resourcemanager.appsummary.log.file}
log4j.appender.RMSUMMARY.MaxFileSize=256MB
log4j.appender.RMSUMMARY.MaxBackupIndex=20
log4j.appender.RMSUMMARY.layout=org.apache.log4j.PatternLayout
log4j.appender.RMSUMMARY.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n

# HS audit log configs
#mapreduce.hs.audit.logger=INFO,HSAUDIT
#log4j.logger.org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger=${mapreduce.hs.audit.logger}
#log4j.additivity.org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger=false
#log4j.appender.HSAUDIT=org.apache.log4j.DailyRollingFileAppender
#log4j.appender.HSAUDIT.File=${hadoop.log.dir}/hs-audit.log
#log4j.appender.HSAUDIT.layout=org.apache.log4j.PatternLayout
#log4j.appender.HSAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
#log4j.appender.HSAUDIT.DatePattern=.yyyy-MM-dd

# Http Server Request Logs
#log4j.logger.http.requests.namenode=INFO,namenoderequestlog
#log4j.appender.namenoderequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.namenoderequestlog.Filename=${hadoop.log.dir}/jetty-namenode-yyyy_mm_dd.log
#log4j.appender.namenoderequestlog.RetainDays=3

#log4j.logger.http.requests.datanode=INFO,datanoderequestlog
#log4j.appender.datanoderequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.datanoderequestlog.Filename=${hadoop.log.dir}/jetty-datanode-yyyy_mm_dd.log
#log4j.appender.datanoderequestlog.RetainDays=3

#log4j.logger.http.requests.resourcemanager=INFO,resourcemanagerrequestlog
#log4j.appender.resourcemanagerrequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.resourcemanagerrequestlog.Filename=${hadoop.log.dir}/jetty-resourcemanager-yyyy_mm_dd.log
#log4j.appender.resourcemanagerrequestlog.RetainDays=3

#log4j.logger.http.requests.jobhistory=INFO,jobhistoryrequestlog
#log4j.appender.jobhistoryrequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.jobhistoryrequestlog.Filename=${hadoop.log.dir}/jetty-jobhistory-yyyy_mm_dd.log
#log4j.appender.jobhistoryrequestlog.RetainDays=3

#log4j.logger.http.requests.nodemanager=INFO,nodemanagerrequestlog
#log4j.appender.nodemanagerrequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.nodemanagerrequestlog.Filename=${hadoop.log.dir}/jetty-nodemanager-yyyy_mm_dd.log
#log4j.appender.nodemanagerrequestlog.RetainDays=3


# WebHdfs request log on datanodes
# Specify -Ddatanode.webhdfs.logger=INFO,HTTPDRFA on datanode startup to
# direct the log to a separate file.
#datanode.webhdfs.logger=INFO,console
#log4j.logger.datanode.webhdfs=${datanode.webhdfs.logger}
#log4j.appender.HTTPDRFA=org.apache.log4j.DailyRollingFileAppender
#log4j.appender.HTTPDRFA.File=${hadoop.log.dir}/hadoop-datanode-webhdfs.log
#log4j.appender.HTTPDRFA.layout=org.apache.log4j.PatternLayout
#log4j.appender.HTTPDRFA.layout.ConversionPattern=%d{ISO8601} %m%n
#log4j.appender.HTTPDRFA.DatePattern=.yyyy-MM-dd

#
# Fair scheduler state dump
#
# Use following logger to dump the state to a separate file

#log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler.statedump=DEBUG,FSSTATEDUMP
#log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler.statedump=false
#log4j.appender.FSSTATEDUMP=org.apache.log4j.RollingFileAppender
#log4j.appender.FSSTATEDUMP.File=${hadoop.log.dir}/fairscheduler-statedump.log
#log4j.appender.FSSTATEDUMP.layout=org.apache.log4j.PatternLayout
#log4j.appender.FSSTATEDUMP.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
#log4j.appender.FSSTATEDUMP.MaxFileSize=${hadoop.log.maxfilesize}
#log4j.appender.FSSTATEDUMP.MaxBackupIndex=${hadoop.log.maxbackupindex}
```

### dataset

### dev

#### application.conf

streaming/src/main/resources/dev/application.conf

```conf
#sparksql自动广播
spark.sql.autoBroadcastJoinThreshold="10485760"
#sparksql shuffle默认分区数
spark.sql.shuffle.partitions="200"

#shuffle的时候是否压缩输出文件
spark.shuffle.compress="true"

spark.shuffle.io.maxRetries="3"

spark.shuffle.io.retryWait="5s"
#shuffle spill的时候压缩
spark.shuffle.spill.compress="true"

spark.serializer="org.apache.spark.serializer.KryoSerializer"

spark.memory.fraction="0.6"

spark.memory.storageFraction="0.5"

spark.default.parallelism="20"

spark.speculation="true"

spark.speculation.multiplier="1.5"

data.path="dataset/pmt.json"

GeoLiteCity.dat = "conf/GeoLiteCity.dat"

IP_FILE="qqwry.dat"

INSTALL_DIR="conf/"

ods.table.name= "ODS_%s"
```

#### log4j.properties

streaming/src/main/resources/dev/log4j.properties

```properties
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Define some default values that can be overridden by system properties
hadoop.root.logger=INFO,console
hadoop.log.dir=.
hadoop.log.file=hadoop.log

# Define the root logger to the system property "hadoop.root.logger".
log4j.rootLogger=${hadoop.root.logger}, EventCounter

# Logging Threshold
log4j.threshold=ALL

# Null Appender
log4j.appender.NullAppender=org.apache.log4j.varia.NullAppender

#
# Rolling File Appender - cap space usage at 5gb.
#
hadoop.log.maxfilesize=256MB
hadoop.log.maxbackupindex=20
log4j.appender.RFA=org.apache.log4j.RollingFileAppender
log4j.appender.RFA.File=${hadoop.log.dir}/${hadoop.log.file}

log4j.appender.RFA.MaxFileSize=${hadoop.log.maxfilesize}
log4j.appender.RFA.MaxBackupIndex=${hadoop.log.maxbackupindex}

log4j.appender.RFA.layout=org.apache.log4j.PatternLayout

# Pattern format: Date LogLevel LoggerName LogMessage
log4j.appender.RFA.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
# Debugging Pattern format
#log4j.appender.RFA.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n


#
# Daily Rolling File Appender
#

log4j.appender.DRFA=org.apache.log4j.DailyRollingFileAppender
log4j.appender.DRFA.File=${hadoop.log.dir}/${hadoop.log.file}

# Rollver at midnight
log4j.appender.DRFA.DatePattern=.yyyy-MM-dd

# 30-day backup
#log4j.appender.DRFA.MaxBackupIndex=30
log4j.appender.DRFA.layout=org.apache.log4j.PatternLayout

# Pattern format: Date LogLevel LoggerName LogMessage
log4j.appender.DRFA.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
# Debugging Pattern format
#log4j.appender.DRFA.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n


#
# console
# Add "console" to rootlogger above if you want to use this
#

log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM HH:mm:ss} %p %c{2}: %m%n

#
# TaskLog Appender
#

#Default values
hadoop.tasklog.taskid=null
hadoop.tasklog.iscleanup=false
hadoop.tasklog.noKeepSplits=4
hadoop.tasklog.totalLogFileSize=100
hadoop.tasklog.purgeLogSplits=true
hadoop.tasklog.logsRetainHours=12

log4j.appender.TLA=org.apache.hadoop.mapred.TaskLogAppender
log4j.appender.TLA.taskId=${hadoop.tasklog.taskid}
log4j.appender.TLA.isCleanup=${hadoop.tasklog.iscleanup}
log4j.appender.TLA.totalLogFileSize=${hadoop.tasklog.totalLogFileSize}

log4j.appender.TLA.layout=org.apache.log4j.PatternLayout
log4j.appender.TLA.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n

#
# HDFS block state change log from block manager
#
# Uncomment the following to suppress normal block state change
# messages from BlockManager in NameNode.
#log4j.logger.BlockStateChange=WARN

#
#Security appender
#
hadoop.security.logger=INFO,NullAppender
hadoop.security.log.maxfilesize=256MB
hadoop.security.log.maxbackupindex=20
log4j.category.SecurityLogger=${hadoop.security.logger}
hadoop.security.log.file=SecurityAuth-${user.name}.audit
log4j.appender.RFAS=org.apache.log4j.RollingFileAppender
log4j.appender.RFAS.File=${hadoop.log.dir}/${hadoop.security.log.file}
log4j.appender.RFAS.layout=org.apache.log4j.PatternLayout
log4j.appender.RFAS.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
log4j.appender.RFAS.MaxFileSize=${hadoop.security.log.maxfilesize}
log4j.appender.RFAS.MaxBackupIndex=${hadoop.security.log.maxbackupindex}

#
# Daily Rolling Security appender
#
log4j.appender.DRFAS=org.apache.log4j.DailyRollingFileAppender
log4j.appender.DRFAS.File=${hadoop.log.dir}/${hadoop.security.log.file}
log4j.appender.DRFAS.layout=org.apache.log4j.PatternLayout
log4j.appender.DRFAS.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
log4j.appender.DRFAS.DatePattern=.yyyy-MM-dd

#
# hadoop configuration logging
#

# Uncomment the following line to turn off configuration deprecation warnings.
# log4j.logger.org.apache.hadoop.conf.Configuration.deprecation=WARN

#
# hdfs audit logging
#
hdfs.audit.logger=INFO,NullAppender
hdfs.audit.log.maxfilesize=256MB
hdfs.audit.log.maxbackupindex=20
log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit=${hdfs.audit.logger}
log4j.additivity.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit=false
log4j.appender.RFAAUDIT=org.apache.log4j.RollingFileAppender
log4j.appender.RFAAUDIT.File=${hadoop.log.dir}/hdfs-audit.log
log4j.appender.RFAAUDIT.layout=org.apache.log4j.PatternLayout
log4j.appender.RFAAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
log4j.appender.RFAAUDIT.MaxFileSize=${hdfs.audit.log.maxfilesize}
log4j.appender.RFAAUDIT.MaxBackupIndex=${hdfs.audit.log.maxbackupindex}

#
# mapred audit logging
#
mapred.audit.logger=INFO,NullAppender
mapred.audit.log.maxfilesize=256MB
mapred.audit.log.maxbackupindex=20
log4j.logger.org.apache.hadoop.mapred.AuditLogger=${mapred.audit.logger}
log4j.additivity.org.apache.hadoop.mapred.AuditLogger=false
log4j.appender.MRAUDIT=org.apache.log4j.RollingFileAppender
log4j.appender.MRAUDIT.File=${hadoop.log.dir}/mapred-audit.log
log4j.appender.MRAUDIT.layout=org.apache.log4j.PatternLayout
log4j.appender.MRAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
log4j.appender.MRAUDIT.MaxFileSize=${mapred.audit.log.maxfilesize}
log4j.appender.MRAUDIT.MaxBackupIndex=${mapred.audit.log.maxbackupindex}

# Custom Logging levels

#log4j.logger.org.apache.hadoop.mapred.JobTracker=DEBUG
#log4j.logger.org.apache.hadoop.mapred.TaskTracker=DEBUG
#log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit=DEBUG

# Jets3t library
log4j.logger.org.jets3t.service.impl.rest.httpclient.RestS3Service=ERROR

# AWS SDK & S3A FileSystem
log4j.logger.com.amazonaws=ERROR
log4j.logger.com.amazonaws.http.AmazonHttpClient=ERROR
log4j.logger.org.apache.hadoop.fs.s3a.S3AFileSystem=WARN

#
# Event Counter Appender
# Sends counts of logging messages at different severity levels to Hadoop Metrics.
#
log4j.appender.EventCounter=org.apache.hadoop.log.metrics.EventCounter

#
# Job Summary Appender
#
# Use following logger to send summary to separate file defined by
# hadoop.mapreduce.jobsummary.log.file :
# hadoop.mapreduce.jobsummary.logger=INFO,JSA
#
hadoop.mapreduce.jobsummary.logger=${hadoop.root.logger}
hadoop.mapreduce.jobsummary.log.file=hadoop-mapreduce.jobsummary.log
hadoop.mapreduce.jobsummary.log.maxfilesize=256MB
hadoop.mapreduce.jobsummary.log.maxbackupindex=20
log4j.appender.JSA=org.apache.log4j.RollingFileAppender
log4j.appender.JSA.File=${hadoop.log.dir}/${hadoop.mapreduce.jobsummary.log.file}
log4j.appender.JSA.MaxFileSize=${hadoop.mapreduce.jobsummary.log.maxfilesize}
log4j.appender.JSA.MaxBackupIndex=${hadoop.mapreduce.jobsummary.log.maxbackupindex}
log4j.appender.JSA.layout=org.apache.log4j.PatternLayout
log4j.appender.JSA.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n
log4j.logger.org.apache.hadoop.mapred.JobInProgress$JobSummary=${hadoop.mapreduce.jobsummary.logger}
log4j.additivity.org.apache.hadoop.mapred.JobInProgress$JobSummary=false

#
# Yarn ResourceManager Application Summary Log
#
# Set the ResourceManager summary log filename
yarn.server.resourcemanager.appsummary.log.file=rm-appsummary.log
# Set the ResourceManager summary log level and appender
yarn.server.resourcemanager.appsummary.logger=${hadoop.root.logger}
#yarn.server.resourcemanager.appsummary.logger=INFO,RMSUMMARY

# To enable AppSummaryLogging for the RM,
# set yarn.server.resourcemanager.appsummary.logger to
# <LEVEL>,RMSUMMARY in hadoop-env.sh

# Appender for ResourceManager Application Summary Log
# Requires the following properties to be set
#    - hadoop.log.dir (Hadoop Log directory)
#    - yarn.server.resourcemanager.appsummary.log.file (resource manager app summary log filename)
#    - yarn.server.resourcemanager.appsummary.logger (resource manager app summary log level and appender)

log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary=${yarn.server.resourcemanager.appsummary.logger}
log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary=false
log4j.appender.RMSUMMARY=org.apache.log4j.RollingFileAppender
log4j.appender.RMSUMMARY.File=${hadoop.log.dir}/${yarn.server.resourcemanager.appsummary.log.file}
log4j.appender.RMSUMMARY.MaxFileSize=256MB
log4j.appender.RMSUMMARY.MaxBackupIndex=20
log4j.appender.RMSUMMARY.layout=org.apache.log4j.PatternLayout
log4j.appender.RMSUMMARY.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n

# HS audit log configs
#mapreduce.hs.audit.logger=INFO,HSAUDIT
#log4j.logger.org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger=${mapreduce.hs.audit.logger}
#log4j.additivity.org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger=false
#log4j.appender.HSAUDIT=org.apache.log4j.DailyRollingFileAppender
#log4j.appender.HSAUDIT.File=${hadoop.log.dir}/hs-audit.log
#log4j.appender.HSAUDIT.layout=org.apache.log4j.PatternLayout
#log4j.appender.HSAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
#log4j.appender.HSAUDIT.DatePattern=.yyyy-MM-dd

# Http Server Request Logs
#log4j.logger.http.requests.namenode=INFO,namenoderequestlog
#log4j.appender.namenoderequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.namenoderequestlog.Filename=${hadoop.log.dir}/jetty-namenode-yyyy_mm_dd.log
#log4j.appender.namenoderequestlog.RetainDays=3

#log4j.logger.http.requests.datanode=INFO,datanoderequestlog
#log4j.appender.datanoderequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.datanoderequestlog.Filename=${hadoop.log.dir}/jetty-datanode-yyyy_mm_dd.log
#log4j.appender.datanoderequestlog.RetainDays=3

#log4j.logger.http.requests.resourcemanager=INFO,resourcemanagerrequestlog
#log4j.appender.resourcemanagerrequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.resourcemanagerrequestlog.Filename=${hadoop.log.dir}/jetty-resourcemanager-yyyy_mm_dd.log
#log4j.appender.resourcemanagerrequestlog.RetainDays=3

#log4j.logger.http.requests.jobhistory=INFO,jobhistoryrequestlog
#log4j.appender.jobhistoryrequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.jobhistoryrequestlog.Filename=${hadoop.log.dir}/jetty-jobhistory-yyyy_mm_dd.log
#log4j.appender.jobhistoryrequestlog.RetainDays=3

#log4j.logger.http.requests.nodemanager=INFO,nodemanagerrequestlog
#log4j.appender.nodemanagerrequestlog=org.apache.hadoop.http.HttpRequestLogAppender
#log4j.appender.nodemanagerrequestlog.Filename=${hadoop.log.dir}/jetty-nodemanager-yyyy_mm_dd.log
#log4j.appender.nodemanagerrequestlog.RetainDays=3


# WebHdfs request log on datanodes
# Specify -Ddatanode.webhdfs.logger=INFO,HTTPDRFA on datanode startup to
# direct the log to a separate file.
#datanode.webhdfs.logger=INFO,console
#log4j.logger.datanode.webhdfs=${datanode.webhdfs.logger}
#log4j.appender.HTTPDRFA=org.apache.log4j.DailyRollingFileAppender
#log4j.appender.HTTPDRFA.File=${hadoop.log.dir}/hadoop-datanode-webhdfs.log
#log4j.appender.HTTPDRFA.layout=org.apache.log4j.PatternLayout
#log4j.appender.HTTPDRFA.layout.ConversionPattern=%d{ISO8601} %m%n
#log4j.appender.HTTPDRFA.DatePattern=.yyyy-MM-dd

#
# Fair scheduler state dump
#
# Use following logger to dump the state to a separate file

#log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler.statedump=DEBUG,FSSTATEDUMP
#log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler.statedump=false
#log4j.appender.FSSTATEDUMP=org.apache.log4j.RollingFileAppender
#log4j.appender.FSSTATEDUMP.File=${hadoop.log.dir}/fairscheduler-statedump.log
#log4j.appender.FSSTATEDUMP.layout=org.apache.log4j.PatternLayout
#log4j.appender.FSSTATEDUMP.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
#log4j.appender.FSSTATEDUMP.MaxFileSize=${hadoop.log.maxfilesize}
#log4j.appender.FSSTATEDUMP.MaxBackupIndex=${hadoop.log.maxbackupindex}
```

### structured

#### ForeachSink.scala

streaming/src/main/scala/cn/xhchen/structured/ForeachSink.scala

```scala
package cn.xhchen.structured

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}

object ForeachSink {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil")

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("hdfs_sink")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取 Kafka 数据
    val source: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node01:9092,node02:9092,node03:9092")
      .option("subscribe", "streaming_test_2")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as value")
      .as[String]

    // 1::Toy Story (1995)::Animation|Children's|Comedy

    // 3. 处理 CSV, Dataset(String), Dataset(id, name, category)
    val result = source.map(item => {
      val arr = item.split("::")
      (arr(0).toInt, arr(1).toString, arr(2).toString)
    }).as[(Int, String, String)].toDF("id", "name", "category")

    // 4. 落地到 MySQL
    class MySQLWriter extends ForeachWriter[Row] {
      private val driver = "com.mysql.jdbc.Driver"
      private var connection: Connection = _
      private val url = "jdbc::mysql://node01:3306/streaming-movies-result"
      private var statement: Statement = _

      override def open(partitionId: Long, version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url)
        statement = connection.createStatement()
        true
      }

      override def process(value: Row): Unit = {
        statement.executeUpdate(s"insert into movies values(${value.get(0)}, ${value.get(1)}, ${value.get(2)})")
      }

      override def close(errorOrNull: Throwable): Unit = {
        connection.close()
      }
    }

    result.writeStream
      .foreach(new MySQLWriter)
      .start()
      .awaitTermination()
  }
}
```

#### HDFSSink.scala

streaming/src/main/scala/cn/xhchen/structured/HDFSSink.scala

```scala
package cn.xhchen.structured

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HDFSSink {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil")

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("hdfs_sink")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取 Kafka 数据
    val source: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node01:9092,node02:9092,node03:9092")
      .option("subscribe", "streaming_test_2")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as value")
      .as[String]

    // 1::Toy Story (1995)::Animation|Children's|Comedy

    // 3. 处理 CSV, Dataset(String), Dataset(id, name, category)
    val result = source.map(item => {
      val arr = item.split("::")
      (arr(0).toInt, arr(1).toString, arr(2).toString)
    }).as[(Int, String, String)].toDF("id", "name", "category")

    // 4. 落地到 HDFS 中
    result.writeStream
      .format("parquet")
      .option("path", "dataset/streaming/moives/")
      .option("checkpointLocation", "checkpoint")
      .start()
      .awaitTermination()
  }
}
```

#### HDFSSource.scala

streaming/src/main/scala/cn/xhchen/structured/HDFSSource.scala

```scala
package cn.xhchen.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

object HDFSSource {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil")

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("hdfs_source")
      .master("local[6]")
      .getOrCreate()

    // 2. 数据读取, 目录只能是文件夹, 不能是某一个文件
    val schema = new StructType()
      .add("name", "string")
      .add("age", "integer")

    val source = spark.readStream
      .schema(schema)
      .json("hdfs://node01:8020/dataset/dataset")

    // 3. 输出结果
    source.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .start()
      .awaitTermination()
  }
}
```

#### KafkaSink.scala

streaming/src/main/scala/cn/xhchen/structured/KafkaSink.scala

```scala
package cn.xhchen.structured

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

object KafkaSink {

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\winutil")

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("hdfs_sink")
      .master("local[6]")
      .getOrCreate()
  spark.sparkContext.setLogLevel("warn")
    import spark.implicits._

    // 2. 读取 Kafka 数据
    val source: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
      .option("subscribe", "streaming_test_2")
      .option("startingOffsets", "earliest")
      //.option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as value")
      .as[String]

    // 1::Toy Story (1995)::Animation|Children's|Comedy

    // 3. 处理 CSV, Dataset(String), Dataset(id, name, category)
    /*val result = source.map(item => {
      val arr = item.split("::")
      (arr(0).toInt, arr(1).toString, arr(2).toString)
    }).as[(Int, String, String)].toDF("id", "name", "category")*/


    // 4. 落地到 HDFS 中
    source.writeStream
      .format("kafka")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "checkpoint")
      .option("kafka.bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
      .option("topic", "streaming_test_3")
      .start()
      .awaitTermination()


  }
}
```

#### KafkaSource.scala

streaming/src/main/scala/cn/xhchen/structured/KafkaSource.scala

```scala
package cn.xhchen.structured

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BooleanType, DateType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSource {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("hdfs_source")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取 Kafka 数据
    val source: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
      .option("subscribe", "streaming_test_1")
      .option("startingOffsets", "earliest")
      .load()

    // 3. 定义 JSON 中的类型
    val eventType = new StructType()
      .add("has_sound", BooleanType)
      .add("has_motion", BooleanType)
      .add("has_person", BooleanType)
      .add("start_time", DateType)
      .add("end_time", DateType)

    val cameraType = new StructType()
      .add("device_id", StringType)
      .add("last_event", eventType)

    val deviceType = new StructType()
      .add("cameras", cameraType)

    val schema = new StructType()
      .add("devices", deviceType)

    // 4. 解析 JSON
    // 需求: DataFrame(time, has_person)
    import org.apache.spark.sql.functions._

    val jsonOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.sss'Z'")

    val result = source.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
      .select(from_json('value, schema, jsonOptions).alias("parsed_value"))
      .selectExpr("parsed_value.devices.cameras.last_event.start_time", "parsed_value.devices.cameras.last_event.has_person")

    // 5. 打印数据
    result.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }
}
```

#### SocketWordCount.scala

streaming/src/main/scala/cn/xhchen/structured/SocketWordCount.scala

```scala
package cn.xhchen.structured

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SocketWordCount {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("socket_structured")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 2. 数据集的生成, 数据读取
    val source: DataFrame = spark.readStream
      .format("socket")
      .option("host", "192.168.169.101")
      .option("port", 9999)
      .load()

    val sourceDS: Dataset[String] = source.as[String]

    // 3. 数据的处理
    val words = sourceDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey(_._1)
      .count()

    // 4. 结果集的生成和输出
    words.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
      .awaitTermination()
  }
}
```

#### Triggers.scala

streaming/src/main/scala/cn/xhchen/structured/Triggers.scala

```scala
package cn.xhchen.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object Triggers {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil")
    // 创建数据源
    val spark = SparkSession.builder()
      .appName("triggers")
      .master("local[6]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // timestamp, value
    val source = spark.readStream
      .format("rate")
      .load()

    // 简单处理
    //
    val result = source

    // 落地
    source.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()
  }
}
```

### test

### java

### scala

#### BusinessArea.scala

streaming/src/test/scala/BusinessArea.scala

```scala
import java.util

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

import scala.util.Try

object BusinessArea {

  def main(args: Array[String]): Unit = {

    val MASTER_ADDRESS="hadoop01:7051,hadoop02:7051,hadoop03:7051"

    //1、创建SparkSession
    val spark = SparkSession.builder().appName("parseip").master("local[2]").getOrCreate()

    val kuduContext = new KuduContext(MASTER_ADDRESS,spark.sparkContext)

    //2、读取ods数据
    import org.apache.kudu.spark.kudu._
    import spark.implicits._
    spark.read.option("kudu.master",MASTER_ADDRESS)
      .option("kudu.table","ODS_20190815")
      .kudu
    //3、取出经纬度
      .selectExpr("longitude","latitude")
    //4、数据处理
    //4.1  数据过滤
      .filter("longitude is not null and latitude is not null")
    //4.2  数据去重
      .distinct()
      .as[(Float,Float)]
    //5、根据经纬度调用高德的接口获取数据
      .map(tude=>{
      val longitude = tude._1
      val latitude = tude._2

      val url = "https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=55f651a99ab19180f80105acd129d85d"

      val json: String = HttpClient.get(url)

      val areas = parseJson(json)

      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)

      (geoHash,areas)
    }).toDF("geoHash","areas").distinct()
    //6、根据经纬度生成geohash

    //7、商圈信息保存入kudu

  }

  def parseJson(json:String)={
     Try{

        val obj: JSONObject = JSON.parseObject(json)

        val regeocode = obj.getJSONObject("regeocode")

        val addressComponent: JSONObject = regeocode.getJSONObject("addressComponent")

        val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")

        val areas: util.List[BusinessArea] = businessAreas.toJavaList(classOf[BusinessArea])

        import scala.collection.JavaConverters._
        areas.asScala.map(_.name).mkString(",")}.getOrElse("")

  }
}

case class BusinessArea(location:String,name:String,id:String)
```

#### ConfigUtils.scala

streaming/src/test/scala/ConfigUtils.scala

```scala
import com.typesafe.config.ConfigFactory

object ConfigUtils {

  val conf = ConfigFactory.load()
  /**
    * spark.memory.storageFraction="0.5"
    *
    * spark.default.parallelism="20"
    */
  val spark_memory_storageFraction = conf.getString("spark.memory.storageFraction")
  val spark_default_parallelism = conf.getString("spark.default.parallelism")

  val data_path = conf.getString("data.path")

  val GeoLiteCity_data = conf.getString("GeoLiteCity.dat")

  val IP_FILE = conf.getString("IP_FILE")
  val INSTALL_DIR = conf.getString("INSTALL_DIR")

  val ods_table_name = conf.getString("ods.table.name")


}
```

#### Graphx$.java

streaming/src/test/scala/Graphx$.java

```java
object Graphx {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("app"))
    val vertexId = Seq[(Long,(String,Int))](
      (1L,("name",20)),
      (2L,("lisi",30)),
      (2L,("wag",35))
    )
    val vertices: RDD[(VertexId, (String, Int))] = sc.parallelize(vertexId)

    val edges = sc.parallelize(Seq[Edge[Int]](
      Edge(1,133,1),
      Edge(2,133,1)
    ))

    val graph = Graph(vertices,edges)

    val connect: Graph[VertexId, Int] = graph.connectedComponents()
    //(id,aggid) join (id,(name,age)) => (id,(aggid,(name,age)))
    //aggid
    val info: RDD[(VertexId, (VertexId, (String, Int)))] = connect.vertices.join(vertices)

    info.map(x=>x match {
      case (id,(aggid,(name,age)))=>
        (aggid,(id,name,age))
    }).groupByKey().foreach(println)

  }
}
```

#### HttpClient.scala

streaming/src/test/scala/HttpClient.scala

```scala
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

object HttpClient {

  def get(url:String): String ={

    val client = new HttpClient()

    val getMethod = new GetMethod(url)

    val code: Int = client.executeMethod(getMethod)

    if(code==200){
      getMethod.getResponseBodyAsString
    }else{
      ""
    }
  }
}
```

#### IPAddressUtils.java

streaming/src/test/scala/IPAddressUtils.java

```java
/**
 * Created by angel；
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IP地址服务
 */
public class IPAddressUtils {
    private static Logger log = LoggerFactory.getLogger(IPAddressUtils.class);
    /**
     * 纯真IP数据库名
     */
    private String IP_FILE= ConfigUtils.IP_FILE();
    /**
     * 纯真IP数据库保存的文件夹
     */
    private String INSTALL_DIR=ConfigUtils.INSTALL_DIR();

    /**
     * 常量，比如记录长度等等
     */
    private static final int IP_RECORD_LENGTH = 7;
    /**
     * 常量，读取模式1
     */
    private static final byte REDIRECT_MODE_1 = 0x01;
    /**
     * 常量，读取模式2
     */
    private static final byte REDIRECT_MODE_2 = 0x02;

    /**
     * 缓存，查询IP时首先查询缓存，以减少不必要的重复查找
     */
    private Map<String, IPLocation> ipCache;
    /**
     * 随机文件访问类
     */
    private RandomAccessFile ipFile;
    /**
     * 内存映射文件
     */
    private MappedByteBuffer mbb;
    /**
     * 起始地区的开始和结束的绝对偏移
     */
    private long ipBegin, ipEnd;

    /**
     * 为提高效率而采用的临时变量
     */
    private IPLocation loc;
    /**
     * 为提高效率而采用的临时变量
     */
    private byte[] buf;
    /**
     * 为提高效率而采用的临时变量
     */
    private byte[] b4;
    /**
     * 为提高效率而采用的临时变量
     */
    private byte[] b3;
    /**
     * IP地址库文件错误
     */
    private static final String BAD_IP_FILE     =   "IP地址库文件错误";
    /**
     * 未知国家
     */
    private static final String UNKNOWN_COUNTRY   =   "未知国家";
    /**
     * 未知地区
     */
    private static final String UNKNOWN_AREA    =   "未知地区";


    public void init() {
        try {
            // 缓存一定要用ConcurrentHashMap， 避免多线程下获取为空
            ipCache = new ConcurrentHashMap();
            loc = new IPLocation();
            buf = new byte[100];
            b4 = new byte[4];
            b3 = new byte[3];
            try {
                ipFile = new RandomAccessFile(IP_FILE, "r");
            } catch (FileNotFoundException e) {
                // 如果找不到这个文件，再尝试再当前目录下搜索，这次全部改用小写文件名
                //     因为有些系统可能区分大小写导致找不到ip地址信息文件
                String filename = new File(IP_FILE).getName().toLowerCase();
                File[] files = new File(INSTALL_DIR).listFiles();
                for(int i = 0; i < files.length; i++) {
                    if(files[i].isFile()) {
                        if(files[i].getName().toLowerCase().equals(filename)) {
                            try {
                                ipFile = new RandomAccessFile(files[i], "r");
                            } catch (FileNotFoundException e1) {
                                log.error("IP地址信息文件没有找到，IP显示功能将无法使用:{}" + e1.getMessage(), e1);
                                ipFile = null;
                            }
                            break;
                        }
                    }
                }
            }
            // 如果打开文件成功，读取文件头信息
            if(ipFile != null) {
                try {
                    ipBegin = readLong4(0);
                    ipEnd = readLong4(4);
                    if(ipBegin == -1 || ipEnd == -1) {
                        ipFile.close();
                        ipFile = null;
                    }
                } catch (IOException e) {
                    log.error("IP地址信息文件格式有错误，IP显示功能将无法使用"+ e.getMessage(), e);
                    ipFile = null;
                }
            }

        } catch (Exception e) {
            log.error("IP地址服务初始化异常:" + e.getMessage(), e);
        }
    }

    /**
     * 查询IP地址位置 - synchronized的作用是避免多线程时获取区域信息为空
     * @param ip
     * @return
     */
    public synchronized IPLocation getIPLocation(final String ip) {
        IPLocation location = new IPLocation();
        location.setArea(this.getArea(ip));
        location.setCountry(this.getCountry(ip));

        return location;
    }

    /**
     * 从内存映射文件的offset位置开始的3个字节读取一个int
     * @param offset
     * @return
     */
    private int readInt3(int offset) {
        mbb.position(offset);
        return mbb.getInt() & 0x00FFFFFF;
    }

    /**
     * 从内存映射文件的当前位置开始的3个字节读取一个int
     * @return
     */
    private int readInt3() {
        return mbb.getInt() & 0x00FFFFFF;
    }

    /**
     * 根据IP得到国家名
     * @param ip ip的字节数组形式
     * @return 国家名字符串
     */
    public String getCountry(byte[] ip) {
        // 检查ip地址文件是否正常
        if(ipFile == null)
            return BAD_IP_FILE;
        // 保存ip，转换ip字节数组为字符串形式
        String ipStr = Util.getIpStringFromBytes(ip);
        // 先检查cache中是否已经包含有这个ip的结果，没有再搜索文件
        if(ipCache.containsKey(ipStr)) {
            IPLocation ipLoc = ipCache.get(ipStr);
            return ipLoc.getCountry();
        } else {
            IPLocation ipLoc = getIPLocation(ip);
            ipCache.put(ipStr, ipLoc.getCopy());
            return ipLoc.getCountry();
        }
    }

    /**
     * 根据IP得到国家名
     * @param ip IP的字符串形式
     * @return 国家名字符串
     */
    public String getCountry(String ip) {
        return getCountry(Util.getIpByteArrayFromString(ip));
    }

    /**
     * 根据IP得到地区名
     * @param ip ip的字节数组形式
     * @return 地区名字符串
     */
    public String getArea(final byte[] ip) {
        // 检查ip地址文件是否正常
        if(ipFile == null)
            return BAD_IP_FILE;
        // 保存ip，转换ip字节数组为字符串形式
        String ipStr = Util.getIpStringFromBytes(ip);
        // 先检查cache中是否已经包含有这个ip的结果，没有再搜索文件
        if(ipCache.containsKey(ipStr)) {
            IPLocation ipLoc = ipCache.get(ipStr);
            return ipLoc.getArea();
        } else {
            IPLocation ipLoc = getIPLocation(ip);
            ipCache.put(ipStr, ipLoc.getCopy());
            return ipLoc.getArea();
        }
    }

    /**
     * 根据IP得到地区名
     * @param ip IP的字符串形式
     * @return 地区名字符串
     */
    public String getArea(final String ip) {
        return getArea(Util.getIpByteArrayFromString(ip));
    }

    /**
     * 根据ip搜索ip信息文件，得到IPLocation结构，所搜索的ip参数从类成员ip中得到
     * @param ip 要查询的IP
     * @return IPLocation结构
     */
    private IPLocation getIPLocation(final byte[] ip) {
        IPLocation info = null;
        long offset = locateIP(ip);
        if(offset != -1)
            info = getIPLocation(offset);
        if(info == null) {
            info = new IPLocation();
            info.setCountry (  UNKNOWN_COUNTRY);
            info.setArea(UNKNOWN_AREA);
        }
        return info;
    }

    /**
     * 从offset位置读取4个字节为一个long，因为java为big-endian格式，所以没办法
     * 用了这么一个函数来做转换
     * @param offset
     * @return 读取的long值，返回-1表示读取文件失败
     */
    private long readLong4(long offset) {
        long ret = 0;
        try {
            ipFile.seek(offset);
            ret |= (ipFile.readByte() & 0xFF);
            ret |= ((ipFile.readByte() << 8) & 0xFF00);
            ret |= ((ipFile.readByte() << 16) & 0xFF0000);
            ret |= ((ipFile.readByte() << 24) & 0xFF000000);
            return ret;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 从offset位置读取3个字节为一个long，因为java为big-endian格式，所以没办法
     * 用了这么一个函数来做转换
     * @param offset 整数的起始偏移
     * @return 读取的long值，返回-1表示读取文件失败
     */
    private long readLong3(long offset) {
        long ret = 0;
        try {
            ipFile.seek(offset);
            ipFile.readFully(b3);
            ret |= (b3[0] & 0xFF);
            ret |= ((b3[1] << 8) & 0xFF00);
            ret |= ((b3[2] << 16) & 0xFF0000);
            return ret;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 从当前位置读取3个字节转换成long
     * @return 读取的long值，返回-1表示读取文件失败
     */
    private long readLong3() {
        long ret = 0;
        try {
            ipFile.readFully(b3);
            ret |= (b3[0] & 0xFF);
            ret |= ((b3[1] << 8) & 0xFF00);
            ret |= ((b3[2] << 16) & 0xFF0000);
            return ret;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 从offset位置读取四个字节的ip地址放入ip数组中，读取后的ip为big-endian格式，但是
     * 文件中是little-endian形式，将会进行转换
     * @param offset
     * @param ip
     */
    private void readIP(long offset, byte[] ip) {
        try {
            ipFile.seek(offset);
            ipFile.readFully(ip);
            byte temp = ip[0];
            ip[0] = ip[3];
            ip[3] = temp;
            temp = ip[1];
            ip[1] = ip[2];
            ip[2] = temp;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 从offset位置读取四个字节的ip地址放入ip数组中，读取后的ip为big-endian格式，但是
     * 文件中是little-endian形式，将会进行转换
     * @param offset
     * @param ip
     */
    private void readIP(int offset, byte[] ip) {
        mbb.position(offset);
        mbb.get(ip);
        byte temp = ip[0];
        ip[0] = ip[3];
        ip[3] = temp;
        temp = ip[1];
        ip[1] = ip[2];
        ip[2] = temp;
    }

    /**
     * 把类成员ip和beginIp比较，注意这个beginIp是big-endian的
     * @param ip 要查询的IP
     * @param beginIp 和被查询IP相比较的IP
     * @return 相等返回0，ip大于beginIp则返回1，小于返回-1。
     */
    private int compareIP(byte[] ip, byte[] beginIp) {
        for(int i = 0; i < 4; i++) {
            int r = compareByte(ip[i], beginIp[i]);
            if(r != 0)
                return r;
        }
        return 0;
    }

    /**
     * 把两个byte当作无符号数进行比较
     * @param b1
     * @param b2
     * @return 若b1大于b2则返回1，相等返回0，小于返回-1
     */
    private int compareByte(byte b1, byte b2) {
        if((b1 & 0xFF) > (b2 & 0xFF)) // 比较是否大于
            return 1;
        else if((b1 ^ b2) == 0)// 判断是否相等
            return 0;
        else
            return -1;
    }

    /**
     * 这个方法将根据ip的内容，定位到包含这个ip国家地区的记录处，返回一个绝对偏移
     * 方法使用二分法查找。
     * @param ip 要查询的IP
     * @return 如果找到了，返回结束IP的偏移，如果没有找到，返回-1
     */
    private long locateIP(byte[] ip) {
        long m = 0;
        int r;
        // 比较第一个ip项
        readIP(ipBegin, b4);
        r = compareIP(ip, b4);
        if(r == 0) return ipBegin;
        else if(r < 0) return -1;
        // 开始二分搜索
        for(long i = ipBegin, j = ipEnd; i < j; ) {
            m = getMiddleOffset(i, j);
            readIP(m, b4);
            r = compareIP(ip, b4);
            // log.debug(Utils.getIpStringFromBytes(b));
            if(r > 0)
                i = m;
            else if(r < 0) {
                if(m == j) {
                    j -= IP_RECORD_LENGTH;
                    m = j;
                } else
                    j = m;
            } else
                return readLong3(m + 4);
        }
        // 如果循环结束了，那么i和j必定是相等的，这个记录为最可能的记录，但是并非
        //     肯定就是，还要检查一下，如果是，就返回结束地址区的绝对偏移
        m = readLong3(m + 4);
        readIP(m, b4);
        r = compareIP(ip, b4);
        if(r <= 0) return m;
        else return -1;
    }

    /**
     * 得到begin偏移和end偏移中间位置记录的偏移
     * @param begin
     * @param end
     * @return
     */
    private long getMiddleOffset(long begin, long end) {
        long records = (end - begin) / IP_RECORD_LENGTH;
        records >>= 1;
        if(records == 0) records = 1;
        return begin + records * IP_RECORD_LENGTH;
    }

    /**
     * 给定一个ip国家地区记录的偏移，返回一个IPLocation结构
     * @param offset 国家记录的起始偏移
     * @return IPLocation对象
     */
    private IPLocation getIPLocation(long offset) {
        try {
            // 跳过4字节ip
            ipFile.seek(offset + 4);
            // 读取第一个字节判断是否标志字节
            byte b = ipFile.readByte();
            if(b == REDIRECT_MODE_1) {
                // 读取国家偏移
                long countryOffset = readLong3();
                // 跳转至偏移处
                ipFile.seek(countryOffset);
                // 再检查一次标志字节，因为这个时候这个地方仍然可能是个重定向
                b = ipFile.readByte();
                if(b == REDIRECT_MODE_2) {
                    loc.setCountry (  readString(readLong3()));
                    ipFile.seek(countryOffset + 4);
                } else
                    loc.setCountry ( readString(countryOffset));
                // 读取地区标志
                loc.setArea( readArea(ipFile.getFilePointer()));
            } else if(b == REDIRECT_MODE_2) {
                loc.setCountry ( readString(readLong3()));
                loc.setArea( readArea(offset + 8));
            } else {
                loc.setCountry (  readString(ipFile.getFilePointer() - 1));
                loc.setArea( readArea(ipFile.getFilePointer()));
            }
            return loc;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 给定一个ip国家地区记录的偏移，返回一个IPLocation结构，此方法应用与内存映射文件方式
     * @param offset 国家记录的起始偏移
     * @return IPLocation对象
     */
    private IPLocation getIPLocation(int offset) {
        // 跳过4字节ip
        mbb.position(offset + 4);
        // 读取第一个字节判断是否标志字节
        byte b = mbb.get();
        if(b == REDIRECT_MODE_1) {
            // 读取国家偏移
            int countryOffset = readInt3();
            // 跳转至偏移处
            mbb.position(countryOffset);
            // 再检查一次标志字节，因为这个时候这个地方仍然可能是个重定向
            b = mbb.get();
            if(b == REDIRECT_MODE_2) {
                loc.setCountry (  readString(readInt3()));
                mbb.position(countryOffset + 4);
            } else
                loc.setCountry (  readString(countryOffset));
            // 读取地区标志
            loc.setArea(readArea(mbb.position()));
        } else if(b == REDIRECT_MODE_2) {
            loc.setCountry ( readString(readInt3()));
            loc.setArea(readArea(offset + 8));
        } else {
            loc.setCountry (  readString(mbb.position() - 1));
            loc.setArea(readArea(mbb.position()));
        }
        return loc;
    }

    /**
     * 从offset偏移开始解析后面的字节，读出一个地区名
     * @param offset 地区记录的起始偏移
     * @return 地区名字符串
     * @throws IOException
     */
    private String readArea(long offset) throws IOException {
        ipFile.seek(offset);
        byte b = ipFile.readByte();
        if(b == REDIRECT_MODE_1 || b == REDIRECT_MODE_2) {
            long areaOffset = readLong3(offset + 1);
            if(areaOffset == 0)
                return UNKNOWN_AREA;
            else
                return readString(areaOffset);
        } else
            return readString(offset);
    }

    /**
     * @param offset 地区记录的起始偏移
     * @return 地区名字符串
     */
    private String readArea(int offset) {
        mbb.position(offset);
        byte b = mbb.get();
        if(b == REDIRECT_MODE_1 || b == REDIRECT_MODE_2) {
            int areaOffset = readInt3();
            if(areaOffset == 0)
                return UNKNOWN_AREA;
            else
                return readString(areaOffset);
        } else
            return readString(offset);
    }

    /**
     * 从offset偏移处读取一个以0结束的字符串
     * @param offset 字符串起始偏移
     * @return 读取的字符串，出错返回空字符串
     */
    private String readString(long offset) {
        try {
            ipFile.seek(offset);
            int i;
            for(i = 0, buf[i] = ipFile.readByte(); buf[i] != 0; buf[++i] = ipFile.readByte());
            if(i != 0)
                return Util.getString(buf, 0, i, "GBK");
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }

    /**
     * 从内存映射文件的offset位置得到一个0结尾字符串
     * @param offset 字符串起始偏移
     * @return 读取的字符串，出错返回空字符串
     */
    private String readString(int offset) {
        try {
            mbb.position(offset);
            int i;
            for(i = 0, buf[i] = mbb.get(); buf[i] != 0; buf[++i] = mbb.get());
            if(i != 0)
                return Util.getString(buf, 0, i, "GBK");
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }

    public String getCity(final String ipAddress){
        try {
            if(ipAddress.startsWith("192.168.")){
                log.error("此IP[{}]段不进行处理！", ipAddress);
                return null;
            }
            return getIPLocation(ipAddress).getCity();
        }catch (Exception e){
            log.error("根据IP[{}]获取省份失败:{}", ipAddress, e.getMessage());
            return null;
        }
    }

    public IPLocation getregion(String ip){
        IPAddressUtils ipAddressUtils = new IPAddressUtils();
        ipAddressUtils.init();
        return ipAddressUtils.getIPLocation(ip);

    }

    public static void main(String[] args){
        IPAddressUtils ip = new IPAddressUtils();
        ip.init();
        String address = "61.237.126.185";
        System.out.println("IP地址["+address + "]获取到的区域信息:" + ip.getIPLocation(address).getCountry() + ", 获取到的城市:" + ip.getIPLocation(address).getCity() + ", 运营商:"+ip.getIPLocation(address).getArea());
        System.out.println(ip.getIPLocation(address).getRegion());
    }

}

```

#### IPLocation.java

streaming/src/test/scala/IPLocation.java

```java
/**
 * Created by angel；
 */
public class IPLocation {
    /**
     * 国家
     */
    private String country;
    /**
     * 区域 - 省份 + 城市
     */
    private String area;
    private String region;

    public IPLocation() {
        country = region = area = "";
    }

    public synchronized IPLocation getCopy() {
        IPLocation ret = new IPLocation();
        ret.country = country;
        ret.area = area;
        ret.region = region;
        return ret;
    }

    public String getRegion() {
        String region = "";
        if(country != null){
            String[] array = country.split("省");
            if(array != null && array.length > 1){
                region =  array[0]+"省";
            } else {
                region = country;
            }
            if(region.length() > 3){
                region.replace("内蒙古", "");
            }
        }
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        String city = "";
        if(country != null){
            String[] array = country.split("省");
            if(array != null && array.length > 1){
                city =  array[1];
            } else {
                city = country;
            }
            if(city.length() > 3){
                city.replace("内蒙古", "");
            }
        }
        return city;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        //如果为局域网，纯真IP地址库的地区会显示CZ88.NET,这里把它去掉
        if(area.trim().equals("CZ88.NET")){
            this.area="本机或本网络";
        }else{
            this.area = area;
        }
    }
}

```

#### KuduTest.scala

streaming/src/test/scala/KuduTest.scala

```scala
import java.util

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.junit.Test

import scala.util.Try

class KuduTest {

  val spark = SparkSession.builder().master("local[2]").appName("app").getOrCreate()
  val MASTER_ADDRESS="hadoop01:7051,hadoop02:7051,hadoop03:7051"
  val kuducontext = new KuduContext(MASTER_ADDRESS,spark.sparkContext)
  import spark.implicits._
  //指定操作的kudu表名
  val tableName = "student1"


  @Test
  def createTable:Unit={

    val schema = new StructType()
        .add("id",IntegerType)
        .add("name",StringType)
        .add("age",IntegerType)

    val keys = Seq[String]("id")

    val options = new CreateTableOptions()
    val colums = new util.ArrayList[String]()
    colums.add("id")
    options.addHashPartitions(colums,3)
    options.setNumReplicas(3)
    kuducontext.createTable(tableName = tableName,schema=schema,keys=keys,options=options)
  }

  @Test
  def insert(): Unit ={

    var data:Seq[(Int,String,Int)] = Nil

    for(i<- 1 to 10){
      data = data.+:((i,"zhangsan-"+i,20+i))
    }
    //dataFrame的列名必须要和表的列名相同
    val df = data.toDF("id","name","age")

    kuducontext.insertRows(df,tableName)
  }

  @Test
  def select(): Unit ={
    kuducontext.kuduRDD(spark.sparkContext,tableName,Seq("id","name","age"))
      .foreach(println)
  }

  /**
    * 删除的时候，只能用id字段，如果加上别的字段，那么删除报错
    */
  @Test
  def delete(): Unit ={
    val df = Seq((1),(2)).toDF("id")
    kuducontext.deleteRows(df,tableName)
  }

  @Test
  def update(): Unit ={
    val df = Seq((3,"wangwu",33)).toDF("id","name","age")
    kuducontext.updateRows(df,tableName)
  }

  @Test
  def sqlselect={
    import org.apache.kudu.spark.kudu._

    val ds:Option[DataFrame] = Try(Some(spark.read
      .option("kudu.master",MASTER_ADDRESS)
      .option("kudu.table",tableName)
      .kudu)).getOrElse(None)


  }

  @Test
  def sqlWrite(): Unit ={
    var data:Seq[(Int,String,Int)] = Nil

    for(i<- 1 to 10){
      data = data.+:((10+i,"lisi-"+i,50+i))
    }

    val df = data.toDF("id","name","age")
    import org.apache.kudu.spark.kudu._
    df.write.mode(SaveMode.Append).option("kudu.master",MASTER_ADDRESS)
      .option("kudu.table",tableName)
      .kudu
  }

  @Test
  def sql(): Unit ={
    import org.apache.kudu.spark.kudu._

    spark.read
      .option("kudu.master",MASTER_ADDRESS)
      .option("kudu.table",tableName)
      .kudu.createOrReplaceTempView("tmp")


    spark.sql("delete from tmp where age>20").show()
  }
}
```

#### KuduUtils.scala

streaming/src/test/scala/KuduUtils.scala

```scala
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType

object KuduUtils {

  def write2Kudu(kuduContext:KuduContext,schema:StructType,keys:Seq[String],
                 options:CreateTableOptions,tableName:String,data:DataFrame): Unit ={

    if(!kuduContext.tableExists(tableName)){
      kuduContext.createTable(tableName,schema,keys,options)
    }

    kuduContext.insertRows(data,tableName)
  }
}
```

#### ParseIpProcess.scala

streaming/src/test/scala/ParseIpProcess.scala

```scala
import java.util

import com.maxmind.geoip.{Location, LookupService}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

object ParseIpProcess {

  def main(args: Array[String]): Unit = {
    val MASTER_ADDRESS="hadoop01:7051,hadoop02:7051,hadoop03:7051"

    //1、创建SparkSession
    val spark = SparkSession.builder().appName("parseip").master("local[2]").getOrCreate()

    val kuduContext = new KuduContext(MASTER_ADDRESS,spark.sparkContext)
    //2、读取数据
    val source =spark.read.json(ConfigUtils.data_path).cache()

    source.createOrReplaceTempView("source_data")
    //3、将ip列弄出来单独进行处理
    import spark.implicits._
    source.selectExpr("ip").filter("ip is not null and ip!=''").distinct().as[String]
      .map(ip=>{
        //4、根据IP解析经纬度 、省市
        val lookupService = new LookupService(ConfigUtils.GeoLiteCity_data)
        val location: Location = lookupService.getLocation(ip)

        val longitude = location.longitude
        val latitude = location.latitude

        val ipaddress = new IPAddressUtils

        val regions = ipaddress.getregion(ip)

        val region = regions.getRegion

        val city = regions.getCity
        (ip,longitude,latitude,region,city)
      }).toDF("ip","longitude","latitude","region","city").createOrReplaceTempView("ip_info")


    //5、将ip对应的省市信息补充到元数据中
    val result = spark.sql(
      """
        |select
        | a.ip,
        |a.sessionid,
        |a.advertisersid,
        |a.adorderid,
        |a.adcreativeid,
        |a.adplatformproviderid,
        |a.sdkversion,
        |a.adplatformkey,
        |a.putinmodeltype,
        |a.requestmode,
        |a.adprice,
        |a.adppprice,
        |a.requestdate,
        |a.appid,
        |a.appname,
        |a.uuid,
        |a.device,
        |a.client,
        |a.osversion,
        |a.density,
        |a.pw,
        |a.ph,
        |b.longitude,
        |b.latitude,
        |b.region,
        |b.city,
        |a.ispid,
        |a.ispname,
        |a.networkmannerid,
        |a.networkmannername,
        |a.iseffective,
        |a.isbilling,
        |a.adspacetype,
        |a.adspacetypename,
        |a.devicetype,
        |a.processnode,
        |a.apptype,
        |a.district,
        |a.paymode,
        |a.isbid,
        |a.bidprice,
        |a.winprice,
        |a.iswin,
        |a.cur,
        |a.rate,
        |a.cnywinprice,
        |a.imei,
        |a.mac,
        |a.idfa,
        |a.openudid,
        |a.androidid,
        |a.rtbprovince,
        |a.rtbcity,
        |a.rtbdistrict,
        |a.rtbstreet,
        |a.storeurl,
        |a.realip,
        |a.isqualityapp,
        |a.bidfloor,
        |a.aw,
        |a.ah,
        |a.imeimd5,
        |a.macmd5,
        |a.idfamd5,
        |a.openudidmd5,
        |a.androididmd5,
        |a.imeisha1,
        |a.macsha1,
        |a.idfasha1,
        |a.openudidsha1,
        |a.androididsha1,
        |a.uuidunknow,
        |a.userid,
        |a.iptype,
        |a.initbidprice,
        |a.adpayment,
        |a.agentrate,
        |a.lomarkrate,
        |a.adxrate,
        |a.title,
        |a.keywords,
        |a.tagid,
        |a.callbackdate,
        |a.channelid,
        |a.mediatype,
        |a.email,
        |a.tel,
        |a.sex,
        |a.age
        | from source_data a left join ip_info b
        | on a.ip = b.ip
      """.stripMargin)
    //6、数据写入kudu
    val columns = new util.ArrayList[String]()
    columns.add("ip")
    val options = new CreateTableOptions
    options.addHashPartitions(columns,3)
    options.setNumReplicas(3)
    KuduUtils.write2Kudu(kuduContext = kuduContext,schema= result.schema,Seq[String]("ip"),options,ConfigUtils.ods_table_name.format("20190815"),result)
  }
}
```

#### Util.java

streaming/src/test/scala/Util.java

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;


/**
 * 工具类，提供IP字符串转数组的方法
 */
public class Util {
    private static final Logger log = LoggerFactory.getLogger(Util.class);


    /**
     * 从ip的字符串形式得到字节数组形式
     *
     * @param ip 字符串形式的ip
     * @return 字节数组形式的ip
     */
    public static byte[] getIpByteArrayFromString(String ip) {
        byte[] ret = new byte[4];
        StringTokenizer st = new StringTokenizer(ip, ".");
        try {
            ret[0] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
            ret[1] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
            ret[2] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
            ret[3] = (byte) (Integer.parseInt(st.nextToken()) & 0xFF);
        } catch (Exception e) {
            log.error("从ip的字符串形式得到字节数组形式报错" + e.getMessage(), e);
        }
        return ret;
    }

    /**
     * 字节数组IP转String
     * @param ip ip的字节数组形式
     * @return 字符串形式的ip
     */
    public static String getIpStringFromBytes(byte[] ip) {
        StringBuilder sb = new StringBuilder();
        sb.delete(0, sb.length());
        sb.append(ip[0] & 0xFF);
        sb.append('.');
        sb.append(ip[1] & 0xFF);
        sb.append('.');
        sb.append(ip[2] & 0xFF);
        sb.append('.');
        sb.append(ip[3] & 0xFF);
        return sb.toString();
    }

    /**
     * 根据某种编码方式将字节数组转换成字符串
     *
     * @param b        字节数组
     * @param offset   要转换的起始位置
     * @param len      要转换的长度
     * @param encoding 编码方式
     * @return 如果encoding不支持，返回一个缺省编码的字符串
     */
    public static String getString(byte[] b, int offset, int len, String encoding) {
        try {
            return new String(b, offset, len, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b, offset, len);
        }
    }
}

```

#### pom.xml

streaming/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>cn.xhchen</groupId>
    <artifactId>streaming</artifactId>
    <version>0.1.0</version>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <scala.version>2.11.5</scala.version>
        <scala.v>2.11</scala.v>
        <hadoop.version>2.6.1</hadoop.version>
        <spark.version>2.1.0</spark.version>
        <kudu.version>1.6.0-cdh5.14.0</kudu.version>
        <elasticsearch.verion>6.0.0</elasticsearch.verion>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.11.8</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.5</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.12</version>
        </dependency>

        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
            <scope>provided</scope>
        </dependency>

        <!--导入kudu客户端依赖-->
        <!-- https://mvnrepository.com/artifact/org.apache.kudu/kudu-client -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client</artifactId>
            <version>${kudu.version}</version>
            <scope>test</scope>
        </dependency>

        <!--导入kudu客户端工具类依赖-->
        <!-- https://mvnrepository.com/artifact/org.apache.kudu/kudu-client-tools -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client-tools</artifactId>
            <version>${kudu.version}</version>
        </dependency>

        <!--导入kudu整合spark的依赖-->
        <!-- https://mvnrepository.com/artifact/org.apache.kudu/kudu-spark2 -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-spark2_${scala.v}</artifactId>
            <version>${kudu.version}</version>
        </dependency>

        <!--导入spark Mlib依赖-->
        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-mllib -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-mllib_${scala.v}</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <!--导入elasticsearch-spark依赖-->
        <!-- https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20 -->
        <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch-spark-20_${scala.v}</artifactId>
            <version>${elasticsearch.verion}</version>
        </dependency>

        <!--导入spark Graphx依赖-->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-graphx_${scala.v}</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <!--导入fastjson操作json的依赖-->
        <!-- https://mvnrepository.com/artifact/com.alibaba/fastjson -->
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.44</version>
        </dependency>

        <dependency>
            <groupId>net.sf.json-lib</groupId>
            <artifactId>json-lib</artifactId>
            <version>2.4</version>
            <classifier>jdk15</classifier>
        </dependency>

        <!-- 根据ip解析经纬度 -->
        <dependency>
            <groupId>com.maxmind.geoip</groupId>
            <artifactId>geoip-api</artifactId>
            <version>1.3.0</version>
        </dependency>
        <dependency>
            <groupId>com.maxmind.geoip2</groupId>
            <artifactId>geoip2</artifactId>
            <version>2.12.0</version>
        </dependency>

        <!--对经纬度进行geohash编码的依赖-->
        <dependency>
            <groupId>ch.hsr</groupId>
            <artifactId>geohash</artifactId>
            <version>1.3.0</version>
        </dependency>

        <!-- 导入impala的依赖 -->
        <dependency>
            <groupId>com.cloudera</groupId>
            <artifactId>ImpalaJDBC41</artifactId>
            <version>2.5.42</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.thrift/libfb303 -->
        <!--解决：Caused by: java.lang.ClassNotFoundException: org.apache.thrift.protocol.TPro-->
        <dependency>
            <groupId>org.apache.thrift</groupId>
            <artifactId>libfb303</artifactId>
            <version>0.9.3</version>
            <type>pom</type>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.thrift/libthrift -->
        <!--解决：Caused by: java.lang.ClassNotFoundException: org.apache.thrift.protocol.TPro-->
        <dependency>
            <groupId>org.apache.thrift</groupId>
            <artifactId>libthrift</artifactId>
            <version>0.9.3</version>
            <type>pom</type>
        </dependency>

        <!--Caused by: java.lang.ClassNotFoundException: org.apache.hive.service.cli.thrift.TCLIService$Iface-->
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-jdbc</artifactId>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.hive</groupId>
                    <artifactId>hive-service-rpc</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.apache.hive</groupId>
                    <artifactId>hive-service</artifactId>
                </exclusion>
            </exclusions>
            <version>1.1.0</version>
        </dependency>
        <!--导入hive的依赖-->
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-service</artifactId>
            <version>1.1.0</version>
        </dependency>

        <!-- 导入加载配置文件的依赖 -->
        <dependency>
            <groupId>com.typesafe</groupId>
            <artifactId>config</artifactId>
            <version>1.2.1</version>
        </dependency>
    </dependencies>
    <profiles>
        <profile>
            <id>dev</id>
            <activation>
                <!--默认生效的配置组-->
                <activeByDefault>true</activeByDefault>
                <property>
                    <name>env</name>
                    <value>Dev</value>
                </property>
            </activation>
            <build>
                <!--配置文件路径-->
                <resources>
                    <resource>
                        <directory>src/main/resources/dev</directory>
                    </resource>
                </resources>
            </build>
        </profile>
        <profile>
            <id>test</id>
            <activation>
                <property>
                    <name>env</name>
                    <value>Test</value>
                </property>
            </activation>
            <build>
                <!--配置文件路径-->
                <resources>
                    <resource>
                        <directory>src/main/resources/test</directory>
                    </resource>
                </resources>
            </build>
        </profile>
        <profile>
            <id>prod</id>
            <activation>
                <property>
                    <name>env</name>
                    <value>Prod</value>
                </property>
            </activation>
            <build>
                <!--配置文件路径-->
                <resources>
                    <resource>
                        <directory>src/main/resources/prod</directory>
                    </resource>
                </resources>
            </build>
        </profile>
    </profiles>
    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>

        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.5.1</version>
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
        </plugins>
    </build>
</project>
```

## gen_files.py

```py
import os

for index in range(100):
    // 1. 文件内容
    content = """
    {"name": "Michael"}
    {"name": "Andy", "age": 30}
    {"name": "Justin", "age": 19}
    """

    // 2. 文件路径
    file_name = "/export/dataset/text{0}.json".format(index)

    // 3. 打开文件, 写入内容
    with open(file_name, "w") as file:
        file.write(content)

    // 4. 执行 HDFS 命令, 创建 HDFS 目录, 上传文件到 HDFS 中
    os.system("/export/servers/hadoop/bin/hdfs dfs -mkdir -p /dataset/dataset/")
    os.system("/export/servers/hadoop/bin/hdfs dfs -put {0} /dataset/dataset/".format(file_name))
```
