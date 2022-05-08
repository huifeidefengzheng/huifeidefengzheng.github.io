---
title: 03_SparkCoreInsight.md
date: 2019/9/5 08:16:25
updated: 2019/9/5 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - Spark
---





## 示例-03

### spark-warehouse

#### cn/xhchen/spark/rdd

##### AccessLogAgg.scala

spark/src/main/scala/cn/xhchen/spark/rdd/AccessLogAgg.scala

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

##### Accumulator.scala

spark/src/main/scala/cn/xhchen/spark/rdd/Accumulator.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2
import org.junit.Test

import scala.collection.mutable

class Accumulator {

  /**
    * RDD -> (1, 2, 3, 4, 5) -> Set(1,2,3,4,5)
    */
  @Test
  def acc(): Unit = {
    val config = new SparkConf().setAppName("acc").setMaster("local[6]")
    val sc = new SparkContext(config)

    val numAcc = new NumAccumulator()
    // 注册给 Spark
    sc.register(numAcc, "num")

    sc.parallelize(Seq("1", "2", "3"))
      .foreach(item => numAcc.add(item))

    println(numAcc.value)

    sc.stop()
  }
}

class NumAccumulator extends AccumulatorV2[String, Set[String]] {
  private val nums: mutable.Set[String] = mutable.Set()

  /**
    * 告诉 Spark 框架, 这个累加器对象是否是空的
    */
  override def isZero: Boolean =  {
    nums.isEmpty
  }

  /**
    * 提供给 Spark 框架一个拷贝的累加器
    * @return
    */
  override def copy(): AccumulatorV2[String, Set[String]] = {
    val newAccumulator = new NumAccumulator()
    nums.synchronized {
      newAccumulator.nums ++= this.nums
    }
    newAccumulator
  }

  /**
    * 帮助 Spark 框架, 清理累加器的内容
    */
  override def reset(): Unit = {
    nums.clear()
  }

  /**
    * 外部传入要累加的内容, 在这个方法中进行累加
    */
  override def add(v: String): Unit = {
    nums += v
  }

  /**
    * 累加器在进行累加的时候, 可能每个分布式节点都有一个实例
    * 在最后 Driver 进行一次合并, 把所有的实例的内容合并起来, 会调用这个 merge 方法进行合并
    */
  override def merge(other: AccumulatorV2[String, Set[String]]): Unit = {
    nums ++= other.value
  }

  /**
    * 提供给外部累加结果
    * 为什么一定要给不可变的, 因为外部有可能再进行修改, 如果是可变的集合, 其外部的修改会影响内部的值
    */
  override def value: Set[String] = {
    nums.toSet
  }
}
```

##### ActionOp.scala

spark/src/main/scala/cn/xhchen/spark/rdd/ActionOp.scala

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

##### Broadcast.scala

spark/src/main/scala/cn/xhchen/spark/rdd/Broadcast.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class Broadcast {

  /**
    * 资源占用比较大, 有十个对应的 value
    */
  @Test
  def bc1(): Unit = {
    // 数据, 假装这个数据很大, 大概一百兆
    val v = Map("Spark" -> "http://spark.apache.cn", "Scala" -> "http://www.scala-lang.org")

    val config = new SparkConf().setMaster("local[6]").setAppName("bc")
    val sc = new SparkContext(config)

    // 将其中的 Spark 和 Scala 转为对应的网址
    val r = sc.parallelize(Seq("Spark", "Scala"))
    val result = r.map(item => v(item)).collect()

    println(result)
  }

  /**
    * 使用广播, 大幅度减少 value 的复制
    */
  @Test
  def bc2(): Unit = {
    // 数据, 假装这个数据很大, 大概一百兆
    val v = Map("Spark" -> "http://spark.apache.cn", "Scala" -> "http://www.scala-lang.org")

    val config = new SparkConf().setMaster("local[6]").setAppName("bc")
    val sc = new SparkContext(config)

    // 创建广播
    val bc = sc.broadcast(v)

    // 将其中的 Spark 和 Scala 转为对应的网址
    val r = sc.parallelize(Seq("Spark", "Scala"))

    // 在算子中使用广播变量代替直接引用集合, 只会复制和executor一样的数量
    // 在使用广播之前, 复制 map 了 task 数量份
    // 在使用广播以后, 复制次数和 executor 数量一致
    val result = r.map(item => bc.value(item)).collect()

    result.foreach(println(_))
  }
}
```

##### CacheOp.scala

spark/src/main/scala/cn/xhchen/spark/rdd/CacheOp.scala

```scala
package cn.xhchen.spark.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
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

  @Test
  def cache(): Unit = {
    val conf = new SparkConf().setAppName("cache_prepare").setMaster("local[6]")
    val sc = new SparkContext(conf)

    // RDD 的处理部分
    val source = sc.textFile("dataset/access_log_sample.txt")
    val countRDD = source.map( item => (item.split(" ")(0), 1) )
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    var aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg )
    aggRDD = aggRDD.cache()

    // 两个 RDD 的 Action 操作
    // 每一个 Action 都会完整运行一下 RDD 的整个血统
    val lessIp = aggRDD.sortBy(item => item._2, ascending = true).first()
    val moreIp = aggRDD.sortBy(item => item._2, ascending = false).first()

    println((lessIp, moreIp))
  }

  @Test
  def persist(): Unit = {
    val conf = new SparkConf().setAppName("cache_prepare").setMaster("local[6]")
    val sc = new SparkContext(conf)

    // RDD 的处理部分
    val source = sc.textFile("dataset/access_log_sample.txt")
    val countRDD = source.map( item => (item.split(" ")(0), 1) )
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    var aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg )
    aggRDD = aggRDD.persist(StorageLevel.MEMORY_ONLY)
    println(aggRDD.getStorageLevel)

    // 两个 RDD 的 Action 操作
    // 每一个 Action 都会完整运行一下 RDD 的整个血统
//    val lessIp = aggRDD.sortBy(item => item._2, ascending = true).first()
//    val moreIp = aggRDD.sortBy(item => item._2, ascending = false).first()

//    println((lessIp, moreIp))
  }

  @Test
  def checkpoint(): Unit = {
    val conf = new SparkConf().setAppName("cache_prepare").setMaster("local[6]")
    val sc = new SparkContext(conf)
    // 设置保存 checkpoint 的目录, 也可以设置为 HDFS 上的目录
    sc.setCheckpointDir("checkpoint")

    // RDD 的处理部分
    val source = sc.textFile("dataset/access_log_sample.txt")
    val countRDD = source.map( item => (item.split(" ")(0), 1) )
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    var aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg )

    // checkpoint
    // aggRDD = aggRDD.cache
    // 不准确的说, Checkpoint 是一个 Action 操作, 也就是说
    // 如果调用 checkpoint, 则会重新计算一下 RDD, 然后把结果存在 HDFS 或者本地目录中
    // 所以, 应该在 Checkpoint 之前, 进行一次 Cache
    aggRDD = aggRDD.cache()

    aggRDD.checkpoint()

    // 两个 RDD 的 Action 操作
    // 每一个 Action 都会完整运行一下 RDD 的整个血统
    val lessIp = aggRDD.sortBy(item => item._2, ascending = true).first()
    val moreIp = aggRDD.sortBy(item => item._2, ascending = false).first()

    println((lessIp, moreIp))
  }
}
```

##### Closure.scala

spark/src/main/scala/cn/xhchen/spark/rdd/Closure.scala

```scala
package cn.xhchen.spark.rdd

import org.junit.Test

class Closure {

  /**
    * 编写一个高阶函数, 在这个函数内要有一个变量, 返回一个函数, 通过这个变量完成一个计算
    */
  @Test
  def test(): Unit = {
//    val f: Int => Double = closure()
//    val area = f(5)
//    println(area)

    // 在这能否访问到 Factor, 不能
    // 说明 Factor 在一个单独的作用域中

    // 在拿到 f 的时候, 可以通过 f 间接的访问到 closure 作用域中的内容
    // 说明 f 携带了一个作用域
    // 如果一个函数携带了一个外包的作用域, 这种函数我们称之为叫做闭包
    val f = closure()
    f(5)

    // 闭包的本质是什么?
    // f 就是闭包, 闭包的本质就是一个函数
    // 在 Scala 中函数是一个特殊的类型, FunctionX
    // 闭包也是一个 FunctionX 类型的对象
    // 闭包是一个对象
  }

  /**
    * 返回一个新的函数
    */
  def closure(): Int => Double = {
    val factor = 3.14
    val areaFunction = (r: Int) => {
      math.pow(r, 2) * factor
    }
    areaFunction
  }
}
```

##### SourceAnalysis.scala

spark/src/main/scala/cn/xhchen/spark/rdd/SourceAnalysis.scala

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

spark/src/main/scala/cn/xhchen/spark/rdd/StagePractice.scala

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

spark/src/main/scala/cn/xhchen/spark/rdd/TransformationOp.scala

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

spark/src/main/scala/cn/xhchen/spark/rdd/WordCount.scala

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

##### AggProcessor.scala

spark/src/main/scala/cn/xhchen/spark/sql/AggProcessor.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.junit.Test

class AggProcessor {
  // 1. 创建 SparkSession
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("agg processor")
    .getOrCreate()

  import spark.implicits._

  @Test
  def groupBy(): Unit = {
    // 2. 数据读取
    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val sourceDF = spark.read
      .schema(schema)
      .option("header", value = true)
      .csv("dataset/beijingpm_with_nan.csv")

    // 3. 数据去掉空值
    val cleanDF = sourceDF.where('pm =!= Double.NaN)

    // 分组
    val groupedDF: RelationalGroupedDataset = cleanDF.groupBy('year, $"month")

    // 4. 使用 functions 函数来完成聚合
    import org.apache.spark.sql.functions._

    // 本质上, avg 这个函数定义了一个操作, 把表达式设置给 pm 列
    // select avg(pm) from ... group by
    groupedDF.agg(avg('pm) as "pm_avg")
      .orderBy('pm_avg.desc)
      .show()

    groupedDF.agg(stddev(""))
      .orderBy('pm_avg.desc)
      .show()

    // 5. 使用 GroupedDataset 的 API 来完成聚合
    groupedDF.avg("pm")
      .select($"avg(pm)" as "pm_avg")
      .orderBy("pm_avg")
      .show()

    groupedDF.sum()
      .select($"avg(pm)" as "pm_avg")
      .orderBy("pm_avg")
      .show()
  }

  @Test
  def multiAgg(): Unit = {
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    import org.apache.spark.sql.functions._

    // 需求1: 不同年, 不同来源, PM 值的平均数
    // select source, year, avg(pm) as pm from ... group by source, year
    val postAndYearDF = pmFinal.groupBy('source, 'year)
      .agg(avg('pm) as "pm")

    // 需求2: 在整个数据集中, 按照不同的来源来统计 PM 值的平均数
    // select source, avg(pm) as pm from ... group by source
    val postDF = pmFinal.groupBy('source)
      .agg(avg('pm) as "pm")
      .select('source, lit(null) as "year", 'pm)

    // 合并在同一个结果集中
    postAndYearDF.union(postDF)
      .sort('source, 'year.asc_nulls_last, 'pm)
      .show()
  }

  @Test
  def rollup(): Unit = {
    import org.apache.spark.sql.functions._

    val sales = Seq(
      ("Beijing", 2016, 100),
      ("Beijing", 2017, 200),
      ("Shanghai", 2015, 50),
      ("Shanghai", 2016, 150),
      ("Guangzhou", 2017, 50)
    ).toDF("city", "year", "amount")

    // 滚动分组, A, B 两列, AB, A, null
    sales.rollup('city, 'year)
      .agg(sum('amount) as "amount")
      .sort('city.asc_nulls_last, 'year.asc_nulls_last)
      .show()
  }

  @Test
  def rollup1(): Unit = {
    import org.apache.spark.sql.functions._

    // 1. 数据集读取
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    // 2. 聚合和统计
    // 需求1: 每个PM值计量者, 每年PM值统计的平均数 groupby source year
    // 需求2: 每个PM值计量者, 整体上的PM平均值 groupby source
    // 需求3: 全局所有的计量者, 和日期的PM值的平均值 groupby null
    pmFinal.rollup('source, 'year)
      .agg(avg('pm) as "pm")
      .sort('source.asc_nulls_last, 'year.asc_nulls_last)
      .show()
  }

  @Test
  def cube(): Unit = {
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    import org.apache.spark.sql.functions._

    pmFinal.cube('source, 'year)
      .agg(avg('pm) as "pm")
      .sort('source.asc_nulls_last, 'year.asc_nulls_last)
      .show()
  }

  @Test
  def cubeSql(): Unit = {
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    pmFinal.createOrReplaceTempView("pm_final")

    val result = spark.sql("select source, year, avg(pm) as pm from pm_final group by source, year " +
      "grouping sets ((source, year), (source), (year), ())" +
      "order by source asc nulls last, year asc nulls last")

    result.show()
  }

}
```

##### Column.scala

spark/src/main/scala/cn/xhchen/spark/sql/Column.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql
import org.apache.spark.sql.{ColumnName, DataFrame, Dataset, SparkSession}
import org.junit.Test

class Column {
  // 1. 创建 spark 对象
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("column")
    .getOrCreate()

  import spark.implicits._

  @Test
  def creation(): Unit = {
    val ds: Dataset[Person] = Seq(Person("zhangsan", 15), Person("lisi", 10)).toDS()
    val ds1: Dataset[Person] = Seq(Person("zhangsan", 15), Person("lisi", 10)).toDS()
    val df: DataFrame = Seq(("zhangsan", 15), ("lisi", 10)).toDF("name", "age")

    // 2. ' 必须导入spark的隐式转换才能使用 str.intern()
    val column: Symbol = 'name

    // 3. $ 必须导入spark的隐式转换才能使用
    val column1: ColumnName = $"name"

    // 4. col 必须导入 functions
    import org.apache.spark.sql.functions._

    val column2: sql.Column = col("name")

    // 5. column 必须导入 functions
    val column3: sql.Column = column("name")

    // 这四种创建方式, 有关联的 Dataset 吗?

    ds.select(column).show()

    // Dataset 可以, DataFrame 可以使用 Column 对象选中行吗?
    df.select(column).show()

    // select 方法可以使用 column 对象来选中某个列, 那么其他的算子行吗?
    df.where(column === "zhangsan").show()

    // column 有几个创建方式, 四种
    // column 对象可以用作于 Dataset 和 DataFrame 中
    // column 可以和命令式的弱类型的 API 配合使用 select where

    // 6. dataset.col
    // 使用 dataset 来获取 column 对象, 会和某个 Dataset 进行绑定, 在逻辑计划中, 就会有不同的表现
    val column4: sql.Column = ds.col("name")
    val column5: sql.Column = ds1.col("name")

    // 这会报错
//    ds.select(column5).show()

    // 为什么要和 dataset 来绑定呢?
//    ds.join(ds1, ds.col("name") === ds1.col("name"))

    // 7. dataset.apply
    val column6: sql.Column = ds.apply("name")
    val column7: sql.Column = ds("name")
  }

  @Test
  def as(): Unit = {
    val ds: Dataset[Person] = Seq(Person("zhangsan", 15), Person("lisi", 10)).toDS()

    // select name, count(age) as age from table group by name
    ds.select('name as "new_name").show()

    ds.select('age.as[Long]).show()
  }

  @Test
  def api(): Unit = {
    val ds: Dataset[Person] = Seq(Person("zhangsan", 15), Person("lisi", 10)).toDS()

    // 需求一, ds 增加列, 双倍年龄
    // 'age * 2 其实本质上就是将一个表达式(逻辑计划表达式) 附着到 column 对象上
    // 表达式在执行的时候对应每一条数据进行操作
    ds.withColumn("doubled", 'age * 2).show()

    // 需求二, 模糊查询
    // select * from table where name like zhang%
    ds.where('name like "zhang%").show()

    // 需求三, 排序, 正反序
    ds.sort('age asc).show()

    // 需求四, 枚举判断
    ds.where('name isin ("zhangsan", "wangwu", "zhaoliu")).show()
  }
}
```

##### HiveAccess.scala

spark/src/main/scala/cn/xhchen/spark/sql/HiveAccess.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object HiveAccess {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    //    1. 开启 Hive 支持
    //    2. 指定 Metastore 的位置
    //    3. 指定 Warehouse 的位置
    val spark = SparkSession.builder()
      .appName("hive access1")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://node01:9083")
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
      .csv("hdfs:///dataset/studenttab10k")

    val resultDF = dataframe.where('age > 50)

    // 3. 写入数据, 使用写入表的 API, saveAsTable
    resultDF.write.mode(SaveMode.Overwrite).saveAsTable("spark03.student")
  }
}
```

##### Intro.scala

spark/src/main/scala/cn/xhchen/spark/sql/Intro.scala

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

##### JoinProcessor.scala

spark/src/main/scala/cn/xhchen/spark/sql/JoinProcessor.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class JoinProcessor {
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("join")
    .getOrCreate()

  import spark.implicits._

  private val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3))
    .toDF("id", "name", "cityId")
  person.createOrReplaceTempView("person")

  private val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
    .toDF("id", "name")
  cities.createOrReplaceTempView("cities")

  @Test
  def introJoin(): Unit = {
    val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
      .toDF("id", "name", "cityId")

    val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
      .toDF("id", "name")

    val df = person.join(cities, person.col("cityId") === cities.col("id"))
      .select(person.col("id"),
        person.col("name"),
        cities.col("name") as "city")
//      .show()
    df.createOrReplaceTempView("user_city")

    spark.sql("select id, name, city from user_city where city = 'Beijing'")
      .show()
  }

  @Test
  def crossJoin(): Unit = {
    person.crossJoin(cities)
      .where(person.col("cityId") === cities.col("id"))
      .show()

    spark.sql("select u.id, u.name, c.name from person u cross join cities c " +
      "where u.cityId = c.id")
      .show()
  }

  @Test
  def inner(): Unit = {
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "inner")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p inner join cities c on p.cityId = c.id")
      .show()
  }

  @Test
  def fullOuter(): Unit = {
    // 内连接, 就是只显示能连接上的数据, 外连接包含一部分没有连接上的数据, 全外连接, 指左右两边没有连接上的数据, 都显示出来
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "full")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p full outer join cities c " +
      "on p.cityId = c.id")
      .show()
  }

  @Test
  def leftRight(): Unit = {
    // 左连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "left")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p left join cities c " +
      "on p.cityId = c.id")
      .show()

    // 右连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "right")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p right join cities c " +
      "on p.cityId = c.id")
      .show()
  }

  @Test
  def leftAntiSemi(): Unit = {
    // 左连接 anti
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftanti")
      .show()

    spark.sql("select p.id, p.name " +
      "from person p left anti join cities c " +
      "on p.cityId = c.id")
      .show()

    // 右连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftsemi")
      .show()

    spark.sql("select p.id, p.name " +
      "from person p left semi join cities c " +
      "on p.cityId = c.id")
      .show()
  }
}
```

##### MySQLWrite.scala

spark/src/main/scala/cn/xhchen/spark/sql/MySQLWrite.scala

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

##### NullProcessor.scala

spark/src/main/scala/cn/xhchen/spark/sql/NullProcessor.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.junit.Test

class NullProcessor {
  // 1. 创建 SparkSession
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("null processor")
    .getOrCreate()

  @Test
  def nullAndNaN(): Unit = {


    // 2. 导入数据集

    // 3. 读取数据集
    //    1. 通过Saprk-csv自动的推断类型来读取, 推断数字的时候会将 NaN 推断为 字符串
//    spark.read
//      .option("header", true)
//      .option("inferSchema", true)
//      .csv(...)
    //    2. 直接读取字符串, 在后续的操作中使用 map 算子转类型
//    spark.read.csv().map( row => row... )
    //    3. 指定 Schema, 不要自动推断
    val schema = StructType(
      List(
        StructField("id", LongType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val sourceDF = spark.read
      .option("header", value = true)
      .schema(schema)
      .csv("dataset/beijingpm_with_nan.csv")

    sourceDF.show()

    // 4. 丢弃
    // 2019, 12, 12, NaN
    // 规则:
    //      1. any, 只有有一个 NaN 就丢弃
    sourceDF.na.drop("any").show()
    sourceDF.na.drop().show()
    //      2. all, 所有数据都是 NaN 的行才丢弃
    sourceDF.na.drop("all").show()
    //      3. 某些列的规则
    sourceDF.na.drop("any", List("year", "month", "day", "hour")).show()

    // 5. 填充
    // 规则:
    //     1. 针对所有列数据进行默认值填充
    sourceDF.na.fill(0).show()
    //     2. 针对特定列填充
    sourceDF.na.fill(0, List("year", "month")).show()
  }

  @Test
  def strProcessor(): Unit = {
    // 读取数据集
    val sourceDF = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

//    sourceDF.show()

    // 1. 丢弃
    import spark.implicits._
//    sourceDF.where('PM_Dongsi =!= "NA").show()

    // 2. 替换
    import org.apache.spark.sql.functions._
    // select name, age, case
    // when ... then ...
    // when ... then ...
    // else
    sourceDF.select(
      'No as "id", 'year, 'month, 'day, 'hour, 'season,
      when('PM_Dongsi === "NA", Double.NaN)
        .otherwise('PM_Dongsi cast DoubleType)
        .as("pm")
    ).show()

    // 原类型和转换过后的类型, 必须一致
    sourceDF.na.replace("PM_Dongsi", Map("NA" -> "NaN", "NULL" -> "null")).show()
  }

}
```

##### ReadWrite.scala

spark/src/main/scala/cn/xhchen/spark/sql/ReadWrite.scala

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

##### TypedTransformation.scala

spark/src/main/scala/cn/xhchen/spark/sql/TypedTransformation.scala

```scala
package cn.xhchen.spark.sql

import java.lang

import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.junit.Test

class TypedTransformation {
  // 1. 创建 SparkSession
  val spark = SparkSession.builder().master("local[6]").appName("typed").getOrCreate()
  import spark.implicits._

  @Test
  def trans(): Unit = {
    // 3. flatMap
    val ds1 = Seq("hello spark", "hello hadoop").toDS
    ds1.flatMap( item => item.split(" ") ).show()

    // 4. map
    val ds2 = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDS()
    ds2.map(person => Person(person.name, person.age * 2)).show()

    // 5. mapPartitions
    ds2.mapPartitions(
      // iter 不能大到每个 Executor 的内存放不下, 不然就会 OOM
      // 对每个元素进行转换, 后生成一个新的集合
      iter => {
        val result = iter.map(person => Person(person.name, person.age * 2))
        result
      }
    ).show()
  }

  @Test
  def trans1(): Unit = {
    val ds = spark.range(10)
    ds.transform(dataset => dataset.withColumn("doubled", 'id * 2))
      .show()
  }

  @Test
  def as(): Unit = {
    // 1. 读取
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val df: DataFrame = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv("dataset/studenttab10k")

    // 2. 转换
    // 本质上: Dataset[Row].as[Student] => Dataset[Student]
    // Dataset[(String, int, float)].as[Student] => Dataset[Student]
    val ds: Dataset[Student] = df.as[Student]

    // 3. 输出
    ds.show()
  }

  @Test
  def filter(): Unit = {
    val ds = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDS()
    ds.filter( person => person.age > 15 ).show()
  }

  @Test
  def groupByKey(): Unit = {
    val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 16), Person("lisi", 20)).toDS()

    // select count(*) from person group by name
    val grouped: KeyValueGroupedDataset[String, Person] = ds.groupByKey(person => person.name)
    val result: Dataset[(String, Long)] = grouped.count()

    result.show()
  }

  @Test
  def split(): Unit = {
    val ds = spark.range(15)
    // randomSplit, 切多少份, 权重多少
    val datasets: Array[Dataset[lang.Long]] = ds.randomSplit(Array(5, 2, 3))
    datasets.foreach(_.show())

    // sample
    ds.sample(withReplacement = false, fraction = 0.4).show()
  }

  @Test
  def sort(): Unit = {
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
    ds.orderBy('age.desc).show() // select * from ... order by ... asc
    ds.sort('age.asc).show()
  }

  @Test
  def dropDuplicates(): Unit = {
    val ds = spark.createDataset(Seq(Person("zhangsan", 15), Person("zhangsan", 15), Person("lisi", 15)))
    ds.distinct().show()
    ds.dropDuplicates("age").show()
  }

  @Test
  def collection(): Unit = {
    val ds1 = spark.range(1, 10)
    val ds2 = spark.range(5, 15)

    // 差集
    ds1.except(ds2).show()

    // 交集
    ds1.intersect(ds2).show()

    // 并集
    ds1.union(ds2).show()

    // limit
    ds1.limit(3).show()
  }

}

case class Student(name: String, age: Int, gpa: Float)
```

##### UntypedTransformation.scala

spark/src/main/scala/cn/xhchen/spark/sql/UntypedTransformation.scala

```scala
package cn.xhchen.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class UntypedTransformation {
  val spark = SparkSession.builder().master("local[6]").appName("typed").getOrCreate()
  import spark.implicits._

  @Test
  def select(): Unit = {
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 8)).toDS

    // select * from ...
    // from ... select ...
    // 在 Dataset 中, select 可以在任何位置调用
    // select count(*)
    ds.select('name).show()

    ds.selectExpr("sum(age)").show()

    import org.apache.spark.sql.functions._

    ds.select(expr("sum(age)")).show()
  }

  @Test
  def column(): Unit = {
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 8)).toDS

    import org.apache.spark.sql.functions._

    // select rand() from ...
    // 如果想使用函数功能
    // 1. 使用 functions.xx
    // 2. 使用表达式, 可以使用 expr("...") 随时随地编写表达式
    ds.withColumn("random", expr("rand()")).show()

    ds.withColumn("name_new", 'name).show()

    ds.withColumn("name_jok", 'name === "").show()

    ds.withColumnRenamed("name", "new_name").show()
  }

  @Test
  def groupBy(): Unit = {
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

    // 为什么 GroupByKey 是有类型的, 最主要的原因是因为 groupByKey 所生成的对象中的算子是有类型的
//    ds.groupByKey( item => item.name ).mapValues()

    // 为什么  GroupBy 是无类型的, 因为 groupBy 所生成的对象中的算子是无类型的, 针对列进行处理的
    import org.apache.spark.sql.functions._

    ds.groupBy('name).agg(mean("age")).show()
  }
}
```

##### dependency-reduced-pom.xml

spark/dependency-reduced-pom.xml

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

spark/pom.xml

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
