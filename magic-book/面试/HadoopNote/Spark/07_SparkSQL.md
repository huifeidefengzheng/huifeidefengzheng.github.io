---
title: 07_SparkSQL.md
date: 2019/9/5 08:16:25
updated: 2019/9/5 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - Spark
---


### 8. Dataset (DataFrame) 的基础操作

.导读
这一章节主要目的是介绍 `Dataset` 的基础操作, 当然, `DataFrame` 就是 `Dataset`, 所以这些操作大部分也适用于 `DataFrame`

. 有类型的转换操作
. 无类型的转换操作
. 基础 `Action`
. 空值如何处理
. 统计操作

#### 8.1. 有类型操作-TypedTransformation.scala

| 分类 | 算子 | 解释

##### 转换->flatMap

通过 `flatMap` 可以将一条数据转为一个数组, 后再展开这个数组放入 `Dataset`

```scala
import spark.implicits._
val ds = Seq("hello world", "hello pc").toDS()
ds.flatMap( _.split(" ") ).show()
```

##### 转换

###### map

`map` 可以将数据集中每条数据转为另一种形式

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 15), Person("lisi", 15)).toDS()
ds.map( person => Person(person.name, person.age * 2) ).show()
```

###### mapPartitions

`mapPartitions` 和 `map` 一样, 但是 `map` 的处理单位是每条数据, `mapPartitions` 的处理单位是每个分区

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 15), Person("lisi", 15)).toDS()
ds.mapPartitions( iter => {
    val returnValue = iter.map(
      item => Person(item.name, item.age * 2)
    )
    returnValue
  } )
  .show()
```

###### transform

`map` 和 `mapPartitions` 以及 `transform` 都是转换, `map` 和 `mapPartitions` 是针对数据, 而 `transform` 是针对整个数据集, 这种方式最大的区别就是 `transform` 可以直接拿到 `Dataset` 进行操作

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190526111401.png)

```scala
import spark.implicits._
val ds = spark.range(5)
ds.transform( dataset => dataset.withColumn("doubled", 'id * 2) )
```

###### as

`as[Type]` 算子的主要作用是将弱类型的 `Dataset` 转为强类型的 `Dataset`, 它有很多适用场景, 但是最常见的还是在读取数据的时候, 因为 `DataFrameReader` 体系大部分情况下是将读出来的数据转换为 `DataFrame` 的形式, 如果后续需要使用 `Dataset` 的强类型 `API`, 则需要将 `DataFrame` 转为 `Dataset`. 可以使用 `as[Type]` 算子完成这种操作

```scala
import spark.implicits._

val structType = StructType(
  Seq(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("gpa", FloatType)
  )
)

val sourceDF = spark.read
  .schema(structType)
  .option("delimiter", "\t")
  .csv("dataset/studenttab10k")

val dataset = sourceDF.as[Student]
dataset.show()
```

##### 过滤

###### filter

`filter` 用来按照条件过滤数据集

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 15), Person("lisi", 15)).toDS()
//    val ds = spark.range(5)
ds.filter( person => person.name ### "lisi" ).show()
```

##### 聚合

###### groupByKey

`grouByKey` 算子的返回结果是 `KeyValueGroupedDataset`, 而不是一个 `Dataset`, 所以必须要先经过 `KeyValueGroupedDataset` 中的方法进行聚合, 再转回 `Dataset`, 才能使用 `Action` 得出结果

其实这也印证了分组后必须聚合的道理

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 15), Person("lisi", 15)).toDS()
ds.groupByKey( person => person.name ).count().show()
```

##### 切分

###### randomSplit

`randomSplit` 会按照传入的权重随机将一个 `Dataset` 分为多个 `Dataset`, 传入 `randomSplit` 的数组有多少个权重, 最终就会生成多少个 `Dataset`, 这些权重的加倍和应该为 1, 否则将被标准化

```scala
val ds = spark.range(15)
val datasets: Array[Dataset[lang.Long]] = ds.randomSplit(Array[Double](2, 3))
datasets.foreach(dataset => dataset.show())
```

###### sample

`sample` 会随机在 `Dataset` 中抽样

```scala
val ds = spark.range(15)
ds.sample(withReplacement = false, fraction = 0.4).show()
```

##### 排序

###### orderBy

`orderBy` 配合 `Column` 的 `API`, 可以实现正反序排列

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.orderBy("age").show()
ds.orderBy('age.desc).show()
```

###### sort

其实 `orderBy` 是 `sort` 的别名, 所以它们所实现的功能是一样的

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.sort('age.desc).show()
```

##### 分区

###### coalesce

减少分区, 此算子和 `RDD` 中的 `coalesce` 不同, `Dataset` 中的 `coalesce` 只能减少分区数, `coalesce` 会直接创建一个逻辑操作, 并且设置 `Shuffle` 为 `false`

```scala
val ds = spark.range(15)
ds.coalesce(1).explain(true)
```

###### repartitions

`repartitions` 有两个作用, 一个是重分区到特定的分区数, 另一个是按照某一列来分区, 类似于 `SQL` 中的 `DISTRIBUTE BY`

```scala
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.repartition(4)
ds.repartition('name)
```

##### 去重

###### dropDuplicates`

使用 `dropDuplicates` 可以去掉某一些列中重复的行

```scala
import spark.implicits._
val ds = spark.createDataset(Seq(Person("zhangsan", 15), Person("zhangsan", 15), Person("lisi", 15)))
ds.dropDuplicates("age").show()
```

###### distinct

当 `dropDuplicates` 中没有传入列名的时候, 其含义是根据所有列去重, `dropDuplicates()` 方法还有一个别名, 叫做 `distinct`

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190525182912.png)

所以, 使用 `distinct` 也可以去重, 并且只能根据所有的列来去重

```scala
import spark.implicits._
val ds = spark.createDataset(Seq(Person("zhangsan", 15), Person("zhangsan", 15), Person("lisi", 15)))
ds.distinct().show()
```

##### 集合操作

###### except

`except` 和 `SQL` 语句中的 `except` 一个意思, 是求得 `ds1` 中不存在于 `ds2` 中的数据, 其实就是差集

```scala
val ds1 = spark.range(1, 10)
val ds2 = spark.range(5, 15)

ds1.except(ds2).show()
```

###### intersect

求得两个集合的交集

```scala
val ds1 = spark.range(1, 10)
val ds2 = spark.range(5, 15)

ds1.intersect(ds2).show()
```

###### union

求得两个集合的并集

```scala
val ds1 = spark.range(1, 10)
val ds2 = spark.range(5, 15)

ds1.union(ds2).show()
```

###### limit

限制结果集数量

```scala
val ds = spark.range(1, 10)
ds.limit(3).show()
```

#### 8.2. 无类型转换-UntypedTransformation.scala

##### 选择

###### select

`select` 用来选择某些列出现在结果集中

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.select($"name").show()
```

###### selectExpr

在 `SQL` 语句中, 经常可以在 `select` 子句中使用 `count(age)`, `rand()` 等函数, 在 `selectExpr` 中就可以使用这样的 `SQL` 表达式, 同时使用 `select` 配合 `expr` 函数也可以做到类似的效果

```scala
import spark.implicits._
import org.apache.spark.sql.functions._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.selectExpr("count(age) as count").show()
ds.selectExpr("rand() as random").show()
ds.select(expr("count(age) as count")).show()
```

###### withColumn

通过 `Column` 对象在 `Dataset` 中创建一个新的列或者修改原来的列

```scala
import spark.implicits._
import org.apache.spark.sql.functions._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.withColumn("random", expr("rand()")).show()
```

###### withColumnRenamed

修改列名

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.withColumnRenamed("name", "new_name").show()
```

##### 剪除

###### drop

剪掉某个列

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.drop('age).show()
```

##### 聚合·

###### groupBy

按照给定的行进行分组

```scala
import spark.implicits._
val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
ds.groupBy('name).count().show()
```

#### 8.5. Column 对象-Column.scala

.导读
Column 表示了 Dataset 中的一个列, 并且可以持有一个表达式, 这个表达式作用于每一条数据, 对每条数据都生成一个值, 之所以有单独这样的一个章节是因为列的操作属于细节, 但是又比较常见, 会在很多算子中配合出现

[cols="15h,15h,~"]
|===
| 分类 | 操作 | 解释

.6+| 创建 | `'` a|
单引号 `'` 在 Scala 中是一个特殊的符号, 通过 `'` 会生成一个 `Symbol` 对象, `Symbol` 对象可以理解为是一个字符串的变种, 但是比字符串的效率高很多, 在 `Spark` 中, 对 `Scala` 中的 `Symbol` 对象做了隐式转换, 转换为一个 `ColumnName` 对象, `ColumnName` 是 `Column` 的子类, 所以在 `Spark` 中可以如下去选中一个列

```scala
val spark = SparkSession.builder().appName("column").master("local[6]").getOrCreate()
import spark.implicits._
val personDF = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

val c1: Symbol = 'name
```

| `$` a|
同理, `$` 符号也是一个隐式转换, 同样通过 `spark.implicits` 导入, 通过 `$` 可以生成一个 `Column` 对象

```scala
val spark = SparkSession.builder().appName("column").master("local[6]").getOrCreate()
import spark.implicits._
val personDF = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

val c2: ColumnName = $"name"
```

| `col` a|
`SparkSQL` 提供了一系列的函数, 可以通过函数实现很多功能, 在后面课程中会进行详细介绍, 这些函数中有两个可以帮助我们创建 `Column` 对象, 一个是 `col`, 另外一个是 `column`

```scala
val spark = SparkSession.builder().appName("column").master("local[6]").getOrCreate()
import org.apache.spark.sql.functions._
val personDF = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

val c3: sql.Column = col("name")
```

| `column` a|

```scala
val spark = SparkSession.builder().appName("column").master("local[6]").getOrCreate()
import org.apache.spark.sql.functions._
val personDF = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

val c4: sql.Column = column("name")
```

| `Dataset.col` a|
前面的 `Column` 对象创建方式所创建的 `Column` 对象都是 `Free` 的, 也就是没有绑定任何 `Dataset`, 所以可以作用于任何 `Dataset`, 同时, 也可以通过 `Dataset` 的 `col` 方法选择一个列, 但是这个 `Column` 是绑定了这个 `Dataset` 的, 所以只能用于创建其的 `Dataset` 上

```scala
val spark = SparkSession.builder().appName("column").master("local[6]").getOrCreate()
val personDF = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

val c5: sql.Column = personDF.col("name")
```

| `Dataset.apply` a|
可以通过 `Dataset` 对象的 `apply` 方法来获取一个关联此 `Dataset` 的 `Column` 对象

```scala
val spark = SparkSession.builder().appName("column").master("local[6]").getOrCreate()
val personDF = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

val c6: sql.Column = personDF.apply("name")
```

`apply` 的调用有一个简写形式

```scala
val c7: sql.Column = personDF("name")
```

.2+| 别名和转换 | `as[Type]` a|
`as` 方法有两个用法, 通过 `as[Type]` 的形式可以将一个列中数据的类型转为 `Type` 类型

```scala
personDF.select(col("age").as[Long]).show()
```

| `as(name)` a|
通过 `as(name)` 的形式使用 `as` 方法可以为列创建别名

```scala
personDF.select(col("age").as("age_new")).show()
```

| 添加列 | `withColumn` a|
通过 `Column` 在添加一个新的列时候修改 `Column` 所代表的列的数据

```scala
personDF.withColumn("double_age", 'age * 2).show()
```

.3+| 操作 | `like` a|
通过 `Column` 的 `API`, 可以轻松实现 `SQL` 语句中 `LIKE` 的功能

```scala
personDF.filter('name like "%zhang%").show()
```

| `isin` a|
通过 `Column` 的 `API`, 可以轻松实现 `SQL` 语句中 `ISIN` 的功能

```scala
personDF.filter('name isin ("hello", "zhangsan")).show()
```

| `sort` a|
在排序的时候, 可以通过 `Column` 的 `API` 实现正反序

```scala
personDF.sort('age.asc).show()
personDF.sort('age.desc).show()
```

### 9. 缺失值处理-NullProcessor.scala

.导读
`DataFrame` 中什么时候会有无效值
. `DataFrame` 如何处理无效的值
. `DataFrame` 如何处理 `null`

缺失值的处理思路::
如果想探究如何处理无效值, 首先要知道无效值从哪来, 从而分析可能产生的无效值有哪些类型, 在分别去看如何处理无效值

什么是缺失值::
+
一个值本身的含义是这个值不存在则称之为缺失值, 也就是说这个值本身代表着缺失, 或者这个值本身无意义, 比如说 `null`, 比如说空字符串

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190527220736.png)

关于数据的分析其实就是统计分析的概念, 如果这样的话, 当数据集中存在缺失值, 则无法进行统计和分析, 对很多操作都有影响

缺失值如何产生的::
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190527215718.png)

Spark 大多时候处理的数据来自于业务系统中, 业务系统中可能会因为各种原因, 产生一些异常的数据

例如说因为前后端的判断失误, 提交了一些非法参数. 再例如说因为业务系统修改 `MySQL` 表结构产生的一些空值数据等. 总之在业务系统中出现缺失值其实是非常常见的一件事, 所以大数据系统就一定要考虑这件事.

缺失值的类型::
+
常见的缺失值有两种

* `null`, `NaN` 等特殊类型的值, 某些语言中 `null` 可以理解是一个对象, 但是代表没有对象, `NaN` 是一个数字, 可以代表不是数字

针对这一类的缺失值, `Spark` 提供了一个名为 `DataFrameNaFunctions` 特殊类型来操作和处理

* `"Null"`, `"NA"`, `" "` 等解析为字符串的类型, 但是其实并不是常规字符串数据

针对这类字符串, 需要对数据集进行采样, 观察异常数据, 总结经验, 各个击破

`DataFrameNaFunctions`::
`DataFrameNaFunctions` 使用 `Dataset` 的 `na` 函数来获取

```scala
val df = ...
val naFunc: DataFrameNaFunctions = df.na
```

当数据集中出现缺失值的时候, 大致有两种处理方式, 一个是丢弃, 一个是替换为某值, `DataFrameNaFunctions` 中包含一系列针对空值数据的方案

* `DataFrameNaFunctions.drop` 可以在当某行中包含 `null` 或 `NaN` 的时候丢弃此行
* `DataFrameNaFunctions.fill` 可以在将 `null` 和 `NaN` 充为其它值
* `DataFrameNaFunctions.replace` 可以把 `null` 或 `NaN`  替换为其它值, 但是和 `fill` 略有一些不同, 这个方法针对值来进行替换

如何使用 `SparkSQL` 处理 `null` 和 `NaN` ?::

首先要将数据读取出来, 此次使用的数据集直接存在 `NaN`, 在指定 `Schema` 后, 可直接被转为 `Double.NaN`

```scala
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

val df = spark.read
  .option("header", value = true)
  .schema(schema)
  .csv("dataset/beijingpm_with_nan.csv")
```

对于缺失值的处理一般就是丢弃和填充

丢弃包含 `null` 和 `NaN` 的行::
+
当某行数据所有值都是 `null` 或者 `NaN` 的时候丢弃此行

```scala
df.na.drop("all").show()
```

当某行中特定列所有值都是 `null` 或者 `NaN` 的时候丢弃此行

```scala
df.na.drop("all", List("pm", "id")).show()
```

当某行数据任意一个字段为 `null` 或者 `NaN` 的时候丢弃此行

```scala
df.na.drop().show()
df.na.drop("any").show()
```

当某行中特定列任意一个字段为 `null` 或者 `NaN` 的时候丢弃此行

```scala
df.na.drop(List("pm", "id")).show()
df.na.drop("any", List("pm", "id")).show()
```

填充包含 `null` 和 `NaN` 的列::
+
填充所有包含 `null` 和 `NaN` 的列

```scala
df.na.fill(0).show()
```

填充特定包含 `null` 和 `NaN` 的列

```scala
df.na.fill(0, List("pm")).show()
```

根据包含 `null` 和 `NaN` 的列的不同来填充

```scala
import scala.collection.JavaConverters._

df.na.fill(Map[String, Any]("pm" -> 0).asJava).show
```

如何使用 `SparkSQL` 处理异常字符串 ?::

读取数据集, 这次读取的是最原始的那个 `PM` 数据集

```scala
val df = spark.read
  .option("header", value = true)
  .csv("dataset/BeijingPM20100101_20151231.csv")
```

使用函数直接转换非法的字符串

```scala
df.select('No as "id", 'year, 'month, 'day, 'hour, 'season,
    when('PM_Dongsi #### "NA", 0)
    .otherwise('PM_Dongsi cast DoubleType)
    .as("pm"))
  .show()
```

使用 `where` 直接过滤

```scala
df.select('No as "id", 'year, 'month, 'day, 'hour, 'season, 'PM_Dongsi)
  .where('PM_Dongsi =!= "NA")
  .show()
```

使用 `DataFrameNaFunctions` 替换, 但是这种方式被替换的值和新值必须是同类型

```scala
df.select('No as "id", 'year, 'month, 'day, 'hour, 'season, 'PM_Dongsi)
  .na.replace("PM_Dongsi", Map("NA" -> "NaN"))
  .show()
```

### 10. 聚合

.导读
`groupBy`
. `rollup`
. `cube`
. `pivot`
. `RelationalGroupedDataset` 上的聚合操作

`groupBy`::
`groupBy` 算子会按照列将 `Dataset` 分组, 并返回一个 `RelationalGroupedDataset` 对象, 通过 `RelationalGroupedDataset` 可以对分组进行聚合

Step 1: 加载实验数据::

```scala
private val spark = SparkSession.builder()
    .master("local[6]")
    .appName("aggregation")
    .getOrCreate()

  import spark.implicits._

  private val schema = StructType(
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

  private val pmDF = spark.read
    .schema(schema)
    .option("header", value = true)
    .csv("dataset/pm_without_null.csv")
```

Step 2: 使用 `functions` 函数进行聚合::

```scala
import org.apache.spark.sql.functions._

val groupedDF: RelationalGroupedDataset = pmDF.groupBy('year)

groupedDF.agg(avg('pm) as "pm_avg")
  .orderBy('pm_avg)
  .show()
```

Step 3: 除了使用 `functions` 进行聚合, 还可以直接使用 `RelationalGroupedDataset` 的 `API` 进行聚合::

```scala
groupedDF.avg("pm")
  .orderBy('pm_avg)
  .show()

groupedDF.max("pm")
  .orderBy('pm_avg)
  .show()
```

多维聚合::
我们可能经常需要针对数据进行多维的聚合, 也就是一次性统计小计, 总计等, 一般的思路如下

Step 1: 准备数据::

```scala
private val spark = SparkSession.builder()
  .master("local[6]")
  .appName("aggregation")
  .getOrCreate()

import spark.implicits._

private val schemaFinal = StructType(
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

private val pmFinal = spark.read
  .schema(schemaFinal)
  .option("header", value = true)
  .csv("dataset/pm_final.csv")
```

Step 2: 进行多维度聚合::

```scala
import org.apache.spark.sql.functions._

val groupPostAndYear = pmFinal.groupBy('source, 'year)
  .agg(sum("pm") as "pm")

val groupPost = pmFinal.groupBy('source)
  .agg(sum("pm") as "pm")
  .select('source, lit(null) as "year", 'pm)

groupPostAndYear.union(groupPost)
  .sort('source, 'year asc_nulls_last, 'pm)
  .show()
```

大家其实也能看出来, 在一个数据集中又小计又总计, 可能需要多个操作符, 如何简化呢? 请看下面

`rollup` 操作符::
`rollup` 操作符其实就是 `groupBy` 的一个扩展, `rollup` 会对传入的列进行滚动 `groupBy`, `groupBy` 的次数为列数量 `+ 1`, 最后一次是对整个数据集进行聚合

Step 1: 创建数据集::

```scala
import org.apache.spark.sql.functions._

val sales = Seq(
  ("Beijing", 2016, 100),
  ("Beijing", 2017, 200),
  ("Shanghai", 2015, 50),
  ("Shanghai", 2016, 150),
  ("Guangzhou", 2017, 50)
).toDF("city", "year", "amount")
```

Step 1: `rollup` 的操作::

```scala
sales.rollup("city", "year")
  .agg(sum("amount") as "amount")
  .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
  .show()

/**
  * 结果集:
  * +``````-+```+```--+
  * |     city|year|amount|
  * +``````-+```+```--+
  * | Shanghai|2015|    50| <-- 上海 2015 的小计
  * | Shanghai|2016|   150|
  * | Shanghai|null|   200| <-- 上海的总计
  * |Guangzhou|2017|    50|
  * |Guangzhou|null|    50|
  * |  Beijing|2016|   100|
  * |  Beijing|2017|   200|
  * |  Beijing|null|   300|
  * |     null|null|   550| <-- 整个数据集的总计
  * +``````-+```+```--+
  */
```

Step 2: 如果使用基础的 groupBy 如何实现效果?::

```scala
val cityAndYear = sales
  .groupBy("city", "year") // 按照 city 和 year 聚合
  .agg(sum("amount") as "amount")

val city = sales
  .groupBy("city") // 按照 city 进行聚合
  .agg(sum("amount") as "amount")
  .select($"city", lit(null) as "year", $"amount")

val all = sales
  .groupBy() // 全局聚合
  .agg(sum("amount") as "amount")
  .select(lit(null) as "city", lit(null) as "year", $"amount")

cityAndYear
  .union(city)
  .union(all)
  .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
  .show()

/**
  * 统计结果:
  * +``````-+```+```--+
  * |     city|year|amount|
  * +``````-+```+```--+
  * | Shanghai|2015|    50|
  * | Shanghai|2016|   150|
  * | Shanghai|null|   200|
  * |Guangzhou|2017|    50|
  * |Guangzhou|null|    50|
  * |  Beijing|2016|   100|
  * |  Beijing|2017|   200|
  * |  Beijing|null|   300|
  * |     null|null|   550|
  * +``````-+```+```--+
  */
```

很明显可以看到, 在上述案例中, `rollup` 就相当于先按照 `city`, `year` 进行聚合, 后按照 `city` 进行聚合, 最后对整个数据集进行聚合, 在按照 `city` 聚合时, `year` 列值为 `null`, 聚合整个数据集的时候, 除了聚合列, 其它列值都为 `null`

使用 `rollup` 完成 `pm` 值的统计::
上面的案例使用 `rollup` 来实现会非常的简单

```scala
import org.apache.spark.sql.functions._

pmFinal.rollup('source, 'year)
  .agg(sum("pm") as "pm_total")
  .sort('source.asc_nulls_last, 'year.asc_nulls_last)
  .show()
```

`cube`::
`cube` 的功能和 `rollup` 是一样的, 但也有区别, 区别如下

* `rollup(A, B).sum(C)`

其结果集中会有三种数据形式: `A B C`, `A null C`, `null null C`

不知道大家发现没, 结果集中没有对 `B` 列的聚合结果

* `cube(A, B).sum(C)`

其结果集中会有四种数据形式: `A B C`, `A null C`, `null null C`, `null B C`

不知道大家发现没, 比 `rollup` 的结果集中多了一个 `null B C`, 也就是说, `rollup` 只会按照第一个列来进行组合聚合, 但是 `cube` 会将全部列组合聚合

```scala
import org.apache.spark.sql.functions._

pmFinal.cube('source, 'year)
  .agg(sum("pm") as "pm_total")
  .sort('source.asc_nulls_last, 'year.asc_nulls_last)
  .show()

/**
  * 结果集为
  *
  * +```---+```+``````-+
  * | source|year| pm_total|
  * +```---+```+``````-+
  * | dongsi|2013| 735606.0|
  * | dongsi|2014| 745808.0|
  * | dongsi|2015| 752083.0|
  * | dongsi|null|2233497.0|
  * |us_post|2010| 841834.0|
  * |us_post|2011| 796016.0|
  * |us_post|2012| 750838.0|
  * |us_post|2013| 882649.0|
  * |us_post|2014| 846475.0|
  * |us_post|2015| 714515.0|
  * |us_post|null|4832327.0|
  * |   null|2010| 841834.0| <-- 新增
  * |   null|2011| 796016.0| <-- 新增
  * |   null|2012| 750838.0| <-- 新增
  * |   null|2013|1618255.0| <-- 新增
  * |   null|2014|1592283.0| <-- 新增
  * |   null|2015|1466598.0| <-- 新增
  * |   null|null|7065824.0|
  * +```---+```+``````-+
  */
```

`SparkSQL` 中支持的 `SQL` 语句实现 `cube` 功能::
`SparkSQL` 支持 `GROUPING SETS` 语句, 可以随意排列组合空值分组聚合的顺序和组成, 既可以实现 `cube` 也可以实现 `rollup` 的功能

```scala
pmFinal.createOrReplaceTempView("pm_final")
spark.sql(
  """
    |select source, year, sum(pm)
    |from pm_final
    |group by source, year
    |grouping sets((source, year), (source), (year), ())
    |order by source asc nulls last, year asc nulls last
  """.stripMargin)
  .show()
```

`RelationalGroupedDataset`::
常见的 `RelationalGroupedDataset` 获取方式有三种

* `groupBy`
* `rollup`
* `cube`

无论通过任何一种方式获取了 `RelationalGroupedDataset` 对象, 其所表示的都是是一个被分组的 `DataFrame`, 通过这个对象, 可以对数据集的分组结果进行聚合

```scala
val groupedDF: RelationalGroupedDataset = pmDF.groupBy('year)
```

需要注意的是, `RelationalGroupedDataset` 并不是 `DataFrame`, 所以其中并没有 `DataFrame` 的方法, 只有如下一些聚合相关的方法, 如下这些方法在调用过后会生成 `DataFrame` 对象, 然后就可以再次使用 `DataFrame` 的算子进行操作了

|===
| 操作符 | 解释

| `avg` | 求平均数
| `count` | 求总数
| `max` | 求极大值
| `min` | 求极小值
| `mean` | 求均数
| `sum` | 求和
| `agg` a|
聚合, 可以使用 `sql.functions` 中的函数来配合进行操作

```scala
pmDF.groupBy('year)
    .agg(avg('pm) as "pm_avg")
```

### 11. 连接

.导读
无类型连接 `join`
. 连接类型 `Join Types`

无类型连接算子 `join` 的 `API`::
Step 1: 什么是连接::
+
按照 PostgreSQL 的文档中所说, 只要能在一个查询中, 同一时间并发的访问多条数据, 就叫做连接.

做到这件事有两种方式

. 一种是把两张表在逻辑上连接起来, 一条语句中同时访问两张表

```sql
select * from user join address on user.address_id = address.id
```

. 还有一种方式就是表连接自己, 一条语句也能访问自己中的多条数据

```sql
select * from user u1 join (select * from user) u2 on u1.id = u2.id
```

Step 2: `join` 算子的使用非常简单, 大致的调用方式如下::

```scala
join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame
```

Step 3: 简单连接案例::
+
表结构如下

```text
+---+```--+```--+            +---+``````-+
| id|  name|cityId|            | id|     name|
+---+```--+```--+            +---+``````-+
|  0|  Lucy|     0|            |  0|  Beijing|
|  1|  Lily|     0|            |  1| Shanghai|
|  2|   Tim|     2|            |  2|Guangzhou|
|  3|Danial|     0|            +---+``````-+
+---+```--+``````

如果希望对这两张表进行连接, 首先应该注意的是可以连接的字段, 比如说此处的左侧表 `cityId` 和右侧表 `id` 就是可以连接的字段, 使用 `join` 算子就可以将两个表连接起来, 进行统一的查询

​```scala
val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
  .toDF("id", "name", "cityId")

val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
  .toDF("id", "name")

person.join(cities, person.col("cityId") #### cities.col("id"))
  .select(person.col("id"),
    person.col("name"),
    cities.col("name") as "city")
  .show()

/**
  * 执行结果:
  *
  * +---+```--+``````-+
  * | id|  name|     city|
  * +---+```--+``````-+
  * |  0|  Lucy|  Beijing|
  * |  1|  Lily|  Beijing|
  * |  2|   Tim|Guangzhou|
  * |  3|Danial|  Beijing|
  * +---+```--+``````-+
  */
```

Step 4: 什么是连接?::
+
现在两个表连接得到了如下的表

```text
+---+```--+``````-+
| id|  name|     city|
+---+```--+``````-+
|  0|  Lucy|  Beijing|
|  1|  Lily|  Beijing|
|  2|   Tim|Guangzhou|
|  3|Danial|  Beijing|
+---+```--+``````-
通过对这张表的查询, 这个查询是作用于两张表的, 所以是同一时间访问了多条数据

​```scala
spark.sql("select name from user_city where city = 'Beijing'").show()

/**
  * 执行结果
  *
  * +```--+
  * |  name|
  * +```--+
  * |  Lucy|
  * |  Lily|
  * |Danial|
  * +```--+
  */
```

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529095232.png)

连接类型::
如果要运行如下代码, 需要先进行数据准备

```scala
private val spark = SparkSession.builder()
  .master("local[6]")
  .appName("aggregation")
  .getOrCreate()

import spark.implicits._

val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3))
  .toDF("id", "name", "cityId")
person.createOrReplaceTempView("person")

val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
  .toDF("id", "name")
cities.createOrReplaceTempView("cities")
```

[cols="15h,15h,~"]
|===
| 连接类型 | 类型字段 | 解释

| 交叉连接 | `cross` a|
解释::
+
交叉连接就是笛卡尔积, 就是两个表中所有的数据两两结对
+
交叉连接是一个非常重的操作, 在生产中, 尽量不要将两个大数据集交叉连接, 如果一定要交叉连接, 也需要在交叉连接后进行过滤, 优化器会进行优化
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529120732.png)

`SQL` 语句::

```sql
select * from person cross join cities
```

`Dataset` 操作::

```scala
person.crossJoin(cities)
  .where(person.col("cityId") #### cities.col("id"))
  .show()
```

| 内连接 | `inner` a|
解释::
+
内连接就是按照条件找到两个数据集关联的数据, 并且在生成的结果集中只存在能关联到的数据
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529115831.png)

`SQL` 语句::

```sql
select * from person inner join cities on person.cityId = cities.id
```

`Dataset` 操作::

```scala
person.join(right = cities,
  joinExprs = person("cityId") #### cities("id"),
  joinType = "inner")
  .show()
```

| 全外连接 | `outer`, `full`, `fullouter` a|
解释::
+
内连接和外连接的最大区别, 就是内连接的结果集中只有可以连接上的数据, 而外连接可以包含没有连接上的数据, 根据情况的不同, 外连接又可以分为很多种, 比如所有的没连接上的数据都放入结果集, 就叫做全外连接
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529120033.png)

`SQL` 语句::

```sql
select * from person full outer join cities on person.cityId = cities.id
```

`Dataset` 操作::

```scala
person.join(right = cities,
  joinExprs = person("cityId") #### cities("id"),
  joinType = "full") // "outer", "full", "full_outer"
  .show()
```

| 左外连接 | `leftouter`, `left` a|
解释::
+
左外连接是全外连接的一个子集, 全外连接中包含左右两边数据集没有连接上的数据, 而左外连接只包含左边数据集中没有连接上的数据
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529120139.png)

`SQL` 语句::

```sql
select * from person left join cities on person.cityId = cities.id
```

`Dataset` 操作::

```scala
person.join(right = cities,
  joinExprs = person("cityId") #### cities("id"),
  joinType = "left") // leftouter, left
  .show()
```

| `LeftAnti` | `leftanti` a|
解释::
`LeftAnti` 是一种特殊的连接形式, 和左外连接类似, 但是其结果集中没有右侧的数据, 只包含左边集合中没连接上的数据
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529120454.png)

`SQL` 语句::

```sql
select * from person left anti join cities on person.cityId = cities.id
```

`Dataset` 操作::

```scala
person.join(right = cities,
  joinExprs = person("cityId") #### cities("id"),
  joinType = "left_anti")
  .show()
```

| `LeftSemi` | `leftsemi` a|
解释::
+
和 `LeftAnti` 恰好相反, `LeftSemi` 的结果集也没有右侧集合的数据, 但是只包含左侧集合中连接上的数据
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529120406.png)

`SQL` 语句::

```sql
select * from person left semi join cities on person.cityId = cities.id
```

`Dataset` 操作::

```scala
person.join(right = cities,
  joinExprs = person("cityId") #### cities("id"),
  joinType = "left_semi")
  .show()
```

| 右外连接 | `rightouter`, `right` a|
解释::
+
右外连接和左外连接刚好相反, 左外是包含左侧未连接的数据, 和两个数据集中连接上的数据, 而右外是包含右侧未连接的数据, 和两个数据集中连接上的数据
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529120222.png)

`SQL` 语句::

```sql
select * from person right join cities on person.cityId = cities.id
```

`Dataset` 操作::

```scala
person.join(right = cities,
  joinExprs = person("cityId") #### cities("id"),
  joinType = "right") // rightouter, right
  .show()
```

[扩展] 广播连接::

Step 1: 正常情况下的 `Join` 过程::
![image](07_SparkSQL/20190529151419.png)
`Join` 会在集群中分发两个数据集, 两个数据集都要复制到 `Reducer` 端, 是一个非常复杂和标准的 `ShuffleDependency`, 有什么可以优化效率吗?

Step 2: `Map` 端 `Join`::
+
前面图中看的过程, 之所以说它效率很低, 原因是需要在集群中进行数据拷贝, 如果能减少数据拷贝, 就能减少开销

如果能够只分发一个较小的数据集呢?

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529152206.png)

可以将小数据集收集起来, 分发给每一个 `Executor`, 然后在需要 `Join` 的时候, 让较大的数据集在 `Map` 端直接获取小数据集, 从而进行 `Join`, 这种方式是不需要进行 `Shuffle` 的, 所以称之为 `Map` 端 `Join`

Step 3: `Map` 端 `Join` 的常规实现::
+
如果使用 `RDD` 的话, 该如何实现 `Map` 端 `Join` 呢?

```scala
val personRDD = spark.sparkContext.parallelize(Seq((0, "Lucy", 0),
  (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3)))

val citiesRDD = spark.sparkContext.parallelize(Seq((0, "Beijing"),
  (1, "Shanghai"), (2, "Guangzhou")))

val citiesBroadcast = spark.sparkContext.broadcast(citiesRDD.collectAsMap())

val result = personRDD.mapPartitions(
  iter => {
    val citiesMap = citiesBroadcast.value
    // 使用列表生成式 yield 生成列表
    val result = for (person <- iter if citiesMap.contains(person._3))
      yield (person._1, person._2, citiesMap(person._3))
    result
  }
).collect()

result.foreach(println(_))
```

Step 4: 使用 `Dataset` 实现 `Join` 的时候会自动进行 `Map` 端 `Join`::
+
自动进行 `Map` 端 `Join` 需要依赖一个系统参数 `spark.sql.autoBroadcastJoinThreshold`, 当数据集小于这个参数的大小时, 会自动进行 `Map` 端 `Join`

如下, 开启自动 `Join`

```scala
println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt / 1024 / 1024)

println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
```

当关闭这个参数的时候, 则不会自动 Map 端 Join 了

```scala
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
```

Step 5: 也可以使用函数强制开启 Map 端 Join::
+
在使用 Dataset 的 join 时, 可以使用 broadcast 函数来实现 Map 端 Join

```scala
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
println(person.crossJoin(broadcast(cities)).queryExecution.sparkPlan.numberedTreeString)
```

即使是使用 SQL 也可以使用特殊的语法开启

```scala
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
val resultDF = spark.sql(
  """
    |select /*+ MAPJOIN (rt) */ * from person cross join cities rt
  """.stripMargin)
println(resultDF.queryExecution.sparkPlan.numberedTreeString)
```

### 8、spark当中的开窗函数

1、需求有json数据格式如下，分别是三个字段，对应学生姓名，所属班级，所得分数，求每个班级当中分数最高的前N个学生（分组求topN）

```scala
import org.apache.spark.sql.SparkSession

/**
  * 开窗函数的使用
  */
object TopN {

  def main(args: Array[String]): Unit = {
    //获取SparkSession
    val spark = SparkSession.builder().appName("topn").master("local[2]").getOrCreate()

    //读取文件
    val df = spark.read.json("F:\\score.txt")

    //注册成临时表
    df.createOrReplaceTempView("score")
    //使用开窗函数 获取每个班级前三名的学生信息
    spark.sql(
      """
        |select clazz,name,score from (
        | select clazz,name,score,row_number() over(partition by clazz order by score desc) rn
        | from score) a
        | where a.rn<=3
      """.stripMargin).show()
    /**
      * 常用开窗函数：（最常用的应该是1.2.3 的排序）
      * --排序函数
      * 1、row_number() over(partition by ... order by ...)
      * 2、rank() over(partition by ... order by ...)
      * 3、dense_rank() over(partition by ... order by ...)
      * --聚合函数
      * 4、count() over(partition by ... order by ...)
      * 5、max() over(partition by ... order by ...)
      * 6、min() over(partition by ... order by ...)
      * 7、sum() over(partition by ... order by ...)
      * 8、avg() over(partition by ... order by ...)
      * 9、first_value() over(partition by ... order by ...)
      * 10、last_value() over(partition by ... order by ...)
      */
    spark.stop()
  }
}
```

### 9、SparkSQL当中的自定义函数

类似于hive当中的自定义函数，我们在spark当中，如果内置函数不够我们使用，我们同样可以使用自定义函数来实现我们的功能，spark当中的自定义函数，同样的也有

UDF(User-Defined-Function)，即最基本的自定义函数，类似to_char,to_date等
UDAF（User- Defined Aggregation Funcation），用户自定义聚合函数，类似在group by之后使用的sum,avg等
UDTF(User-Defined Table-Generating Functions),用户自定义生成函数，有点像stream里面的flatMap

#### 9.1、自定义UDF函数

自定义udf函数总体分为两步:

1、自定义一个方法
2、通过spark.udf.register()将自定义方法注册为udf函数

```scala
import org.apache.spark.sql.SparkSession

/**
  * 自定义udf函数
  *   udf函数:一进一出
  *
  * 需求；自定义一个udf函数，实现字母转大写操作
  */
object UDFDemo {
  def main(args: Array[String]): Unit = {

    //获取SparkSession
    val spark = SparkSession.builder().master("local[*]").appName("app").getOrCreate()

    //构建数据
    import spark.implicits._
    val ds = spark.createDataset(Array[String]("hello","java","python","scala"))

    //注册为临时表
    ds.createOrReplaceTempView("t_tmp")
    //定义一个方法
    def toUpper(str:String):String={
      str.toUpperCase
    }
    //将定义的方法注册为udf函数
    spark.udf.register("toUpper",toUpper _)

    //使用自定义的udf函数将字母转为大写
    spark.sql("select toUpper(value) from t_tmp").show()

    spark.stop()
  }
}
```

#### 9.2、自定义UDAF函数

需求：现有json格式数据内容如下

```json
{"name":"Michael","salary":3000}
{"name":"Andy","salary":4500}
{"name":"Justin","salary":3500}
{"name":"Berta","salary":4000}
```

求取平均工资

```scala
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructType}

/**
  * 自定义udaf函数需要继承UserDefinedAggregateFunction，然后实现其中的方法
  */
class SparkFunctionUDAF  extends UserDefinedAggregateFunction{
  //表示聚合函数的输入类型，在我们需求中我们是求工资的平均值，工资字段类型为Double
  override def inputSchema: StructType = {
    new StructType()
    .add("input", DoubleType)
  }
  //表示缓冲值，也就是在计算期间需要用到的值，我们在计算期间需要用到总的工资数、总人数
  override def bufferSchema: StructType = {
    new StructType()
      .add("sum", DoubleType)
      .add("total", IntegerType)
  }
  //聚合函数返回值类型
  override def dataType: DataType = DoubleType
  //确保数据一致性 一般用true
  override def deterministic: Boolean = true
  //初始化缓冲区 对于我们本次的需求而言 就是需要初始化sum 与 total的值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //sum = 0
    buffer(0) = 0.0
    //total = 0
    buffer(1) = 0
  }
  //每进来一个值 就更新缓冲区的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //更新sum值
    buffer(0) = buffer.getDouble(0) + input.getAs[Double](0)
    //更新总人数
    buffer(1) = buffer.getInt(1) + 1
  }
  //合并两个缓存区的值
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }
  //根据缓冲区计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)/buffer.getInt(1)
  }
}

object UdafDemo {

  def main(args: Array[String]): Unit = {
    //获取sparksession
    val spark = SparkSession.builder().master("local[2]").appName("udafdemo").getOrCreate()
    //读取数据
    val df = spark.read.json("F:\\employees.json")
    //注册为临时表
    df.createOrReplaceTempView("employess")

    //将自定义udaf函数进行注册
    spark.udf.register("myAvg",new SparkFunctionUDAF)

    //使用自定义udaf函数
    spark.sql("select myAvg(salary) from employess").show()

    spark.stop()
  }
}
```

## 示例

### spark-warehouse

#### scala

#### rdd

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

#### sql

##### Column.scala

src/main/scala/cn/xhchen/spark/sql/Column.scala

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

##### NullProcessor.scala

src/main/scala/cn/xhchen/spark/sql/NullProcessor.scala

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

##### TypedTransformation.scala

src/main/scala/cn/xhchen/spark/sql/TypedTransformation.scala

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

src/main/scala/cn/xhchen/spark/sql/UntypedTransformation.scala

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

##### Api.scala-

Api.scala

```scala
package cn.xhchen.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructType}
import org.junit.Test

class Api{

  val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()

  import spark.implicits._
  @Test
  def withColumn_test(): Unit ={


    //1、toDS
    val data: RDD[Person] = spark.sparkContext.parallelize(Seq(Person(1,"张三",20),Person(2,"李四",30),Person(3,"王五",25)))

    val df1 = data.toDF()

    df1.createOrReplaceTempView("tmp_table")
    //新增列 random
    spark.sql(
      """
        |select id,name,age,rand() random
        | from tmp_table
      """.stripMargin)
    //重命名列
    spark.sql(
      """
        |select id,age,name,name as new_name
        | from tmp_table
      """.stripMargin).show
  }

  @Test
  def groupBy(): Unit ={
    //1、toDS
    val data: RDD[Person] = spark.sparkContext.parallelize(Seq(Person(1,"张三",20),Person(2,"李四",30),Person(3,"张三",25)))

    val df1 = data.toDF()

    //对用户名称进行分组，求年龄最大值
    df1.groupBy("name").max("age").show
    //对用户名称进行分组，求年龄最大值、最小值
    import org.apache.spark.sql.functions._
    //agg与max之类聚合的区别
    //  1、max只能用于求最大值这一个聚合操作，在有些需求中如果用到需要多个聚合操作，就不太适合
    //  2、在同时求取多个聚合结果的时候，只能用agg，在agg中写上聚合函数
    df1.groupBy("name").agg(max("age"),min("age")).show
  }


  @Test
  def na(): Unit ={

    val df = spark.read.option("header","true")
      .csv("data/BeijingPM20100101_20151231.csv")

    //
    import spark.implicits._
    import org.apache.spark.sql.functions._
    df.select('No ,'year,'month,'day,'season,
      when('PM_Dongsi === "NA",0.0)
        .otherwise('PM_Dongsi cast DoubleType)
    ).show


  }

  @Test
  def drop(): Unit ={
    //drop
    //  any
    //      一行中只要有一列的值为NaN或者null就删除该行
    //  all
    //     一行中所有列的值为NaN或者null就删除该行
    //  特定列
    //    any  all  ，这两种规则只针对指定列
    //id,year,month,day,hour,season,pm
    val schema = new StructType()
        .add("id",LongType)
        .add("year",IntegerType)
        .add("month",IntegerType)
        .add("day",IntegerType)
        .add("hour",IntegerType)
        .add("season",IntegerType)
        .add("pm",DoubleType)
    val df1 = spark.read.option("header",true)
      .schema(schema)
      .csv("data/beijingpm_with_nan.csv")

    //any:一行中只要有一列的值为NaN或者null就删除该行
    //df1.na.drop("any").show()
    //all:一行中所有列的值为NaN或者null就删除该行
    //df1.na.drop("all").show
    // any:一行中只要 year month day这三列中有一列的值为NaN或者null就删除该行
    //df1.na.drop("any",Array("year","month","day")).show
    //all:一行中year、month、day这三列的值都为NaN或者null就删除该行
    //df1.na.drop("all",Array("year","month","day")).show
    //fill：将NaN或者null的值填充为某一个指定的值
    df1.na.fill(0).show
    // 用replace的时候 key和value的类型要与列的类型要一致
    //fill与replace的区别:
    //  fill只针对NaN或者null进行填充
    //  repalce可以针对任意值进行替换
    df1.na.replace("year",Map(2010->2019)).show
  }

  @Test
  def group(): Unit ={
    val schema = new StructType()
      .add("id",LongType)
      .add("year",IntegerType)
      .add("month",IntegerType)
      .add("day",IntegerType)
      .add("hour",IntegerType)
      .add("season",IntegerType)
      .add("pm",DoubleType)
    val df1 = spark.read.option("header",true)
      .schema(schema)
      .csv("data/beijingpm_with_nan.csv")

    //df1.where('pm =!= "NaN").show

    val groupedDF: RelationalGroupedDataset = df1.where('pm =!= "NaN")
      .groupBy('year,'month)

    //functions函数
    import org.apache.spark.sql.functions._
    groupedDF.agg(max('pm) as "pm_max",min('pm) as "pm_min").show
    //GroupedDataset API
    groupedDF.max("pm").select($"max(pm)" as "pm_max").show
  }

  @Test
  def join(): Unit ={
    val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
      .toDF("id", "name", "cityId")

    val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
      .toDF("id", "name")
    import org.apache.spark.sql.functions._
    import spark.implicits._
   person.join(cities,person.col("cityId")===cities.col("id"))
      .select(person.col("id"),person.col("name"),cities.col("name"))
    //person.join(cities,'cityId ==='id).show
  }

}
```

##### UdafTest.scala

UdafTest.scala

```scala
package cn.xhchen.sql

import org.apache.spark.sql.SparkSession

object UdafTest {

  def main(args: Array[String]): Unit = {
    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()
    //2、读取json文件
    spark.read.json("data/test.json").createOrReplaceTempView("student")
    //3、注册udaf函数
    spark.udf.register("myAvg",new MyUdaf)
    //4、使用
    spark.sql("select myAvg(score) from student").show
  }
}
```

##### UdfTest.scala

UdfTest.scala

```scala
package cn.xhchen.sql

import org.apache.spark.sql.SparkSession

object UdfTest {

  def main(args: Array[String]): Unit = {

    //需求:将id不满8位，补足8位[补0]
    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()
    //2、读取json文件
    spark.read.json("data/test.json").createOrReplaceTempView("student")
    //3、自定义udf
    def increPrfix(id:String):String={
      "0"*(8-id.length)+id
    }
    //4、注册udf函数
    spark.udf.register("increPrfix",increPrfix _)
    //5、使用
    spark.sql("select increPrfix(id) id,name,clazz,score from student").show
  }
}
```

##### Window.scala-spark当中的开窗函数

Window.scala

```scala
package cn.xhchen.sql

import org.apache.spark.sql.SparkSession

object Window {

  def main(args: Array[String]): Unit = {
    /**
      * 常用开窗函数：（最常用的应该是1.2.3 的排序）
      * --排序函数
      * 1、row_number() over(partition by ... order by ...)
      * 2、rank() over(partition by ... order by ...)
      * 3、dense_rank() over(partition by ... order by ...)
      * --聚合函数
      * 4、count() over(partition by ... order by ...)
      * 5、max() over(partition by ... order by ...)
      * 6、min() over(partition by ... order by ...)
      * 7、sum() over(partition by ... order by ...)
      * 8、avg() over(partition by ... order by ...)
      * 9、first_value() over(partition by ... order by ...)
      * 10、last_value() over(partition by ... order by ...)
      */

    //需求: 获得班级成绩前两名的学生信息
    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()
    //2、读取文件注册成临时表
    spark.read.json("data/test.json").createOrReplaceTempView("student")
    //3、使用开窗函数获得学生信息
    //row_number rank dense_rank必须要指定order by
    spark.sql(
      """
        |select t.id,t.name,t.clazz,t.score from(
        |select s.id,s.name,s.clazz,s.score,row_number() over(partition by s.clazz order by s.score) rn
        | from student s) t where t.rn<=2
      """.stripMargin).show

    /**
      * {"id":"003","name":"c","clazz":1,"score":95}  1
      * {"id":"1","name":"a","clazz":1,"score":80}  2
      * {"id":"02","name":"b","clazz":1,"score":78}  3
      *
      * {"id":"06","name":"e","clazz":2,"score":92}  1
      * {"id":"05","name":"d","clazz":2,"score":74} 2
      *
      * {"id":"7","name":"f","clazz":3,"score":99} 1
      * {"id":"8","name":"g","clazz":3,"score":99} 2
      * {"id":"11","name":"j","clazz":3,"score":78} 3
      * {"id":"10","name":"i","clazz":3,"score":55} 4
      * {"id":"9","name":"h","clazz":3,"score":45} 5
      *
      */

    spark.sql(
      """select t.id,t.name,t.clazz,t.score from(
        |select s.*,rank() over(partition by s.clazz order by s.score desc) rn
        | from student s) t where t.rn<=2
      """.stripMargin)//.show

    /**
      * +-----+---+----+-----+---+
      * |clazz| id|name|score| rn|
      * +-----+---+----+-----+---+
      * |    1|003|   c|   95|  1|
      * |    1|  1|   a|   80|  2|
      * |    1| 02|   b|   78|  3|
      *
      * |    3|  7|   f|   99|  1|
      * |    3|  8|   g|   99|  1|2
      * |    3|  8|   g|   99|  1|3
      * |    3| 11|   j|   78|  4|
      * |    3| 10|   i|   55|  5|
      * |    3|  9|   h|   45|  6|
      *
      * |    2| 06|   e|   92|  1|
      * |    2| 05|   d|   74|  2|
      */

    spark.sql(
      """
        |select s.*,dense_rank() over(partition by s.clazz order by s.score desc) rn
        | from student s
      """.stripMargin)//.show

    /**
      * +-----+---+----+-----+---+
      * |clazz| id|name|score| rn|
      * +-----+---+----+-----+---+
      * |    1|003|   c|   95|  1|
      * |    1|  1|   a|   80|  2|
      * |    1| 02|   b|   78|  3|
      *
      * |    3|  7|   f|   99|  1|
      * |    3|  8|   g|   99|  1|
      * |    3| 11|   j|   78|  2|
      * |    3| 10|   i|   55|  3|
      * |    3|  9|   h|   45|  4|
      *
      * |    2| 06|   e|   92|  1|
      * |    2| 05|   d|   74|  2|
      * +-----+---+----+-----+---+
      */
    //聚合与开窗函数结合的时候
    //  1、聚合函数(需要指定字段)
    //  2、over(可以不用指定partition by 与order by),如果不指定就是指全局
    spark.sql(
      """
        |select s.*,max(s.score) over() max_score
        | from student s
      """.stripMargin)//.show

    /**
      * +-----+---+----+-----+---------+
      * |clazz| id|name|score|max_score|
      * +-----+---+----+-----+---------+
      * |    1|  1|   a|   80|       95|
      * |    1| 02|   b|   78|       95|
      * |    1|003|   c|   95|       95|
      * |    3|  7|   f|   99|       99|
      * |    3|  8|   g|   99|       99|
      * |    3|  9|   h|   45|       99|
      * |    3| 10|   i|   55|       99|
      * |    3| 11|   j|   78|       99|
      * |    2| 05|   d|   74|       92|
      * |    2| 06|   e|   92|       92|
      * +-----+---+----+-----+---------+
      */
  }
}
```

##### 作业.txt

作业.txt

```txt
name,price,crawl_time,market,province,city  table
1、每个省份农产品市场的个数
  select provice,count(distinct market)  from table group by provice
2、没有农产品市场的省份
  select b.* from table a right join table2 on a.province = b.province
    where a.province is null
3、根据农产品类型数据，统计前三名
  select name,count(1)
    from table group by name
      order by count(1) desc
    limit 3
4、根据农产品类型数量，统计每个省份前三名
select province,name from(
  select  province,name,rank() over(partition by province order by count(*) desc) rn
    from table
    group by province,name) t where t.rn<=3
5、计算山西省每种农产品价格波动
  select name,(sum(price)-max(price)-min(price))/(count(1)-2) price
    from table where provice = '山西'
  group by name
  
  
```

##### 总结.txt

总结.txt

```txt
1、读取
  spark.read
    format: 指定数据读取的类型
    option: 指定读取时的属性:header、infreschema...
    schema: 指定读取后数据的schema信息
    load: 加载数据
  
  简洁:spark.read.csv
2、写入
  df.write.mode(SaveMode.Append).csv
  写入模式:
    SaveMode.Append：追加
    SaveMode.Overwrite:覆盖
3、parquet:
  读取:
    1、spark.read.format("parquet").load
    2、spark.read.parquet(目录名/具体文件名)
  写入:
    1、df.write.mode(..).parquet(目录名)
    2、df.write.partitionBy(分区字段).mode.parquet
4、json:
  读取:
    1、spark.read.format("json").load
    2、spark.read.json(目录名/具体文件名)
  写入:
    df.write.json(...)
  将DataFrame或者DataSet转为json格式： df.toJson
  读取json格式的RDD：spark.read.json(RDD[String])
5、hive
  编程:
    1、指定metastore的地址: hive.meatstore.uris
    2、指定warehouse路径： spark.sql.warehouse.dir
    3、开启hive支持: enableHiveSupport
  
  读取:spark.sql("select * from hive表")
  写入:df.write.mode.saveAsTable(hive表名)
6、mysql
  读取: spark.read.jdbc(url,table,prop)
  写入: df.write.mode.jdbc(url,table,prop)
7、
  有类型:
    1、map、flatMap、mapPartition、transform[函数只有一个参数:DataSet]
    2、将DataFrame转为DataSet:  df.as[待转类型]
    3、filter
      1、filter(函数)
      2、filter(sql表达式)
      3、filter(column对象)
    4、groupByKey(需要指定key)
    5、split(Array(5,2,3)) //Array中有几个值就分为几份，Array中的值为每一份的权重
    6、orderBy
    5、
      distinct: 所有列的值都必须相同才能去重
      dropDuplicates: 指定列的值都必须相同才能去重
    6、集合
      差集、交集、并集
  无类型:
    1、选择
      1、select
      2、selectExpr
    2、分组
      groupBy
    3、Column
      1、创建
        1、无绑定
          1、'列名 : import spark.implicats._
          2、$"列名" :  import spark.implicats._
          3、col("列名") : import org.apache.spark.sql.functions._
          4、Column("列名") : import org.apache.spark.sql.functions._
        2、有绑定
          1、dataset.col("列名")
          2、dataset.apply("列名")
      2、操作
        1、别名:
          col("列名") as "别名"
        2、类型转换
          col("列名").as[类型]
        3、其他操作
          like
          isin
          ....
8、缺失值
  缺失值: null、"",NaN、"Null" 等都叫缺失值
  API： df.na
  drop:
    any:
      如果一行数据中有任意一列的值为NaN或者null就删除该行
    all:
      如果一行数据中所有列的值全部为NaN或者null才会删除该行
    针对特定列：
      以上两种规则只针对指定的列
  fill:
    对NaN或者null的值进行填充
  replace:
    针对指定的值进行替换
  字符串缺失值的处理:
    select中使用when
    where进行过滤
  
```
