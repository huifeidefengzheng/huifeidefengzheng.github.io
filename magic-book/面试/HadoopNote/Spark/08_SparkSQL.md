---
title: 08_SparkSQL.md
date: 2019/9/5 08:16:25
updated: 2019/9/5 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - Spark
---



#### 10. 聚合-AggProcessor.scala

导读
`groupBy` `rollup` `cube` `pivot` `RelationalGroupedDataset` 上的聚合操作

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
****

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
****

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

`rollup(A, B).sum(C)`
其结果集中会有三种数据形式: `A B C`, `A null C`, `null null C`

不知道大家发现没, 结果集中没有对 `B` 列的聚合结果

`cube(A, B).sum(C)`
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

`groupBy`
`rollup`
`cube`

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

#### 11. 连接-JoinProcessor.scala

导读
无类型连接 `join` 连接类型 `Join Types`

无类型连接算子 `join` 的 `API`::

Step 1: 什么是连接::
+
按照 PostgreSQL 的文档中所说, 只要能在一个查询中, 同一时间并发的访问多条数据, 就叫做连接.

做到这件事有两种方式
 一种是把两张表在逻辑上连接起来, 一条语句中同时访问两张表

```sql
select * from user join address on user.address_id = address.id
```

 还有一种方式就是表连接自己, 一条语句也能访问自己中的多条数据

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
+---+```--+```--+
```

如果希望对这两张表进行连接, 首先应该注意的是可以连接的字段, 比如说此处的左侧表 `cityId` 和右侧表 `id` 就是可以连接的字段, 使用 `join` 算子就可以将两个表连接起来, 进行统一的查询

```scala
val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
  .toDF("id", "name", "cityId")

val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
  .toDF("id", "name")

person.join(cities, person.col("cityId") ##### cities.col("id"))
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
+---+```--+``````-+
```

通过对这张表的查询, 这个查询是作用于两张表的, 所以是同一时间访问了多条数据

```scala
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
****

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
  .where(person.col("cityId") ##### cities.col("id"))
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
  joinExprs = person("cityId") ##### cities("id"),
  joinType = "inner")
  .show()
```

| 全外连接 | `outer`, `full`, `fullouter` a|
解释::
+
内连接和外连接的最大区别, 就是内连接的结果集中只有可以连接上的数据, 而外连接可以包含没有连接上的数据, 根据情况的不同, 外连接又可以分为很多种, 比如所有的没连接上的数据都放入结果集, 就叫做全外连接
+
![image](08_SparkSQL/20190529120033.png)

`SQL` 语句::

```sql
select * from person full outer join cities on person.cityId = cities.id
```

`Dataset` 操作::
+

```scala
person.join(right = cities,
  joinExprs = person("cityId") ##### cities("id"),
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
  joinExprs = person("cityId") ##### cities("id"),
  joinType = "left") // leftouter, left
  .show()
```

| `LeftAnti` | `leftanti` a|
解释::
+
`LeftAnti` 是一种特殊的连接形式, 和左外连接类似, 但是其结果集中没有右侧的数据, 只包含左边集合中没连接上的数据
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529120454.png)

`SQL` 语句::

```sql
select * from person left anti join cities on person.cityId = cities.id
```

`Dataset` 操作::

```scala
person.join(right = cities,
  joinExprs = person("cityId") ##### cities("id"),
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
  joinExprs = person("cityId") ##### cities("id"),
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
  joinExprs = person("cityId") ##### cities("id"),
  joinType = "right") // rightouter, right
  .show()
```

[扩展] 广播连接::

Step 1: 正常情况下的 `Join` 过程::
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190529151419.png)

`Join` 会在集群中分发两个数据集, 两个数据集都要复制到 `Reducer` 端, 是一个非常复杂和标准的 `ShuffleDependency`, 有什么可以优化效率吗?

Step 2: `Map` 端 `Join`::
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

## 示例

### AggProcessor.scala

```scala
package cn.itcast.spark.sql

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

### JoinProcessor.scala

```scala
package cn.itcast.spark.sql

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
