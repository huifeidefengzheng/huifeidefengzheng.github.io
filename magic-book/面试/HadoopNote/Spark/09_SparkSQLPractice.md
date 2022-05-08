---
title: 09_SparkSQL练习项目-出租车利用率分析
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - Spark
---


### SparkSQL 练习项目 - 出租车利用率分析

导读
本项目是 SparkSQL 阶段的练习项目, 主要目的是夯实同学们对于 SparkSQL 的理解和使用
数据集:
2013年纽约市出租车乘车记录
需求:
统计出租车利用率, 到某个目的地后, 出租车等待下一个客人的间隔

#### 1. 业务

导读
. 数据集介绍
. 业务场景介绍
. 和其它业务的关联
. 通过项目能学到什么

数据集结构:

| 字段 | 示例 | 示意|
|----|----|----|
| `hack_license` | `BA96DE419E711691B9445D6A6307C170` | 执照号, 可以唯一标识一辆出租车|
| `pickup_datetime` | `2013-01-01 15:11:48` | 上车时间|
| `dropoff_datetime` | `2013-01-01 15:18:10` | 下车时间|
| `pickup_longitude` | `-73.978165` | 上车点|
| `pickup_latitude` | `40.757977` | 上车点|
| `dropoff_longitude` | `-73.989838` | 下车点|
| `dropoff_latitude` | `40.751171` | 下车点|

其中有三个点需要注意
`hack_license` 是出租车执照, 可以唯一标识一辆出租车
`pickup_datetime` 和 `dropoff_datetime` 分别是上车时间和下车时间, 通过这个时间, 可以获知行车时间
`pickup_longitude` 和 `dropoff_longitude` 是经度, 经度所代表的是横轴, 也就是 X 轴
`pickup_latitude` 和 `dropoff_latitude` 是纬度, 纬度所代表的是纵轴, 也就是 Y 轴

业务场景:
在网约车出现之前, 出行很大一部分要靠出租车和公共交通, 所以经常会见到一些情况, 比如说从东直门打车, 告诉师傅要去昌平, 师傅可能拒载. 这种情况所凸显的是一个出租车调度的难题, 所以需要先通过数据来看到问题, 后解决问题.
所以要统计出租车利用率, 也就是有乘客乘坐的时间, 和无乘客空跑的时间比例. 这是一个理解出租车的重要指标, 影响利用率的一个因素就是目的地, 比如说, 去昌平, 可能出租车师傅不确定自己是否要空放回来, 而去国贸, 下车几分钟内, 一定能有新的顾客上车.
而统计利用率的时候, 需要用到时间数据和空间数据来进行计算, 对于时间计算来说, SparkSQL 提供了很多工具和函数可以使用, 而空间计算仍然是一个比较专业的场景, 需要使用到第三方库.
我们的需求是, 在上述的数据集中, 根据时间算出等待时间, 根据地点落地到某个区, 算出某个区的平均等待时间, 也就是这个下车地点对于出租车利用率的影响.

技术点和其它技术的关系:
数据清洗:
数据清洗在几乎所有类型的项目中都会遇到, 处理数据的类型, 处理空值等问题
JSON 解析:
`JSON` 解析在大部分业务系统的数据分析中都会用到, 如何读取 JSON 数据, 如何把 JSON 数据变为可以使用的对象数据
地理位置信息处理:
地理位置信息的处理是一个比较专业的场景, 在一些租车网站, 或者像滴滴, `Uber` 之类的出行服务上, 也经常会处理地理位置信息
探索性数据分析
从拿到一个数据集, 明确需求以后, 如何逐步了解数据集, 如何从数据集中探索对应的内容等, 是一个数据工程师的基本素质
会话分析
会话分析用于识别同一个用户的多个操作之间的关联, 是分析系统常见的分析模式, 在电商和搜索引擎中非常常见
在这个小节中希望大家掌握的知识:

. `SparkSQL` 中对于类型的处理
. `Scala` 中常见的 `JSON` 解析工具
. `GeoJson` 的使用

#### 2. 流程分析

导读
. 分析的步骤和角度
. 流程
分析的视角:
. 理解数据集

首先要理解数据集, 要回答自己一些问题

* 这个数据集是否以行作为单位, 是否是 `DataFrame` 可以处理的, 大部分情况下都是
* 这个数据集每行记录所代表的实体对象是什么, 例如: 出租车的载客记录
* 表达这个实体对象的最核心字段是什么, 例如: 上下车地点和时间, 唯一标识一辆车的 `License`
理解需求和结果集

* 小学的时候, 有一次考试考的比较差, 老师在帮我分析的时候, 告诉我, 你下次要读懂题意, 再去大题, 这样不会浪费时间, 于是这个信念贯穿了我这些年的工作.
* 按照我对开发工作的理解, 在一开始的阶段进行一个大概的思考和面向对象的设计, 并不会浪费时间, 即使这些设计可能会占用一些时间.
* 对代码的追求也不会浪费时间, 把代码写好, 会减少阅读成本, 沟通成本.
* 对测试的追求也不会浪费时间, 因为在进行回归测试的时候, 可以尽可能的减少修改对已有代码的冲击.

所以第一点, 理解需求再动手, 绝对不会浪费时间. 第二点, 在数据分析的任务中, 如何无法理解需求, 可能根本无从动手.

* 我们的需求是: 出租车在某个地点的平均等待客人时间
* 简单来说, 结果集中应该有的列: 地点, 平均等待时间
反推每一个步骤
结果集中, 应该有的字段有两个, 一个是地点, 一个是等待时间
地点如何获知? 其实就是乘客的下车点, 但是是一个坐标, 如何得到其在哪个区?
等待时间如何获知? 其实就是上一个乘客下车, 到下一个乘客上车之间的时间, 通过这两个时间的差值便可获知
步骤分析:
. 读取数据集
数据集很大, 所以我截取了一小部分, 大概百分之一左右, 如果感兴趣的话, 可以将完整数据集放在集群中, 使用集群来计算 "大数据"
清洗
数据集当中的某些列名可能使用起来不方便, 或者数据集当中某些列的值类型可能不对, 或者数据集中有可能存在缺失值, 这些都是要清洗的动机, 和理由
增加区域列
由于最终要统计的结果是按照区域作为单位, 而不是一个具体的目的地点, 所以要在数据集中增加列中放置区域信息
. 既然是放置行政区名字, 应该现有行政区以及其边界的信息
. 通过上下车的坐标点, 可以判断是否存在于某个行政区中

这些判断坐标点是否属于某个区域, 这些信息, 就是专业的领域了
按照区域, 统计司机两次营运记录之间的时间差
数据集中存在很多出租车师傅的数据, 所以如何将某个师傅的记录发往一个分区, 在这个分区上完成会话分析呢? 这也是一个需要理解的点

#### 3. 数据读取

导读
. 工程搭建
. 数据读取
工程搭建:
. 创建 Maven 工程
. 导入 Maven 配置

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>cn.xhchen</groupId>
    <artifactId>taxi</artifactId>
    <version>0.0.1</version>

    <properties>
        <scala.version>2.11.8</scala.version>
        <spark.version>2.2.0</spark.version>
        <hadoop.version>2.7.5</hadoop.version>
        <slf4j.version>1.7.16</slf4j.version>
        <log4j.version>1.2.17</log4j.version>
        <mysql.version>5.1.35</mysql.version>
        <esri.version>2.2.2</esri.version>
        <json4s.version>3.6.6</json4s.version>
    </properties>

    <dependencies>
        <!-- Scala 库 -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>org.scala-lang.modules</groupId>
            <artifactId>scala-xml_2.11</artifactId>
            <version>1.0.6</version>
        </dependency>

        <!-- Spark 系列包 -->
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
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>

        <!-- 地理位置处理库 -->
        <dependency>
            <groupId>com.esri.geometry</groupId>
            <artifactId>esri-geometry-api</artifactId>
            <version>${esri.version}</version>
        </dependency>

        <!-- JSON 解析库 -->
        <dependency>
            <groupId>org.json4s</groupId>
            <artifactId>json4s-native_2.11</artifactId>
            <version>${json4s.version}</version>
        </dependency>
        <dependency>
            <groupId>org.json4s</groupId>
            <artifactId>json4s-jackson_2.11</artifactId>
            <version>${json4s.version}</version>
        </dependency>

        <!-- 日志相关 -->
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
    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>

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
        </plugins>

    </build>
</project>
```

创建 Scala 源码目录 `src/main/scala`,并且设置这个目录为 `Source Root`
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190602150555.png)
创建文件, 数据读取:
`Step 1`: 创建文件:
创建 Spark Application 主类 `cn.xhchen.taxi.TaxiAnalysisRunner`

```scala
package cn.xhchen.taxi

object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {

  }
}
```

`Step 2`: 数据读取:
数据读取之前要做两件事:
. 初始化环境, 导入必备的一些包
. 在工程根目录中创建 `dataset` 文件夹, 并拷贝数据集进去`half_trip.csv`

代码如下:

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("taxi")
      .getOrCreate()

    // 2. 导入函数和隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 3. 读取文件
    val taxiRaw = spark.read
      .option("header", value = true)
      .csv("dataset/half_trip.csv")

    taxiRaw.show()
    taxiRaw.printSchema()
  }
}
```

运行结果如下:

```text
+--------------------+--------------------+---------+---------+------------------+-------------------+-------------------+---------------+-----------------+-------------+----------------+---------------+-----------------+----------------+
|           medallion|        hack_license|vendor_id|rate_code|store_and_fwd_flag|    pickup_datetime|   dropoff_datetime|passenger_count|trip_time_in_secs|trip_distance|pickup_longitude|pickup_latitude|dropoff_longitude|dropoff_latitude|
+--------------------+--------------------+---------+---------+------------------+-------------------+-------------------+---------------+-----------------+-------------+----------------+---------------+-----------------+----------------+
|89D227B655E5C82AE...|BA96DE419E711691B...|      CMT|        1|                 N|2013-01-01 15:11:48|2013-01-01 15:18:10|              4|              382|         1.00|      -73.978165|      40.757977|       -73.989838|       40.751171|
|0BD7C8F5BA12B88E0...|9FD8F69F0804BDB55...|      CMT|        1|                 N|2013-01-06 00:18:35|2013-01-06 00:22:54|              1|              259|         1.50|      -74.006683|      40.731781|       -73.994499|        40.75066|
|0BD7C8F5BA12B88E0...|9FD8F69F0804BDB55...|      CMT|        1|                 N|2013-01-05 18:49:41|2013-01-05 18:54:23|              1|              282|         1.10|      -74.004707|       40.73777|       -74.009834|       40.726002|
|DFD2202EE08F7A8DC...|51EE87E3205C985EF...|      CMT|        1|                 N|2013-01-07 23:54:15|2013-01-07 23:58:20|              2|              244|          .70|      -73.974602|      40.759945|       -73.984734|       40.759388|
|DFD2202EE08F7A8DC...|51EE87E3205C985EF...|      CMT|        1|                 N|2013-01-07 23:25:03|2013-01-07 23:34:24|              1|              560|         2.10|       -73.97625|      40.748528|       -74.002586|       40.747868|
|20D9ECB2CA0767CF7...|598CCE5B9C1918568...|      CMT|        1|                 N|2013-01-07 15:27:48|2013-01-07 15:38:37|              1|              648|         1.70|      -73.966743|      40.764252|       -73.983322|       40.743763|
|496644932DF393260...|513189AD756FF14FE...|      CMT|        1|                 N|2013-01-08 11:01:15|2013-01-08 11:08:14|              1|              418|          .80|      -73.995804|      40.743977|       -74.007416|       40.744343|
|0B57B9633A2FECD3D...|CCD4367B417ED6634...|      CMT|        1|                 N|2013-01-07 12:39:18|2013-01-07 13:10:56|              3|             1898|        10.70|      -73.989937|      40.756775|        -73.86525|        40.77063|
|2C0E91FF20A856C89...|1DA2F6543A62B8ED9...|      CMT|        1|                 N|2013-01-07 18:15:47|2013-01-07 18:20:47|              1|              299|          .80|      -73.980072|      40.743137|       -73.982712|       40.735336|
|2D4B95E2FA7B2E851...|CD2F522EEE1FF5F5A...|      CMT|        1|                 N|2013-01-07 15:33:28|2013-01-07 15:49:26|              2|              957|         2.50|      -73.977936|      40.786983|       -73.952919|        40.80637|
|E12F6AF991172EAC3...|06918214E951FA000...|      CMT|        1|                 N|2013-01-08 13:11:52|2013-01-08 13:19:50|              1|              477|         1.30|      -73.982452|      40.773167|       -73.964134|       40.773815|
|E12F6AF991172EAC3...|06918214E951FA000...|      CMT|        1|                 N|2013-01-08 09:50:05|2013-01-08 10:02:54|              1|              768|          .70|       -73.99556|      40.749294|       -73.988686|       40.759052|
|78FFD9CD0CDA541F3...|E949C583ECF62C8F0...|      CMT|        1|                 N|2013-01-10 12:07:08|2013-01-10 12:17:29|              1|              620|         2.30|      -73.971497|      40.791321|       -73.964478|       40.775921|
|237F49C3ECC11F502...|93C363DDF8ED9385D...|      CMT|        1|                 N|2013-01-07 07:35:47|2013-01-07 07:46:00|              1|              612|         2.30|       -73.98851|      40.774307|       -73.981094|       40.755325|
|3349F919AA8AE5DC9...|7CE849FEF67514F08...|      CMT|        1|                 N|2013-01-10 15:42:29|2013-01-10 16:04:02|              1|             1293|         3.20|      -73.994911|      40.723221|       -73.971558|       40.761612|
|3349F919AA8AE5DC9...|7CE849FEF67514F08...|      CMT|        1|                 N|2013-01-10 14:27:28|2013-01-10 14:45:21|              1|             1073|         4.40|      -74.010391|      40.708702|       -73.987846|       40.756104|
|4C005EEBAA7BF26B8...|351BE7D984BE17DB2...|      CMT|        1|                 N|2013-01-07 22:09:59|2013-01-07 22:19:50|              1|              591|         1.70|      -73.973732|      40.756287|       -73.998413|       40.756832|
|7D99C30FCE69B1A9D...|460C3F57DD9CB2265...|      CMT|        1|                 N|2013-01-07 17:18:16|2013-01-07 17:20:55|              1|              158|          .70|      -73.968925|      40.767704|        -73.96199|       40.776566|
|E6FBF80668FE0611A...|36773E80775F26CD1...|      CMT|        1|                 N|2013-01-07 06:08:51|2013-01-07 06:13:14|              1|              262|         1.70|       -73.96212|      40.769737|       -73.979561|        40.75539|
|0C5296F3C8B16E702...|D2363240A9295EF57...|      CMT|        1|                 N|2013-01-07 22:25:46|2013-01-07 22:36:56|              1|              669|         2.30|      -73.989708|      40.756714|       -73.977615|       40.787575|
+--------------------+--------------------+---------+---------+------------------+-------------------+-------------------+---------------+-----------------+-------------+----------------+---------------+-----------------+----------------+
only showing top 20 rows

root
 |-- medallion: string (nullable = true)
 |-- hack_license: string (nullable = true)
 |-- vendor_id: string (nullable = true)
 |-- rate_code: string (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- pickup_datetime: string (nullable = true)
 |-- dropoff_datetime: string (nullable = true)
 |-- passenger_count: string (nullable = true)
 |-- trip_time_in_secs: string (nullable = true)
 |-- trip_distance: string (nullable = true)
 |-- pickup_longitude: string (nullable = true)
 |-- pickup_latitude: string (nullable = true)
 |-- dropoff_longitude: string (nullable = true)
 |-- dropoff_latitude: string (nullable = true)
```

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190602153339.png)

下一步:
剪去多余列
现在数据集中包含了一些多余的列, 在后续的计算中并不会使用到, 如果让这些列参与计算的话, 会影响整体性能, 浪费集群资源
类型转换
可以看到, 现在的数据集中, 所有列类型都是 `String`, 而在一些统计和运算中, 不能使用 `String` 来进行, 所以要将这些数据转为对应的类型

#### 5. 数据清洗

导读
. 将 `Row` 对象转为 `Trip`
. 处理转换过程中的报错
数据转换:
通过 `DataFrameReader` 读取出来的数据集是 `DataFrame`, 而 `DataFrame` 中保存的是 `Row` 对象, 但是后续我们在进行处理的时候可能要使用到一些有类型的转换, 也需要每一列数据对应自己的数据类型, 所以, 需要将 `Row` 所代表的弱类型对象转为 `Trip` 这样的强类型对象, 而 `Trip` 对象则是一个样例类, 用于代表一个出租车的行程
`Step 1`: 创建 `Trip` 样例类:
`Trip` 是一个强类型的样例类, 一个 `Trip` 对象代表一个出租车行程, 使用 `Trip` 可以对应数据集中的一条记录

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // 此处省略 Main 方法中内容
  }

}

/**
  * 代表一个行程, 是集合中的一条记录
  * @param license 出租车执照号
  * @param pickUpTime 上车时间
  * @param dropOffTime 下车时间
  * @param pickUpX 上车地点的经度
  * @param pickUpY 上车地点的纬度
  * @param dropOffX 下车地点的经度
  * @param dropOffY 下车地点的纬度
  */
case class Trip(
  license: String,
  pickUpTime: Long,
  dropOffTime: Long,
  pickUpX: Double,
  pickUpY: Double,
  dropOffX: Double,
  dropOffY: Double
)
```

`Step 2`: 将 `Row` 对象转为 `Trip` 对象, 从而将 `DataFrame` 转为 `Dataset[Trip]`:
首先应该创建一个新方法来进行这种转换, 毕竟是一个比较复杂的转换操作, 不能怠慢

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // ... 省略数据读取

    // 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(parse)
  }

  /**
    * 将 Row 对象转为 Trip 对象, 从而将 DataFrame 转为 Dataset[Trip] 方便后续操作
    * @param row DataFrame 中的 Row 对象
    * @return 代表数据集中一条记录的 Trip 对象
    */
  def parse(row: Row): Trip = {

  }
}

case class Trip(...)
```

`Step 3`: 创建 `Row` 对象的包装类型:
因为在针对 `Row` 类型对象进行数据转换时, 需要对一列是否为空进行判断和处理, 在 `Scala` 中为空的处理进行一些支持和封装, 叫做 `Option`, 所以在读取 `Row` 类型对象的时候, 要返回 `Option` 对象, 通过一个包装类, 可以轻松做到这件事
创建一个类 `RichRow` 用以包装 `Row` 类型对象, 从而实现 `getAs` 的时候返回 `Option` 对象

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // ...

    // 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(parse)
  }

  def parse(row: Row): Trip = {...}

}

case class Trip(...)


class RichRow(row: Row) {

  def getAs[T](field: String): Option[T] = {
    if (row.isNullAt(row.fieldIndex(field)) || StringUtils.isBlank(row.getAs[String](field))) {
      None
    } else {
      Some(row.getAs[T](field))
    }
  }
}
```

`Step 4`: 转换:
流程已经存在, 并且也已经为空值处理做了支持, 现在就可以进行转换了
首先根据数据集的情况会发现, 有如下几种类型的信息需要处理
字符串类型
执照号就是字符串类型, 对于字符串类型, 只需要判断空, 不需要处理, 如果是空字符串, 加入数据集的应该是一个 `null`
时间类型
上下车时间就是时间类型, 对于时间类型需要做两个处理
转为时间戳, 比较容易处理
如果时间非法或者为空, 则返回 `0L`
`Double` 类型
上下车的位置信息就是 `Double` 类型, `Double` 类型的数据在数据集中以 `String` 的形式存在, 所以需要将 `String` 类型转为 `Double` 类型
总结来看, 有两类数据需要特殊处理, 一类是时间类型, 一类是 `Double` 类型, 所以需要编写两个处理数据的帮助方法, 后在 `parse` 方法中收集为 `Trip` 类型对象

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // ...

    // 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(parse)
  }

  def parse(row: Row): Trip = {
    // 通过使用转换方法依次转换各个字段数据
    val richRow = new RichRow(row)
    val license = richRow.getAs[String]("hack_license").orNull
    val pickUpTime = parseTime(richRow, "pickup_datetime")
    val dropOffTime = parseTime(richRow, "dropoff_datetime")
    val pickUpX = parseLocation(richRow, "pickup_longitude")
    val pickUpY = parseLocation(richRow, "pickup_latitude")
    val dropOffX = parseLocation(richRow, "dropoff_longitude")
    val dropOffY = parseLocation(richRow, "dropoff_latitude")

    // 创建 Trip 对象返回
    Trip(license, pickUpTime, dropOffTime, pickUpX, pickUpY, dropOffX, dropOffY)
  }

  /**
    * 将时间类型数据转为时间戳, 方便后续的处理
    * @param row 行数据, 类型为 RichRow, 以便于处理空值
    * @param field 要处理的时间字段所在的位置
    * @return 返回 Long 型的时间戳
    */
  def parseTime(row: RichRow, field: String): Long = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = new SimpleDateFormat(pattern, Locale.ENGLISH)

    val timeOption = row.getAs[String](field)
    timeOption.map( time => formatter.parse(time).getTime )
      .getOrElse(0L)
  }

  /**
    * 将字符串标识的 Double 数据转为 Double 类型对象
    * @param row 行数据, 类型为 RichRow, 以便于处理空值
    * @param field 要处理的 Double 字段所在的位置
    * @return 返回 Double 型的时间戳
    */
  def parseLocation(row: RichRow, field: String): Double = {
    row.getAs[String](field).map( loc => loc.toDouble ).getOrElse(0.0D)
  }
}

case class Trip(..)

class RichRow(row: Row) {...}
```

异常处理:
在进行类型转换的时候, 是一个非常容易错误的点, 需要进行单独的处理
`Step 1`: 思路:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190603015655.png)
`parse` 方法应该做的事情应该有两件
捕获异常
异常一定是要捕获的, 无论是否要抛给 `DataFrame`, 都要先捕获一下, 获知异常信息
捕获要使用 `try ... catch ...` 代码块
返回结果
返回结果应该分为两部分来进行说明

* 正确, 正确则返回数据
* 错误, 则应该返回两类信息, 一 告知外面哪个数据出了错, 二 告知错误是什么

对于这种情况, 可以使用 `Scala` 中提供的一个类似于其它语言中多返回值的 `Either`. `Either` 分为两个情况, 一个是 `Left`, 一个是 `Right`, 左右两个结果所代表的意思可有由用户来指定
新建一个`EitherTest.scala`

```scala
object EitherTest {

  def main(args: Array[String]): Unit = {
//<1> 一个函数, 接收一个参数, 根据参数进行除法运算
val process = (b: Double) => {
  val a = 10.0
  a / b
}
//<2> 一个方法, 作用是让 `process` 函数调用起来更安全, 在其中 `catch` 错误, 报错后返回足够的信息 (报错时的参数和报错信息)
def safe(function: Double => Double, b: Double): Either[Double, (Double, Exception)] = {
  try {
    //<3> 正常时返回 `Left`, 放入正确结果
    val result = function(b)
    Left(result)
  } catch {
    //<4> 异常时返回 `Right`, 放入报错时的参数, 和报错信息
    case e: Exception => Right(b, e)
  }
}
//<5> 外部调用
val result = safe(process, 0)
//<6> 处理调用结果, 如果是 Right 的话, 则可以进行响应的异常处理和弥补
result match {                       // <6>
  case Left(r) => println(r)
  case Right((b, e)) => println(b, e)
}

  }
```

`Either` 和 `Option` 比较像, 都是返回不同的情况, 但是 `Either` 的 `Right` 可以返回多个值, 而 `None` 不行
如果一个 `Either` 有两个结果的可能性, 一个是 `Left[L]`, 一个是 `Right[R]`, 则 `Either` 的范型是 `Either[L, R]`
`Step 2`: 完成代码逻辑:
加入一个 Safe 方法, 更安全

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // ...

    // 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
  }

  /**
    * 包裹转换逻辑, 并返回 Either
    */
  def safe[P, R](f: P => R): P => Either[R, (P, Exception)] = {
    new Function[P, Either[R, (P, Exception)]] with Serializable {
      override def apply(param: P): Either[R, (P, Exception)] = {
        try {
          Left(f(param))
        } catch {
          case e: Exception => Right((param, e))
        }
      }
    }
  }

  def parse(row: Row): Trip = {...}
  def parseTime(row: RichRow, field: String): Long = {...}
  def parseLocation(row: RichRow, field: String): Double = {...}
}

case class Trip(..)
class RichRow(row: Row) {...}
```

`Step 3`: 针对转换异常进行处理:
对于 `Either` 来说, 可以获取 `Left` 中的数据, 也可以获取 `Right` 中的数据, 只不过如果当 `Either` 是一个 Right 实例时候, 获取 `Left` 的值会报错
所以, 针对于 `Dataset[Either]` 可以有如下步骤

. 试运行, 观察是否报错
. 如果报错, 则打印信息解决报错
. 如果解决不了, 则通过 `filter` 过滤掉 `Right`
. 如果没有报错, 则继续向下运行

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    ...

    // 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.map( either => either.left.get ).toDS()
  }

  ...
}

...
```

很幸运, 在运行上面的代码时, 没有报错, 如果报错的话, 可以使用如下代码进行过滤

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    ...

    // 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.filter( either => either.isLeft )
      .map( either => either.left.get )
      .toDS()
  }

  ...
}

...
```

观察数据集的时间分布:
观察数据分布常用手段是直方图, 直方图反应的是数据的 `"数量"` 分布
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190603113500.png)
通过这个图可以看到其实就是乘客年龄的分布, 横轴是乘客的年龄, 纵轴是乘客年龄的频数分布
因为我们这个项目中要对出租车利用率进行统计, 所以需要先看一看单次行程的时间分布情况, 从而去掉一些异常数据, 保证数据是准确的
绘制直方图的 "图" 留在后续的 `DMP` 项目中再次介绍, 现在先准备好直方图所需要的数据集, 通过数据集来观察即可, 直方图需要的是两个部分的内容, 一个是数据本身, 另外一个是数据的分布, 也就是频数的分布, 步骤如下
计算每条数据的时长, 但是单位要有变化, 按照分钟, 或者小时来作为时长单位
. 统计每个时长的数据量, 例如有 `500` 个行程是一小时内完成的, 有 `300` 个行程是 `1 - 2` 小时内完成
统计时间分布直方图:
使用 `UDF` 的优点和代价:
`UDF` 是一个很好用的东西, 特别好用, 对整体的逻辑实现会变得更加简单可控, 但是有两个非常明显的缺点, 所以在使用的时候要注意, 虽然有这两个缺点, 但是只在必要的地方使用就没什么问题, 对于逻辑的实现依然是有很大帮助的
. `UDF` 中, 对于空值的处理比较麻烦
例如一个 `UDF` 接收两个参数, 是 `Scala` 中的 `Int` 类型和 `Double` 类型, 那么, 在传入 `UDF` 参数的时候, 如果有数据为 `null`, 就会出现转换异常
使用 `UDF` 的时候, 优化器可能无法对其进行优化
`UDF` 对于 `Catalyst` 是不透明的, `Catalyst` 不可获知 `UDF` 中的逻辑, 但是普通的 `Function` 对于 `Catalyst` 是透明的, `Catalyst` 可以对其进行优化
`Step 1`: 编写 `UDF`, 将行程时长由毫秒单位改为小时单位:
定义 `UDF`, 在 `UDF` 中做两件事

. 计算行程时长
. 将时长由毫秒转为分钟

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    ...

    // 5. 过滤行程无效的数据
    val hours = (pickUp: Long, dropOff: Long) => {
      val duration = dropOff - pickUp
      TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS)
    }
    val hoursUDF = udf(hours)
  }

  ...
}
```

`Step 2:` 统计时长分布:
. 第一步应该按照行程时长进行分组
. 求得每个分组的个数
. 最后按照时长排序并输出结果

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    ...

    // 5. 过滤行程无效的数据
    val hours = (pickUp: Long, dropOff: Long) => {
      val duration = dropOff - pickUp
      TimeUnit.MINUTES.convert(duration, TimeUnit.MILLISECONDS)
    }
    val hoursUDF = udf(hours)
//分组，排序
    taxiGood.groupBy(hoursUDF($"pickUpTime", $"dropOffTime").as("duration"))
      .count()
      .sort("duration")
      .show()
  }

  ...
}
```

若按小时算
`TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS)`
运结果是：

```text
+--------+-----+
|duration|count|
+--------+-----+
|       0| 9999|
|       1|    1|
+--------+-----+
```

按分钟计算`TimeUnit.MINUTES.convert(duration, TimeUnit.MILLISECONDS)`
会发现, 大部分时长都集中在 `1 - 19` 分钟内

```text
--------+-----+
|duration|count|
+--------+-----+
|       0|   86|
|       1|  140|
|       2|  383|
|       3|  636|
|       4|  759|
|       5|  838|
|       6|  791|
|       7|  761|
|       8|  688|
|       9|  625|
|      10|  537|
|      11|  499|
|      12|  395|
|      13|  357|
|      14|  353|
|      15|  264|
|      16|  252|
|      17|  197|
|      18|  181|
|      19|  136|
+--------+-----+
only showing top 20 rows
```

`Step 3:` 注册函数, 在 SQL 表达式中过滤数据:
+
大部分时长都集中在 `1 - 19` 分钟内, 所以这个范围外的数据就可以去掉了, 如果同学使用完整的数据集, 会发现还有一些负的时长, 好像是回到未来的场景一样, 对于这种非法的数据, 也要过滤掉, 并且还要分析原因

```scala
object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    ...

    // 5. 过滤行程无效的数据
    val hours = (pickUp: Long, dropOff: Long) => {
      val duration = dropOff - pickUp
      TimeUnit.MINUTES.convert(duration, TimeUnit.MILLISECONDS)
    }
    val hoursUDF = udf(hours)

    taxiGood.groupBy(hoursUDF($"pickUpTime", $"dropOffTime").as("duration"))
      .count()
      .sort("duration")
      .show()

    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickUpTime, dropOffTime) BETWEEN 0 AND 3")
    taxiClean.show()
  }

  ...
}
```

#### 6. 行政区信息

目标和步骤:
目标:
能够通过 `GeoJSON` 判断一个点是否在一个区域内, 能够使用 `JSON4S` 解析 `JSON` 数据
步骤:
. 需求介绍
. 工具介绍
. 解析 `JSON`
. 读取 `Geometry`

总结:
整体流程
. JSON4S 介绍
. ESRI 介绍
. 编写函数实现 `经纬度 -> Geometry` 转换

* 后续可以使用函数来进行转换, 并且求得时间差

#### 6.1. 需求介绍

目标和步骤:

目标:
+
理解表示地理位置常用的 GeoJSON

步骤:
. 思路整理
. `GeoJSON` 是什么
. `GeoJSON` 的使用

思路整理:
需求
项目的任务是统计出租车在不同行政区的平均等待时间, 所以源数据集和经过计算希望得到的新数据集大致如下
源数据集
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190812104021.png)
目标数据集
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190812104113.png)
目标数据集分析
目标数据集中有三列, `borough`, `avg(seconds)`, `stddev_samp(seconds)`
`borough` 表示目的地行政区的名称
`avg(seconds)` 和 `stddev_samp(seconds)` 是 `seconds` 的聚合, `seconds` 是下车时间和下一次上车时间之间的差值, 代表等待时间
所以有两列数据是现在数据集中没有
`borough` 要根据数据集中的经纬度, 求出其行政区的名字
`seconds` 要根据数据集中上下车时间, 求出差值
步骤
. 求出 `borough`
. 读取行政区位置信息
. 搜索每一条数据的下车经纬度所在的行政区
. 在数据集中添加行政区列
. 求出 `seconds`
. 根据 `borough` 计算平均等待时间, 是一个聚合操作

GeoJSON 是什么:
定义
`GeoJSON` 是一种基于 `JSON` 的开源标准格式, 用来表示地理位置信息
其中定了很多对象, 表示不同的地址位置单位

* 如何表示地理位置

^.^| 类型 2+^.^| 例子

`.| 点 .`

![image](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c2/SFA_Point.svg/51px-SFA_Point.svg.png) a|

```json
{
    "type": "Point",
    "coordinates": [30, 10]
}
```

`.| 线段 .`
![image](https://upload.wikimedia.org/wikipedia/commons/thumb/b/b9/SFA_LineString.svg/51px-SFA_LineString.svg.png) a|

```json
{
    "type": "Point",
    "coordinates": [30, 10]
}
```

`.2+.| 多边形 .a|`
![image](https://upload.wikimedia.org/wikipedia/commons/thumb/b/b9/SFA_LineString.svg/51px-SFA_LineString.svg.png) a|

```json
{
    "type": "Point",
    "coordinates": [30, 10]
}
```

`^.^a`
![image](https://upload.wikimedia.org/wikipedia/commons/thumb/5/55/SFA_Polygon_with_hole.svg/51px-SFA_Polygon_with_hole.svg.png) a|

```json
{
    "type": "Polygon",
    "coordinates": [
        [[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]],
        [[20, 30], [35, 35], [30, 20], [20, 30]]
    ]
}
```

* 数据集
* 行政区范围可以使用 `GeoJSON` 中的多边形来表示
* 课程中为大家提供了一份表示了纽约的各个行政区范围的数据集, 叫做 `nyc-borough-boundaries-polygon.geojson`
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190603155616.png)
* 使用步骤
. 创建一个类型 `Feature`, 对应 `JSON` 文件中的格式
. 通过解析 `JSON`, 创建 `Feature` 对象
. 通过 `Feature` 对象创建 `GeoJSON` 表示一个地理位置的 `Geometry` 对象
. 通过 `Geometry` 对象判断一个经纬度是否在其范围内
总结:
* 思路
. 从需求出发, 设计结果集
. 推导结果集所欠缺的字段
. 补齐欠缺的字段, 生成结果集, 需求完成
* 后续整体上要做的事情
* 需求是查看出租车在不同行政区的等待客人的时间
* 需要补充两个点, 一是出租车下客点的行政区名称, 二是等待时间
* 本章节聚焦于行政区的信息补充
* 学习步骤
. 介绍 `JSON` 解析的工具
. 介绍读取 `GeoJSON` 的工具
. `JSON` 解析
. 读取 `GeoJSON`

##### 6.2. 工具介绍

目标和步骤:
目标:
理解 `JSON` 解析和 `Geometry` 解析所需要的工具, 后续使用这些工具补充行政区信息
步骤:
`JSON4S`
`ESRI Geometry`

JSON4S 介绍:
介绍
一般在 `Java` 中, 常使用如下三个工具解析 `JSON`

`Gson`
`Google` 开源的 `JSON` 解析工具, 比较人性化, 易于使用, 但是性能不如 `Jackson`, 也不如 `Jackson` 有积淀
`Jackson`
`Jackson` 是功能最完整的 `JSON` 解析工具, 也是最老牌的 `JSON` 解析工具, 性能也足够好, 但是 `API` 在一开始支持的比较少, 用起来稍微有点繁琐
`FastJson`
阿里巴巴的 `JSON` 开源解析工具, 以快著称, 但是某些方面用起来稍微有点反直觉
什么是 `JSON` 解析
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190603161629.png)

* 读取 `JSON` 数据的时候, 读出来的是一个有格式的字符串, 将这个字符串转换为对象的过程就叫做解析
* 可以使用 `JSON4S` 来解析 `JSON`, `JSON4S` 是一个其它解析工具的 `Scala` 封装以适应 `Scala` 的对象转换
* `JSON4S` 支持 `Jackson` 作为底层的解析工具
* Step 1: 导入 `Maven` 依赖

```xml
<!-- JSON4S -->
<dependency>
    <groupId>org.json4s</groupId>
    <artifactId>json4s-native_2.11</artifactId>
    <version>${json4s.version}</version>
</dependency>
<!-- JSON4S 的 Jackson 集成库 -->
<dependency>
    <groupId>org.json4s</groupId>
    <artifactId>json4s-jackson_2.11</artifactId>
    <version>${json4s.version}</version>
</dependency>
```

* Step 2: 解析 `JSON`

步骤:
. 解析 `JSON` 对象
. 序列化 `JSON` 对象
. 使用 `Jackson` 反序列化 `Scala` 对象
. 使用 `Jackson` 序列化 `Scala` 对象
代码:
新建一个`JsonTest.scala`

```scala

package cn.xhchen.taxi

import org.json4s.jackson.Serialization

object JsonTest {

  def main(args: Array[String]): Unit = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization.{read, write}

    val product =
      """
        |{"name":"Toy","price":35.35}
      """.stripMargin

    // 隐士转换的形式提供格式工具, 例如 如何解析时间字符串
    implicit val formats = Serialization.formats(NoTypeHints)

    // 具体的解析为某一个对象
    val productObj1 = parse(product).extract[Product]

    // 可以通过一个方法, 直接将 JSON 字符串转为对象, 但是这种方式就无法进行搜索了
    val productObj2 = read[Product](product)

    // 将对象转为 JSON 字符串
    val productObj3 = Product("电视", 10.5)
//    val jsonStr1 = compact(render(productObj3))
    val jsonStr = write(productObj3)

    println(jsonStr)
  }
}

```

GeoJSON 读取工具的介绍:

* 介绍
* 读取 `GeoJSON` 的工具有很多, 但是大部分都过于复杂, 有一些只能 `Java` 中用
* 有一个较为简单, 也没有使用底层 `C` 语言开发的解析 `GeoJSON` 的类库叫做 `ESRI Geometry`, `Scala` 中也可以支持
* 使用
`ESRI Geometry` 的使用比较的简单, 大致就如下这样调用即可

```scala
//<1> 读取 `JSON` 生成 `Geometry` 对象
val mg = GeometryEngine.geometryFromGeoJson(jsonStr, 0, Geometry.Type.Unknown)
//<2> 重点: 一个 `Geometry` 对象就表示一个 `GeoJSON` 支持的对象, 可能是一个点, 也可能是一个多边形
val geometry = mg.getGeometry
//<3> 判断一个 `Geometry` 中是否包含另外一个 `Geometry`
GeometryEngine.contains(geometry, other, csr)
```

总结:

* `JSON` 解析
* `FastJSON` 和 `Gson` 直接在 `Scala` 中使用会出现问题, 因为 `Scala` 的对象体系和 `Java` 略有不同
* 最为适合 `Scala` 的方式是使用 `JSON4S` 作为上层 `API`, `Jackson` 作为底层提供 `JSON` 解析能力, 共同实现 `JSON` 解析
* 其使用方式非常简单, 两行即可解析

```scala
implicit val formats = Serialization.formats(NoTypeHints)
val obj = read[Person](product)
```

* `GeoJSON` 的解析
* 有一个很适合 Scala 的 GeoJSON 解析工具, 叫做 `ESRI Geometry`, 其可以将 GeoJSON 字符串转为 Geometry 对象, 易于使用

```scala
GeometryEngine.geometryFromGeoJson(jsonStr, 0, Geometry.Type.Unknown)
```

* 后续工作
. 读取行政区的数据集, 解析 `JSON` 格式, 将 `JSON` 格式的字符串转为对象
. 使用 `ESRI` 的 `GeometryEngine` 读取行政区的 `Geometry` 对象的 `JSON` 字符串, 生成 `Geometry` 对象
. 使用上车点和下车点的坐标创建 `Point` 对象 ( `Geometry` 的子类)
. 判断 `Point` 是否在行政区的 `Geometry` 的范围内 (行政区的 `Geometry` 其实本质上是子类 `Polygon` 的对象)

##### 6.3. 具体实现

目标和步骤:
目标:
通过 `JSON4S` 和 `ESRI` 配合解析提供的 `GeoJSON` 数据集, 获取纽约的每个行政区的范围
步骤:
. 解析 `JSON`
. 使用 `ESRI` 生成表示行政区的一组 `Geometry` 对象
解析 JSON:
步骤
. 对照 `JSON` 中的格式, 创建解析的目标类
. 解析 `JSON` 数据转为目标类的对象
. 读取数据集, 执行解析
Step 1: 创建目标类

* `GeoJSON`

```json
{
  "type": "FeatureCollection",
  "features": [ // <1>
    {
      "type": "Feature",
      "id": 0,
      "properties": {
        "boroughCode": 5,
        "borough": "Staten Island",
        "@id": "http:\/\/nyc.pediacities.com\/Resource\/Borough\/Staten_Island"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-74.050508064032471, 40.566422034160816],
            [-74.049983525625748, 40.566395924928273]
          ]
        ]
      }
    }
  ]
}
```

<1> `features` 是一个数组, 其中每一个 `Feature` 代表一个行政区

* 新建两个目标类

```scala
case class FeatureCollection(
  features: List[Feature]
)

case class Feature(
  id: Int,
  properties: Map[String, String],
  geometry: JObject
)

case class FeatureProperties(boroughCode: Int, borough: String)
```

* Step 2: 将 `JSON` 字符串解析为目标类对象
创建工具类实现功能

```scala
object FeatureExtraction {

  def parseJson(json: String): FeatureCollection = {
    implicit val format: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val featureCollection = read[FeatureCollection](json)
    featureCollection
  }
}
```

* Step 3: 读取数据集, 转换数据

```scala
val geoJson = Source.fromFile("dataset/nyc-borough-boundaries-polygon.geojson").mkString
val features = FeatureExtraction.parseJson(geoJson)
```

解析 GeoJSON:
步骤
. 转换 `JSON` 为 `Geometry` 对象
表示行政区的 JSON 段在哪

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": 0,
      "properties": {
        "boroughCode": 5,
        "borough": "Staten Island",
        "@id": "http:\/\/nyc.pediacities.com\/Resource\/Borough\/Staten_Island"
      },
      "geometry": { // <1>
        "type": "Polygon",
        "coordinates": [
          [
            [-74.050508064032471, 40.566422034160816],
            [-74.049983525625748, 40.566395924928273]
          ]
        ]
      }
    }
  ]
}
```

<1> `geometry` 段即是 `Geometry` 对象的 `JSON` 表示

通过 `ESRI` 解析此段

```scala
case class Feature(
  id: Int,
  properties: Map[String, String],
  geometry: JObject             // <1>
) {

  def getGeometry: Geometry = { // <2>
    GeometryEngine.geoJsonToGeometry(compact(render(geometry)), 0, Geometry.Type.Unknown).getGeometry
  }
}
```

<1> `geometry` 对象需要使用 `ESRI` 解析并生成, 所以此处并没有使用具体的对象类型, 而是使用 `JObject` 表示一个 `JsonObject`, 并没有具体的解析为某个对象, 节省资源
<2> 将 `JSON` 转为 `Geometry` 对象

在出租车 DataFrame 中增加行政区信息:

步骤
. 将 `Geometry` 数据集按照区域大小排序
. 广播 `Geometry` 信息, 发给每一个 `Executor`
. 创建 `UDF`, 通过经纬度获取行政区信息
. 统计行政区信息

* Step 1: 排序 `Geometry`
* 动机: 后续需要逐个遍历 `Geometry` 对象, 取得每条出租车数据所在的行政区, 大的行政区排在前面效率更好一些

```scala
val areaSortedFeatures = features.features.sortBy(feature => {
    (feature.properties("boroughCode"), - feature.getGeometry.calculateArea2D())
  })
```

* Step 2: 发送广播
* 动机: `Geometry` 对象数组相对来说是一个小数据集, 后续需要使用 `Spark` 来进行计算, 将 `Geometry` 分发给每一个 `Executor` 会显著减少 `IO` 通量

```scala
val featuresBc = spark.sparkContext.broadcast(areaSortedFeatures)
```

* Step 3: 创建 `UDF`
* 动机: 创建 UDF, 接收每个出租车数据的下车经纬度, 转为行政区信息, 以便后续实现功能

```scala
val boroughLookUp = (x: Double, y: Double) => {
  val features: Option[Feature] = featuresBc.value.find(feature => {
    GeometryEngine.contains(feature.getGeometry, new Point(x, y), SpatialReference.create(4326))
  })
  features.map(feature => {
    feature.properties("borough")
  }).getOrElse("NA")
}

val boroughUDF = udf(boroughLookUp)
```

* Step 4: 测试转换结果, 统计每个行政区的出租车数据数量
* 动机: 写完功能最好先看看, 运行一下

```scala
taxiClean.groupBy(boroughUDF('dropOffX, 'dropOffY))
  .count()
  .show()
```

总结:
具体的实现分为两个大步骤
. 解析 `JSON` 生成 `Geometry` 数据
. 通过 `Geometry` 数据, 取得每一条出租车数据的行政区信息
`Geometry` 数据的生成又有如下步骤
. 使用 `JSON4S` 解析行政区区域信息的数据集
. 取得其中每一个行政区信息的 `Geometry` 区域信息, 转为 `ESRI` 的 `Geometry` 对象
查询经纬度信息, 获取其所在的区域, 有如下步骤
. 遍历 `Geometry` 数组, 搜索经纬度所表示的 `Point` 对象在哪个区域内
. 返回区域的名称
使用 `UDF` 的目的是为了统计数据集, 后续会通过函数直接完成功能

#### 7. 会话统计

目标和步骤:
目标:

* 统计每个行政区的所有行程, 查看每个行政区平均等候客人的时间
* 掌握会话统计的方式方法

步骤:
. 会话统计的概念
. 功能实现
会话统计的概念:

* 需求分析
* 需求
统计每个行政区的平均等客时间
* 需求可以拆分为如下几个步骤

. 按照行政区分组
. 在每一个行政区中, 找到同一个出租车司机的先后两次订单, 本质就是再次针对司机的证件号再次分组
. 求出这两次订单的下车时间和上车时间只差, 便是等待客人的时间
. 针对一个行政区, 求得这个时间的平均数

* 问题: 分组效率太低
分组的效率相对较低

* 分组是 `Shuffle`
* 两次分组, 包括后续的计算, 相对比较复杂

* 解决方案: 分区后在分区中排序

. 按照 `License` 重新分区, 如此一来, 所有相同的司机的数据就会在同一个分区中
. 计算分区中连续两条数据的时间差
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190813003239.png)
上述的计算存在一个问题, 一个分组会有多个司机的数据, 如何划分每个司机的数据边界? 其实可以先过滤一下, 计算时只保留同一个司机的数据

* 无论是刚才的多次分组, 还是后续的分区, 都是要找到每个司机的会话, 通过会话来完成功能, 也叫做会话分析

功能实现:

* 步骤
. 过滤掉没有经纬度的数据
. 按照 `License` 重新分区并按照 `License` 和 `pickUpTime` 排序
. 求得每个司机的下车和下次上车的时间差
. 求得每个行政区得统计数据

* Step 1: 过滤没有经纬度的数据

```scala
val taxiDone = taxiClean.where("dropOffX != 0 and dropOffY != 0 and pickUpX != 0 and pickUpY != 0")
```

* Step 2: 划分会话

```scala
val sessions = taxiDone.repartition('license)
  .sortWithinPartitions('license, 'pickUpTime)
```

* Step 3: 求得时间差

. 处理每个分区, 通过 `Scala` 的 `API` 找到相邻的数据

```scala
sessions.mapPartitions(trips => {
  val viter = trips.sliding(2)
})
```

--过滤司机不同的相邻数据

```scala
sessions.mapPartitions(trips => {
  val viter = trips.sliding(2)
    .filter(_.size #### 2)
    .filter(p => p.head.license #### p.last.license)
})
```

--求得时间差

```scala
def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
  val borough = boroughLookUp(t1.dropOffX, t1.dropOffY)
  val duration = (t2.pickUpTime - t1.dropOffTime) / 1000
  (borough, duration)
}

val boroughDurations = sessions.mapPartitions(trips => {
  val viter = trips.sliding(2)
    .filter(_.size #### 2)
    .filter(p => p.head.license #### p.last.license)
  viter.map(p => boroughDuration(p.head, p.last))
}).toDF("borough", "seconds")
```

* Step 4: 统计数据

```scala
boroughDurations.where("seconds > 0")
  .groupBy("borough")
  .agg(avg("seconds"), stddev("seconds"))
  .show()
```

总结:

* 其实会话分析的难点就是理解需求
* 需求是找到每个行政区的待客时间, 就是按照行政区分组
* 需求是找到待客时间, 就是按照司机进行分组, 并且还要按照时间进行排序, 才可找到一个司机相邻的两条数据
* 但是分组和统计的效率较低
* 可以把相同司机的所有形成发往一个分区
* 然后按照司机的 `License` 和上车时间综合排序
* 这样就可以找到同一个司机的两次行程之间的差值

## 示例-taxi

### taxi

#### EitherTest.scala

taxi/src/main/scala/cn/xhchen/taxi/EitherTest.scala

```scala
package cn.xhchen.taxi

object EitherTest {

  def main(args: Array[String]): Unit = {
    /**
      * 相当于 Parse 方法
      */
    def process(b: Double): Double = {
      val a = 10.0
      a / b
    }

    // Either => Left Or Right
    // Option => Some None

    def safe(f: Double => Double, b: Double): Either[Double, (Double, Exception)] = {
      try {
        val result = f(b)
        Left(result)
      } catch {
        case e: Exception => Right(b, e)
      }
    }

    //  process(0.0)
    val result = safe(process, 0)
    result.isLeft

    result match {
      case Left(r) => println(r)
      case Right((b, e)) => println(b, e)
    }
  }
}
```

#### Features.scala

taxi/src/main/scala/cn/xhchen/taxi/Features.scala

```scala
package cn.xhchen.taxi

import com.esri.core.geometry.{Geometry, GeometryEngine}
import org.json4s.JsonAST.JObject
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

case class FeatureCollection(features: List[Feature])

case class Feature(properties: Map[String, String], geometry: JObject) {

  def getGeometry(): Geometry = {
    import org.json4s.jackson.JsonMethods._

    val mapGeo = GeometryEngine.geoJsonToGeometry(compact(render(geometry)), 0, Geometry.Type.Unknown)
    mapGeo.getGeometry
  }
}

object FeatureExtraction {

  // 完成具体的 JSON 解析工作
  def parseJson(json: String): FeatureCollection = {
    import org.json4s.jackson.Serialization.read
    // 1. 导入一个 formats 隐式转换
    implicit val formats = Serialization.formats(NoTypeHints)
    // 2. JSON -> Obj
    val featureCollection = read[FeatureCollection](json)
    featureCollection
  }
}
```

#### JsonTest.scala

taxi/src/main/scala/cn/xhchen/taxi/JsonTest.scala

```scala
package cn.xhchen.taxi

import org.json4s.jackson.Serialization

object JsonTest {

  def main(args: Array[String]): Unit = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization.{read, write}

    val product =
      """
        |{"name":"Toy","price":35.35}
      """.stripMargin

    // 隐士转换的形式提供格式工具, 例如 如何解析时间字符串
    implicit val formats = Serialization.formats(NoTypeHints)

    // 具体的解析为某一个对象
    val productObj1 = parse(product).extract[Product]

    // 可以通过一个方法, 直接将 JSON 字符串转为对象, 但是这种方式就无法进行搜索了
    val productObj2 = read[Product](product)

    // 将对象转为 JSON 字符串
    val productObj3 = Product("电视", 10.5)
//    val jsonStr1 = compact(render(productObj3))
    val jsonStr = write(productObj3)

    println(jsonStr)
  }
}

case class Product(name: String, price: Double)
```

#### TaxiAnalysisRunner.scala

taxi/src/main/scala/cn/xhchen/taxi/TaxiAnalysisRunner.scala

```scala
package cn.xhchen.taxi

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.esri.core.geometry.{GeometryEngine, Point, SpatialReference}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.io.Source

object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("taxi")
      .getOrCreate()

    // 2. 导入隐式转换和函数们
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 3. 数据读取
    val taxiRaw: Dataset[Row] = spark.read
      .option("header", value = true)
      .csv("dataset/half_trip.csv")

    // taxiRaw.show()
    // taxiRaw.printSchema()

    // 4. 转换操作
    val taxiParsed: RDD[Either[Trip, (Row, Exception)]] = taxiRaw.rdd.map(safe(parse))
    // 可以通过如下方式来过滤出来所有异常的 row
    // taxiParsed.filter(e => e.isRight)
    //   .map(e => e.right.get._1)
    val taxiGood: Dataset[Trip] = taxiParsed.map(either => either.left.get ).toDS()

    // 5. 绘制时长直方图
    // 5.1 编写 UDF 完成时长计算, 将毫秒转为小时单位
    val hours = (pickUpTime: Long, dropOffTime: Long) => {
      val duration = dropOffTime - pickUpTime
      val hours = TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS)
      hours
    }
    val hoursUDF = udf(hours)
    // 5.2 进行统计
//    taxiGood.groupBy(hoursUDF($"pickUpTime", $"dropOffTime") as "duration")
//      .count()
//      .sort("duration")
//      .show()

    // 6. 根据直方图的显示, 查看数据分布后, 剪除反常数据
    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickUpTime, dropOffTIme) BETWEEN 0 AND 3")
//    taxiClean.show()

    // 7. 增加行政区信息
    // 7.1. 读取数据集
    val geoJson = Source.fromFile("dataset/nyc-borough-boundaries-polygon.geojson").mkString
    val featureCollection = FeatureExtraction.parseJson(geoJson)
    // 7.2. 排序
    // 后续需要得到每一个出租车在哪个行政区, 拿到经纬度, 遍历 features 搜索其所在的行政区
    // 在搜索的过程中, 行政区越大命中的几率就越高, 所以把大的行政区放在前面, 更容易命中, 减少遍历次数
    val sortedFeatures = featureCollection.features.sortBy(feature => {
      (feature.properties("boroughCode"), - feature.getGeometry().calculateArea2D())
    })
    // 7.3. 广播
    val featuresBC = spark.sparkContext.broadcast(sortedFeatures)
    // 7.4. UDF创建, 完成功能
    val boroughLookUp = (x: Double, y: Double) => {
      // 7.4.1. 搜索经纬度所在的行政区
      val featureHit: Option[Feature] = featuresBC.value.find(feature => {
        GeometryEngine.contains(feature.getGeometry(), new Point(x, y), SpatialReference.create(4326))
      })
      // 7.4.2. 转为行政区信息
      val borough = featureHit.map(feature => feature.properties("borough")).getOrElse("NA")
      borough
    }
    // 7.5. 统计信息
//    val boroughUDF = udf(boroughLookUp)
//    taxiClean.groupBy(boroughUDF('dropOffX, 'dropOffY))
//      .count()
//      .show()

    // 8.1. 过滤没有经纬度的数据
    // 8.2. 会话分析
    val sessions = taxiClean.where("dropOffX != 0 and dropOffY != 0 and pickUpX != 0 and pickUpY != 0")
      .repartition('license)
      .sortWithinPartitions('license, 'pickUpTime)

    // 8.3. 求得时间差
    def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val borough = boroughLookUp(t1.dropOffX, t1.dropOffY)
      val duration = (t2.pickUpTime - t1.dropOffTime) / 1000
      (borough, duration)
    }

    val boroughtDuration = sessions.mapPartitions(trips => {
      val viter = trips.sliding(2)
        .filter(_.size == 2)
        .filter(p => p.head.license == p.last.license)
      viter.map(p => boroughDuration(p.head, p.last))
    }).toDF("borough", "seconds")

    boroughtDuration.where("seconds > 0")
      .groupBy("borough")
      .agg(avg('seconds), stddev('seconds))
      .show()
  }

  /**
    * 作用就是封装 parse 方法, 捕获异常
    */
  def safe[P, R](f: P => R): P => Either[R, (P, Exception)] = {
    new Function[P, Either[R, (P, Exception)]] with Serializable {

      override def apply(param: P): Either[R, (P, Exception)] = {
        try {
          Left(f(param))
        } catch {
          case e: Exception => Right((param, e))
        }
      }
    }
  }

  /**
    * Row -> Trip
    */
  def parse(row: Row): Trip = {
    val richRow = new RichRow(row)
    val license = richRow.getAs[String]("hack_license").orNull
    val pickUpTime = parseTime(richRow, "pickup_datetime")
    val dropOffTime = parseTime(richRow, "dropoff_datetime")
    val pickUpX = parseLocation(richRow, "pickup_longitude")
    val pickUpY = parseLocation(richRow, "pickup_latitude")
    val dropOffX = parseLocation(richRow, "dropoff_longitude")
    val dropOffY = parseLocation(richRow, "dropoff_latitude")
    Trip(license, pickUpTime, dropOffTime, pickUpX, pickUpY, dropOffX, dropOffY)
  }

  def parseTime(row: RichRow, field: String): Long = {
    // 1. 表示出来时间类型的格式 SimpleDateFormat
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = new SimpleDateFormat(pattern, Locale.ENGLISH)
    // 2. 执行转换, 获取 Date 对象, getTime 获取时间戳
    val time: Option[String] = row.getAs[String](field)
    val timeOption: Option[Long] = time.map(time => formatter.parse(time).getTime )
    timeOption.getOrElse(0L)
  }

  def parseLocation(row: RichRow, field: String): Double = {
    // 1. 获取数据
    val location = row.getAs[String](field)
    // 2. 转换数据
    val locationOption = location.map( loc => loc.toDouble )
    locationOption.getOrElse(0.0D)
  }
}

/**
  * DataFrame 中的 Row 的包装类型, 主要为了包装 getAs 方法
  * @param row
  */
class RichRow(row: Row) {

  /**
    * 为了返回 Option 提醒外面处理空值, 提供处理方式
    */
  def getAs[T](field: String): Option[T] = {
    // 1. 判断 row.getAs 是否为空, row 中 对应的 field 是否为空
    if (row.isNullAt(row.fieldIndex(field))) {
      // 2. null -> 返回 None
      None
    } else {
      // 3. not null -> 返回 Some
      Some(row.getAs[T](field))
    }
  }
}

case class Trip(
  license: String,
  pickUpTime: Long,
  dropOffTime: Long,
  pickUpX: Double,
  pickUpY: Double,
  dropOffX: Double,
  dropOffY: Double
)
```

#### pom.xml

taxi/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>cn.xhchen</groupId>
    <artifactId>taxi</artifactId>
    <version>0.0.1</version>

    <properties>
        <scala.version>2.11.8</scala.version>
        <spark.version>2.2.0</spark.version>
        <hadoop.version>2.7.5</hadoop.version>
        <slf4j.version>1.7.16</slf4j.version>
        <log4j.version>1.2.17</log4j.version>
        <mysql.version>5.1.35</mysql.version>
        <esri.version>2.2.2</esri.version>
        <json4s.version>3.6.6</json4s.version>
    </properties>

    <dependencies>
        <!-- Scala 库 -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>org.scala-lang.modules</groupId>
            <artifactId>scala-xml_2.11</artifactId>
            <version>1.0.6</version>
        </dependency>

        <!-- Spark 系列包 -->
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
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>

        <!-- 地理位置处理库 -->
        <dependency>
            <groupId>com.esri.geometry</groupId>
            <artifactId>esri-geometry-api</artifactId>
            <version>${esri.version}</version>
        </dependency>

        <!-- JSON 解析库 -->
        <dependency>
            <groupId>org.json4s</groupId>
            <artifactId>json4s-native_2.11</artifactId>
            <version>${json4s.version}</version>
        </dependency>
        <dependency>
            <groupId>org.json4s</groupId>
            <artifactId>json4s-jackson_2.11</artifactId>
            <version>${json4s.version}</version>
        </dependency>

        <!-- 日志相关 -->
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

        <!-- JSON4S -->
        <dependency>
            <groupId>org.json4s</groupId>
            <artifactId>json4s-native_2.11</artifactId>
            <version>${json4s.version}</version>
        </dependency>
        <!-- JSON4S 的 Jackson 集成库 -->
        <dependency>
            <groupId>org.json4s</groupId>
            <artifactId>json4s-jackson_2.11</artifactId>
            <version>${json4s.version}</version>
        </dependency>
    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>

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
        </plugins>

    </build>
</project>
```

##### Taxi.scala

/Taxi.scala

```scala
package cn.xhchen.taxi2

import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.esri.core.geometry.{Geometry, GeometryEngine, Point, SpatialReference}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source
import scala.util.Try

object Taxi {

  def main(args: Array[String]): Unit = {

    //1、创建sparksession
    val spark = SparkSession.builder().master("local[4]").appName("taxi").getOrCreate()

    import spark.implicits._
    //注册udf函数
    spark.udf.register("hours",hours _)
    //2、读取文件
    val cleanDS = spark.read.option("header",true).csv("dataset/half_trip.csv")
    //3、去重、过滤
      .distinct()
      //列值裁剪
      .select("hack_license","pickup_datetime","dropoff_datetime","pickup_longitude",
        "pickup_latitude","dropoff_longitude","dropoff_latitude")
      //过滤异常数据
      .filter(
        """
          |hack_license is not null and hack_license!=''
          |and pickup_datetime is not null and pickup_datetime!=''
          |and dropoff_datetime is not null and dropoff_datetime!=''
          |and pickup_longitude is not null and pickup_longitude!=''
          |and pickup_latitude is not null and pickup_latitude!=''
          |and dropoff_longitude is not null and dropoff_longitude!=''
          |and dropoff_latitude is not null and dropoff_latitude!=''
        """.stripMargin)
    //4、转为dataset[Object]
      .rdd
      //数据转换，因为可能存在报错行为，用Try包裹
      .map(row=> Try(parse(row)))
      //过滤掉解析报错的数据
      .filter(_.isSuccess)
      //获取运行正常的数据
      .map(_.get)
      .toDS
      //只保留行程时间为0-3小时的数据
      .filter("hours(pickup_datetime,dropoff_datetime) BETWEEN 0 AND 3")

    //读取本地geojson文件
    val jsonStr = Source.fromFile("dataset/nyc-borough-boundaries-polygon.geojson").mkString
    //解析json
    val featureCollection: FeatureCollection = parseJson(jsonStr)

    //排序 面积越大的越靠前，越能更大几率命中经纬度
    import scala.collection.JavaConverters._
    val features = featureCollection.features.asScala.sortBy(feature=>(feature.properties.asScala("boroughCode"),-feature.getGeomotry().calculateArea2D()))

    //广播出去 因为每一个task都需要这一份数据进行搜索
    val featuresBc = spark.sparkContext.broadcast(features)

    //创建udf方法对传入经纬度进行搜索找到行政区区号
    def findbBoroughCode(x:Double,y:Double)={
      val code: Option[Feature] = featuresBc.value.find(feature=>{
        GeometryEngine.contains(feature.getGeomotry(),new Point(x,y),SpatialReference.create(4326))

      })
      code.map(feature=>feature.properties.asScala("borough")).getOrElse("NA")
    }

    def borough(t1:Trip,t2:Trip)={
      //根据t1的下车经纬度找到对应的行政区区号
      val boroughCode = findbBoroughCode(t1.dropoff_longitude,t1.dropoff_latitude)
      //等客时间
      val duration = (t2.pickup_datetime - t1.dropoff_datetime)/1000
      (boroughCode,duration)
    }
    import org.apache.spark.sql.functions._
    /*val findbBoroughCodeFunc = udf(findbBoroughCode _)
    cleanDS.groupBy(findbBoroughCodeFunc('dropoff_longitude,'dropoff_latitude)).count().show()*/
    //分区 将同一个司机的数据分到同一个分区中
    cleanDS.where("pickup_longitude != 0 and pickup_latitude != 0 and dropoff_longitude != 0 and dropoff_latitude != 0")
      .repartition('hack_license)
      .sortWithinPartitions('hack_license,'pickup_datetime)
      .mapPartitions(it=>{
        it.sliding(2,1)
          .filter(_.size==2)
          .filter(x=>x.head.hack_license == x.last.hack_license)
          .map(arr=>borough(arr.head,arr.last))
      }).toDF("borough","seconds")
      .where('seconds>0)
      .groupBy('borough)
      .agg(avg('seconds)).show
  }

  /**
    * 解析json
    * @param json
    * @return
    */
  def parseJson(json:String)={

    JSON.parseObject(json,classOf[FeatureCollection])
  }
  /**
    * 计算一次行程的时间为多少小时
    * @param pickUpTime
    * @param dropOffTime
    * @return
    */
  def hours(pickUpTime:Long,dropOffTime:Long)={
    (dropOffTime-pickUpTime)/1000/60/60
  }

  /**
    * 数据转换，解析字段
    * @param row
    * @return
    */
  def parse(row:Row)={
    val hack_license = row.getAs[String]("hack_license")
    val pickup_datetime = parseTime(row,"pickup_datetime")
    val dropoff_datetime = parseTime(row,"dropoff_datetime")
    val pickup_longitude = parseLocation(row,"pickup_longitude")
    val pickup_latitude = parseLocation(row,"pickup_latitude")
    val dropoff_longitude = parseLocation(row,"dropoff_longitude")
    val dropoff_latitude = parseLocation(row,"dropoff_latitude")

    Trip(hack_license,pickup_datetime,dropoff_datetime,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude)
  }
  /**
    * 解析时间，将日期格式的字符串转为毫秒
    * @param row
    * @return
    */
  def parseTime(row:Row,fieldName:String):Long = {

    val dateStr: String = row.getAs[String](fieldName)
    //解析
    val formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    val date: Date = formatter.parse(dateStr)
    date.getTime
  }

  /**
    * 解析经纬度，将字符串经纬度转为double
    * @param row
    * @param fieldName
    * @return
    */
  def parseLocation(row:Row,fieldName:String):Double={
    row.getAs[String](fieldName).toDouble
  }

}

/**
  *
  * @param hack_license 司机执照号
  * @param pickup_datetime 上车时间
  * @param dropoff_datetime 下车时间
  * @param pickup_longitude 上车经度
  * @param pickup_latitude 上车纬度
  * @param dropoff_longitude 下车经度
  * @param dropoff_latitude 下车纬度
  */
case class Trip(
                 hack_license:String,
                 pickup_datetime:Long,
                 dropoff_datetime:Long,
                 pickup_longitude:Double,
                 pickup_latitude:Double,
                 dropoff_longitude:Double,
                 dropoff_latitude:Double
               )
//注意: json解析时case class中的属性名要与json文件中的属性名一致，如果用fastjson解析，那么集合类型必须是java的集合类型
case class FeatureCollection(features: util.List[Feature])

case class Feature(properties: util.Map[String, String], geometry: JSONObject){
  def getGeomotry()={
    val mapGeo = GeometryEngine.geoJsonToGeometry(geometry.toJSONString, 0, Geometry.Type.Unknown)
    mapGeo.getGeometry
  }
}
```

##### Taxi2.scala

/Taxi2.scala

```scala
package cn.xhchen.taxi2

import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.esri.core.geometry.{Geometry, GeometryEngine, Point, SpatialReference}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source
import scala.util.Try

object Taxi2 {

  def main(args: Array[String]): Unit = {

    //1、创建sparksession
    val spark = SparkSession.builder().master("local[4]").appName("taxi").getOrCreate()

    import spark.implicits._
    //注册udf函数
    spark.udf.register("hours",hours _)

    //注册转换时间函数
    spark.udf.register("parseTime",parseTime _)
    //注册经纬度转double函数
    spark.udf.register("parseLocation",parseLocation _)
    //2、读取文件
    val cleanDS = spark.read.option("header",true).csv("dataset/half_trip.csv")
    //3、去重、过滤
      .distinct()
      //过滤异常数据
      .filter(
        """
          |hack_license is not null and hack_license!=''
          |and pickup_datetime is not null and pickup_datetime!=''
          |and dropoff_datetime is not null and dropoff_datetime!=''
          |and pickup_longitude is not null and pickup_longitude!=''
          |and pickup_latitude is not null and pickup_latitude!=''
          |and dropoff_longitude is not null and dropoff_longitude!=''
          |and dropoff_latitude is not null and dropoff_latitude!=''
        """.stripMargin)
      .selectExpr("hack_license","parseTime(pickup_datetime) as pickup_datetime","parseTime(dropoff_datetime) dropoff_datetime"
      ,"parseLocation(pickup_longitude) pickup_longitude","parseLocation(pickup_latitude) pickup_latitude",
        "parseLocation(dropoff_longitude) dropoff_longitude","parseLocation(dropoff_latitude) dropoff_latitude")
      //只保留行程时间为0-3小时的数据
      .filter("hours(pickup_datetime,dropoff_datetime) BETWEEN 0 AND 3  and pickup_datetime!=0 and dropoff_datetime!=0 " +
      "and pickup_longitude != 0 and pickup_latitude != 0 and dropoff_longitude != 0 and dropoff_latitude != 0")
      .createOrReplaceTempView("taxi")

    spark.sql("cache table taxi")
    //读取本地geojson文件
    val jsonStr = Source.fromFile("dataset/nyc-borough-boundaries-polygon.geojson").mkString
    //解析json
    val featureCollection: FeatureCollection = parseJson(jsonStr)

    //排序 面积越大的越靠前，越能更大几率命中经纬度
    import scala.collection.JavaConverters._
    val features = featureCollection.features.asScala.sortBy(feature=>(feature.properties.asScala("boroughCode"),-feature.getGeomotry().calculateArea2D()))

    //广播出去 因为每一个task都需要这一份数据进行搜索
    val featuresBc = spark.sparkContext.broadcast(features)

    //创建udf方法对传入经纬度进行搜索找到行政区区号
    def findbBoroughCode(x:Double,y:Double)={
      val code: Option[Feature] = featuresBc.value.find(feature=>{
        GeometryEngine.contains(feature.getGeomotry(),new Point(x,y),SpatialReference.create(4326))

      })
      code.map(feature=>feature.properties.asScala("borough")).getOrElse("NA")
    }

    //注册根据经纬度查找区域的udf函数
    spark.udf.register("findbBoroughCode",findbBoroughCode _)
    //根据下车经纬度添加区域列
    spark.sql(
      """
        |select
        | hack_license,pickup_datetime,dropoff_datetime,
        | pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,
        | findbBoroughCode(dropoff_longitude,dropoff_latitude) borough,
        | row_number() over(partition by hack_license order by pickup_datetime) rn
        | from taxi
      """.stripMargin).createOrReplaceTempView("taxi_borough")

    spark.sql(
      """
        |select a.borough,avg((b.pickup_datetime-a.dropoff_datetime)/1000) seconds
        | from taxi_borough a inner join  taxi_borough b
        | on a.hack_license = b.hack_license
        | and a.rn+1 = b.rn
        | group by a.borough
      """.stripMargin).show


  }

  /**
    * 解析json
    * @param json
    * @return
    */
  def parseJson(json:String)={

    JSON.parseObject(json,classOf[FeatureCollection])
  }
  /**
    * 计算一次行程的时间为多少小时
    * @param pickUpTime
    * @param dropOffTime
    * @return
    */
  def hours(pickUpTime:Long,dropOffTime:Long)={
    (dropOffTime-pickUpTime)/1000/60/60
  }

  /**
    * 数据转换，解析字段
    * @param row
    * @return
    */

  /**
    * 解析时间，将日期格式的字符串转为毫秒
    * @param row
    * @return
    */
  def parseTime(value:String):Long = {
    //解析
    Try(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(value).getTime).getOrElse(0L)
  }

  /**
    * 解析经纬度，将字符串经纬度转为double
    * @param row
    * @param fieldName
    * @return
    */
  def parseLocation(value:String):Double={
    Try(value.toDouble).getOrElse(0D)
  }

}

/**
  *
  * @param hack_license 司机执照号
  * @param pickup_datetime 上车时间
  * @param dropoff_datetime 下车时间
  * @param pickup_longitude 上车经度
  * @param pickup_latitude 上车纬度
  * @param dropoff_longitude 下车经度
  * @param dropoff_latitude 下车纬度
  */
case class Trip(
                 hack_license:String,
                 pickup_datetime:Long,
                 dropoff_datetime:Long,
                 pickup_longitude:Double,
                 pickup_latitude:Double,
                 dropoff_longitude:Double,
                 dropoff_latitude:Double
               )
//注意: json解析时case class中的属性名要与json文件中的属性名一致，如果用fastjson解析，那么集合类型必须是java的集合类型
case class FeatureCollection(features: util.List[Feature])

case class Feature(properties: util.Map[String, String], geometry: JSONObject){
  def getGeomotry()={
    val mapGeo = GeometryEngine.geoJsonToGeometry(geometry.toJSONString, 0, Geometry.Type.Unknown)
    mapGeo.getGeometry
  }
}
```

##### 回顾.txt

/回顾.txt

```txt
1、无类型算子
  1、选择
    1、select
    2、selectExpr
  2、列操作
  3、groupBy
  4、Column对象
    1、创建
      1、无绑定
        1、 '列名  需要导入 import spark.implicits._
        2、 $"列名" 需要导入 import spark.implicits._
        3、 col("列名") 需要导入org.apache.spark.sql.functions._
        4、 Column("列名") 需要导入org.apache.spark.sql.functions._
      2、有绑定
        1、dataset.col("列名")
        2、dataset.apply("列名")
    2、别名
      as :  '列名  as  "别名"
    3、操作
      like、isin....
2、缺失值
  1、drop
    1、针对一行的所有列
      1、any : 一行中有一列的值为NaN或者null就会删除该行
      2、all: 一行中所有列的值同时为NaN或者null才会删除该行
    2、针对指定的列
      1、any : 一行中只要指定列中有一列的值为NaN或者null就会删除该行
      2、all:  一行中指定列的值同时为NaN或者null才会删除该行
  2、fill
    针对NaN或者null的值进行填充
  3、replace
    对指定的值替换为我们自己指定的值
  4、字符串的缺失值
    1、删除
      where、filter
    2、填充
      select中使用when函数进行值的填充
3、分组
  1、groupBy
    1、使用functions中的函数
    2、GroupedDataSet中的API
  2、rollup
    rollup(A,B,C)
      ==
    group by A,B,C
    union
    group by A,B
    union
    group by A
    union
    group by null
  3、cube
    cube(A,B,C)
     ==
     group by A,B,C
     union
     group by A,B
     union
     group by A,C
     union
     group by B,C
     union
     group by A
     union
     group by B
     union
     group by C
     union
     group by null
4、连接
  1、cross join  笛卡尔积
  2、full outer join  全外连接 = 左表没有连接上的数据 + 能够连接上的数据 + 右表没有连接上的数据
  3、inner join = 能够连接上的数据
  4、left join = 左表没有连接上的数据 + 能够连接上的数据
  5、right join = 能够连接上的数据 + 右表没有连接上的数据
  6、left anti = 左表没有连接上的数据
  7、left semi = 左表能够连接上的数据
5、自定义udf函数
  1、自定一个函数或者一个方法
  2、将自定义函数或者方法注册到spark:  spark.udf.register("函数名",函数)
6、开窗函数
  1、row_number() over(partition by ..  order by ...)
  2、rank() over(partition by ..  order by ...)
  3、dense_rank() over(partition by ..  order by ...)
  4、max(列名) over()
```

