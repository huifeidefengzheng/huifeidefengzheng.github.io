## DMP:项目代码总结

### 1: application.conf

````properties
#设置sparksql shuffle分区数(默认200)
spark.sql.shuffle.partitions="200"
#sparksql 小表广播出去的大小限制(默认10M)
spark.sql.autoBroadcastJoinThreshold="10485760"
#shuffle的时候是否启动压缩
spark.shuffle.compress="true"
#shuffle拉取数据失败，会自动重试 默认3次
spark.shuffle.io.maxRetries = "3"
#shuffle拉取数据重试间隔时间
spark.shuffle.io.retryWait="5s"
#spark 序列化
spark.serializer="org.apache.spark.serializer.KryoSerializer"
#设置执行与存储的内存比例
spark.memory.fraction="0.6"
#设置存储的内存比例
spark.memory.storageFraction="0.5"
#设置spark core的shuffle的分区数
spark.default.parallelism="10"
#设置数据本地化等待时间
spark.locality.wait="3s"
#是否启动推测机制
spark.speculation.flag="true"
#推测机制启动时机
spark.speculation.multiplier="1.5"
#指定纯真数据库名称
IP_FILE="qqwry.dat"
#指定纯真数据库路径
INSTALL_DIR="D:\\DMP\\dmp_class_09\\src\\main\\resources"
#解析经纬度配置文件
GeoLiteCity.dat = "D:\\DMP\\dmp_class_09\\src\\main\\resources\\GeoLiteCity.dat"
#指定kudumaster地址
kudu.master = "hadoop01:7051,hadoop02:7051,hadoop03:7051"
#ods表名
ods = "ODS_%s"
#商圈库URL
URL="http://restapi.amap.com/v3/geocode/regeo?location=%s&key=260ad39cf66cc70fd3afa09c64e18050"
#app字典文件路径
APPID_NAME="D:\\DMP\\dmp_class_09\\src\\main\\resources\\appID_name"
#设备字典文件路径
DEVICE_DIC="D:\\DMP\\dmp_class_09\\src\\main\\resources\\devicedic"
#标签衰减系数
attenu = "0.9"
````



### 2: ConfigUtils.scala

```scala
package cn.itcast.utils

import com.typesafe.config.ConfigFactory

/**
  * 配置文件参数获取帮助类
  */
object ConfigUtils {
  //加载配置
  val conf = ConfigFactory.load()
  //设置sparksql shuffle分区数
  val SPARK_SQL_SHUFFLE_PARTITIONS= conf.getString("spark.sql.shuffle.partitions")
  //设置sparksql 自动广播时小表大小限制
  val SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD= conf.getString("spark.sql.autoBroadcastJoinThreshold")
  //设置spark shuffle是否启动压缩机制
  val SPARK_SHUFFLE_COMPRESS= conf.getString("spark.shuffle.compress")
 //spark shuffle失败时自动重试次数
  val SPARK_SHUFFLE_IO_MAXRETRIES = conf.getString("spark.shuffle.io.maxRetries")
  //spark shuffle失败时重试间隔
  val SPARK_SHUFFLE_IO_RETRYWAIT= conf.getString("spark.shuffle.io.retryWait")
  //spark序列化方式
  val SPARK_SERIALIZER= conf.getString("spark.serializer")
  //spark 执行与存储的内存比例
  val SPARK_MEMORY_FRACTION= conf.getString("spark.memory.fraction")
  //spark存储的内存比例
  val SPARK_MEMORY_STORAGEFRACTION= conf.getString("spark.memory.storageFraction")
  //spark rdd shuffle默认分区数
  val SPARK_DEFAULT_PARALLELISM= conf.getString("spark.default.parallelism")
  //spark 本地化等待时间
  val SPARK_LOCALITY_WAIT= conf.getString("spark.locality.wait")
  //spark 是否启动推测机制
  val SPARK_SPECULATION= conf.getString("spark.speculation.flag")
  //spark推测机制的启动时机
  val SPARK_SPECULATION_MULTIPLIER= conf.getString("spark.speculation.multiplier")

  //纯真数据库名称
  val IP_FILE = conf.getString("IP_FILE")
  //纯真数据库路径
  val INSTALL_DIR = conf.getString("INSTALL_DIR")
  //经纬度解析配置文件
  val GEOLITECITY_DAT = conf.getString("GeoLiteCity.dat")
  //指定kudu master地址
  val KUDU_MASTER = conf.getString("kudu.master")
  //指定ods表名
  val ODS = conf.getString("ods")
  //获取商圈库URL
  val URL = conf.getString("URL")
  //app字典文件路径
  val APPID_NAME = conf.getString("APPID_NAME")
  //设备字典文件路径
  val DEVICE_DIC = conf.getString("DEVICE_DIC")
  //标签衰减系数
  val ATTENU = conf.getString("attenu")
}
```



### 3: EtlProcess.scala

```scala
package cn.itcast.etl

import cn.itcast.utils.{ConfigUtils, DateUtils, IPAddressUtils, KuduUtils}
import com.maxmind.geoip.{Location, LookupService}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * etl 补全经纬度、省份、城市
  */
object EtlProcess {

  val KUDU_MASTER = "hadoop01:7051,hadoop02:7051,hadoop03:7051"
  //指定数据存入哪个表
  val SINK_TABLE = s"ODS_${DateUtils.getNow()}"

  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("etl")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    val kuduContext = new KuduContext(KUDU_MASTER,spark.sparkContext)
    import spark.implicits._
    //2、读取数据
    val source: DataFrame = -        |t.initbidprice,
        |t.adpayment,
        |t.agentrate,
        |t.lomarkrate,
        |t.adxrate,
        |t.title,
        |t.keywords,
        |t.tagid,
        |t.callbackdate,
        |t.channelid,
        |t.mediatype,
        |t.email,
        |t.tel,
        |t.sex,
        |t.age
        | from t_source t left join ip_info a
        | on t.ip = a.ip
      """.stripMargin)
    //4、落地ods
    //指定主键
    val keys = Seq[String]("ip")
    //指定分区字段
    val columns = keys
    //指定schema
    val schema = result.schema
    //数据写入Kudu
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)

  }
}

```



### 4: AppAnaylysis.scala

```scala
package cn.itcast.indicators

import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的APP分布情况
  */
object AppAnaylysis {
  //指定存入的表名
  val SINK_TABLE = s"app_analysis_${DateUtils.getNow()}"
  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("AppAnaylysis")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()
    import org.apache.kudu.spark.kudu._
    //2、读取ODS表
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      .kudu
      .createOrReplaceTempView("ods")
    //3、指标统计
    spark.sql(
      """
        |select t.appid,t.appname,
        |  sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_request_num,
        |  sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) ad_request_num,
        |  sum(case when requestmode=1 and processnode=3 then 1 else 0 end) valid_request_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1
        |   and adorderid!=0 then 1 else 0 end) bid_num,
        |  sum(case when iswin=1 and isbilling=1 and iseffective=1 and adplatformproviderid>=100000 then 1 else 0 end) bid_success_num,
        |  sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_show_num,
        |  sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) ad_click_num,
        |  sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        |  sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) ad_cost,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) ad_consumtion
        | from ods t
        | group by t.appid,t.appname
      """.stripMargin).createOrReplaceTempView("tmp")
    //3.2  根据上一步的结果，计算竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | t.appid,t.appname,
        | t.org_request_num,t.ad_request_num,t.valid_request_num,
        | t.bid_num,t.bid_success_num,t.bid_success_num/t.bid_num bid_rate,
        | t.ad_show_num,t.ad_click_num,t.media_show_num,t.media_click_num,
        | t.media_click_num/t.media_show_num media_click_rate,t.ad_cost,t.ad_consumtion
        | from tmp t
        |
      """.stripMargin)
    //4、数据落地
    //指定主键
    val keys  =Seq[String]("appid")
    //指定分区字段
    val columns = Seq[String]("appid")
    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)

    //指定表的schema
    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)
  }
}
```



### 5: TagAgg.scala

````scala
package cn.itcast.agg

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

/**
  * 标签聚合
  */
object TagAgg {

  def agg(userGraph: RDD[(VertexId, (String, List[String], Map[String, Double]))])={

    //1、根据aagid进行分组
    val groupedUser: RDD[(VertexId, Iterable[(String, List[String], Map[String, Double])])] = userGraph.groupByKey()
    //2、相同标签进行权重累加
    val userTag = groupedUser.map{
      case (aggid,it:Iterable[(String, List[String], Map[String, Double])])=>
        //[[52:54:00:41:d3:25,929440642823248],[ZPKVVWPBLPRHAAFB,52:54:00:41:d3:25],[ZPKVVWPBLPRHAAFB,CDZVYXDHJMTWVAOSMKZZTUOHLQEXAXPMYJPMHTVC,52:54:00:41:d3:25]]
        //===>[52:54:00:41:d3:25,929440642823248，ZPKVVWPBLPRHAAFB，CDZVYXDHJMTWVAOSMKZZTUOHLQEXAXPMYJPMHTVC]
        //取得用户的所有标识，因为多条数据可能存在相同的标识，所有要进行去重
        val userids = it.map(_._2).flatten.toList.distinct
        val userid = userids.head
        //取得用户所有标签
        //[[(KW-李冰冰,1),(APP-抖音,1)],[（KW-范彬彬，1）,(APP-快手,1)],[(KW-李冰冰,1),(CT-南京,1)]]
        //想要结果:[(KW-李冰冰,2)，(APP-抖音,1),（KW-范彬彬，1）,(APP-快手,1),(CT-南京,1)]
        val tags = it.map(_._3.toList)
          //[(KW-李冰冰,1),(APP-抖音,1),（KW-范彬彬，1）,(APP-快手,1),(KW-李冰冰,1),(CT-南京,1)]
          .flatten
          //[(KW-李冰冰,List((KW-李冰冰,1),(KW-李冰冰,1))),
          // (KW-范彬彬，List(（KW-范彬彬，1）)，
          //(APP-快手,List((APP-快手,1))),
          //(CT-南京,List((CT-南京)))
          // ]
          .groupBy(_._1)
          .map(item=>{
            //将相同标签的权重累加
            val attr = item._2.map(_._2).sum
            (item._1,attr)
          })
        (userid,(userids,tags))
    }

    userTag

  }
}

````



### 6: TagAttenu.scala

```scala
package cn.itcast.attenu

import cn.itcast.utils.ConfigUtils
import org.apache.spark.sql.DataFrame

/**
  * 历史数据标签衰减
  */
object TagAttenu {

  def attenu(yeasterDayData: DataFrame)={

    //1、将历史数据中alluserids，tag两列进行转换，转换为List与Map格式
    yeasterDayData.rdd.map(row=>{
      //取出userid
      val userid = row.getAs[String]("userid")
      //取出所有用户标识
      val allUserIds = row.getAs[String]("allUserIds")
      //取出所有标签
      val tagsStr = row.getAs[String]("tags")
      //将用户所有标识字符串转换为List
      val allIds = allUserIds.split(",").toList
      //(BA-沙滩,1.0),(BA-沙滩,1.0),(BA-沙滩,1.0) =>
      //[(BA-沙滩,1.0),(BA-沙滩,1.0),(BA-沙滩,1.0)]
      val tags: Map[String, Double] = tagsStr.substring(1,tagsStr.length-1).split("\\),\\(").map(str=>{
        //str:(BA-沙滩,1.0
        //[BA-沙滩,1.0]
        val arr = str.split(",")
        //获取标签名
        val tagName = arr(0)
        /**
          * 标签衰减借用牛顿冷却定律,随着时间的流逝，标签的权重慢慢降低。
          *     公式:当前权重=历史权重 x exp（-（衰减系数） x 时间间隔）
          *
          *     公式 = 历史权重 * 衰减系数 [0.9]
          *
          */
          //进行标签衰减
          val attr = try{

          arr(1).toDouble * ConfigUtils.ATTENU.toDouble
        }  catch {
          case e:Exception=>
            println(s"=================>${str}")
            0.0
        }
        (tagName,attr)
      }).toMap
      (userid,(allIds,tags))
    })
  }
}

```



### 7: BusinessArea.scala

```scala
package cn.itcast.business

import ch.hsr.geohash.GeoHash
import cn.itcast.utils.{ConfigUtils, DateUtils, HttpUtils, KuduUtils}
import com.alibaba.fastjson.JSON
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

/**
  * 生成商圈库
  */
object BusinessArea {

  //定义商圈库表名
  val SINK_TABLE = "business_area"

  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]")
      .appName("BusinessArea")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)
    import spark.implicits._
    import org.apache.kudu.spark.kudu._

    //注册udf函数
    spark.udf.register("createGeoCode",createGeoCode _)
    //2、获取ODS
    val source = spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      .kudu

    //获取商圈表数据
    var businessArea:DataFrame = null
    if(kuduContext.tableExists(SINK_TABLE))
      {
        businessArea = spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
          .option("kudu.table",SINK_TABLE)
          .kudu
      }
    //3、数据处理
    //3.1  列裁剪  获取经纬度
    val distinctDF = source.selectExpr("longitude","latitude")
    //3.2  过滤 去掉经纬度为空的数据
      .filter("longitude is not null and latitude is not null")

      //70.12345 -40.12345  ax123
      //70.12346 -40.12346  ax123
      .selectExpr("longitude","latitude","createGeoCode(longitude,latitude) geo_code")
    //3.3  去重
      .distinct()
    //3.4  通过经纬度获取商圈
    //如果商圈表之前没有创建过，那么就直接将所有经纬度都获取到商圈库信息
    var result:DataFrame = null


    if(businessArea==null){
      result = distinctDF.rdd.map(row=>{
        getBusinessArea(row)
      }).toDF("geo_code","areas")
        .filter("areas is not null and areas!=''")
        .distinct()

    }else{
      businessArea.createOrReplaceTempView("business_area")

      distinctDF.createOrReplaceTempView("ods")

      val leftDF = spark.sql(
        """
          |select s.*
          | from ods s left join business_area a
          | on s.geo_code = a.geo_code
          | where a.geo_code is null
        """.stripMargin)
      /**ods
        * ax123  70.123  -40.123
        * ax124  75.123  -45.123
        *
        * business_area
        * ax123  中关村
        */

      result = leftDF.rdd.map(row=>{
        getBusinessArea(row)
      }).toDF("geo_code","areas")
        .filter("areas is not null and areas!=''")
        .distinct()

    }

    //4、将数据落地到kudu
    /**
      * geoCode    商圈列表
      * xxxxxx     商圈名1,商圈名2,...
      */
    //指定主键
    val keys = Seq[String]("geo_code")
    //指定分区字段
    val column = Seq[String]("geo_code")
    //指定schema信息
    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,column,kuduContext,schema)
  }

  /**
    * 生成geoHash编码
    * @param longitude
    * @param latitude
    */
  def createGeoCode(longitude:Float,latitude:Float)={

    GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)
  }

  /**
    * 解析json
    * @param json
    * @return
    */
  def parseJson(json:String):String={

    import scala.collection.JavaConverters._
    Try(JSON.parseObject(json)
      .getJSONObject("regeocode")
      .getJSONObject("addressComponent")
      .getJSONArray("businessAreas")
      .toJavaList(classOf[BusinessAreaObj])
      .asScala
    //[BusinessAreaObj,BusinessAreaObj,BusinessAreaObj]  => [name,name,name]
      .map(_.name)
      .mkString(",")).getOrElse("")
  }

  /**
    * 生成商圈信息
    * @param row
    * @return
    */
  def getBusinessArea(row:Row)={
    //经度
    val longitude = row.getAs[Float]("longitude")
    //纬度
    val latitude = row.getAs[Float]("latitude")
    //geoHash编码
    val geo_code = row.getAs[String]("geo_code")
    //获取url
    val url = ConfigUtils.URL.format(s"${longitude},${latitude}")

    //通过http请求获取商圈库
    val jsonResult: String = HttpUtils.get(url)

    //3.5  解析json
    val areas = parseJson(jsonResult)

    //数据返回
    (geo_code,areas)
  }
}

case class BusinessAreaObj(id:String,name:String,location:String)

```



### 8: UserGraph.scala

````scala
package cn.itcast.graph

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
  * 实现统一用户识别
  */
object UserGraph {

  def graphx(tagRdd: RDD[(String, (List[String], Map[String, Double]))])={

    /*
     各个字符串的hashCode值:
     aaaaaa1.hashCode = 123451
     aaaaaa2.hashCode = 123452
     aaaaaa3.hashCode = 123453
     aaaa.hashCode=123461
     bbbb.hashCode=123462
     cccc.hashCode=123463
     dddd.hashCode=123464

     eeeee1.hashCode = 345671
     eeeee2.hashCode = 345672
     eeeee.hashCode = 345681
     uuuuuu.hashCode = 345682
     ooooooooo.hashCode = 345683
  */
    //1、创建点
    /**(aaaaaa1,(List(aaaaaa1,aaaa,bbbb),Map(.....)))  =>(123451,(aaaaaa1,List(aaaaaa1,aaaa,bbbb),Map(.....)))
      * (aaaaaa2,(List(aaaaaa2,aaaa,cccc),Map(.....))) =>(123452,(aaaaaa2,List(aaaaaa2,aaaa,cccc),Map(.....)))
      * (aaaaaa3,(List(aaaaaa3,aaaa,dddd),Map(.....))) =>(123453,(aaaaaa3,List(aaaaaa3,aaaa,dddd),Map(.....)))
      * (eeeee1,(List(eeeee1,eeeee,uuuuuu),Map(.....))) =>(345671,(eeeee1,List(eeeee1,eeeee,uuuuuu),Map(.....)))
      * (eeeee2,(List(eeeee2,eeeee,ooooooooo),Map(.....))) =>(345672,(eeeee2,List(eeeee2,eeeee,ooooooooo),Map(.....)))
      */
    val vertext: RDD[(Long, (String, List[String], Map[String, Double]))] = tagRdd.map {
      case (userId,(allUserIds,tags)) =>
        (userId.hashCode.toLong,(userId,allUserIds,tags))
    }

    //2、创建边
    /**(aaaaaa1,(List(aaaaaa1,aaaa,bbbb),Map(.....)))  => allUserIds.map(id=>Edge[Int](userId.hashCode.toLong,id.hashCode.toLong))
      *          => List[Edge[Int](123451,123451),Edge[Int](123451,123461),Edge[Int](123451,123462)]
      * (aaaaaa2,(List(aaaaaa2,aaaa,cccc),Map(.....))) =>
      *          => List[Edge[Int](123452,123452),Edge[Int](123452,123461),Edge(123452,123463)]
      * (aaaaaa3,(List(aaaaaa3,aaaa,dddd),Map(.....))) =>
      *         => List[Edge[Int](123453,123453),Edge[Int](123453,123461),Edge[Int](123453,123464)]
      * (eeeee1,(List(eeeee1,eeeee,uuuuuu),Map(.....))) =>
      *         => List[Edge[Int](345671,345671),Edge[Int](345671,345681),Edge[Int](345671,345682)]
      * (eeeee2,(List(eeeee2,eeeee,ooooooooo),Map(.....))) =>
      *         => List[Edge[Int](345672,345672),Edge[Int](345672,345681),Edge[Int](345672,345683)]
      */
    val edge: RDD[Edge[Int]] = tagRdd.flatMap {
      case (userId,(allUserIds,tags)) =>{
        //userid与用户所有标识中的每一个标识都组成一个边
        allUserIds.map(id=>Edge[Int](userId.hashCode.toLong,id.hashCode.toLong))
      }
    }
    //3、构建图
    val graph = Graph(vertext,edge)
    //4、生成连通图
    val connectGraph: Graph[VertexId, Int] = graph.connectedComponents()
    //5、数据返回
    //connectGraph.vertices  =>  (id,aggid)
    //(id,aggid) join (id,(userid,alluserIds,tags)) => (id,(aggid,(userid,alluserids,tags)))
    /**
      *  connectGraph.vertices
      */
    connectGraph.vertices.join(vertext)
      .map {
        case (idHashCode,(aggid,(userid,alluserIds,tags))) =>
          (aggid,(userid,alluserIds,tags))
      }
  }
}

````



### 9: AdChannelAnalysis.scala

````scala
package cn.itcast.indicators

import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的渠道分布情况
  */
object AdChannelAnalysis {

  //数据存入的表名
  val SINK_TABLE = s"ad_channel_analysis_${DateUtils.getNow()}"
  def main(args: Array[String]): Unit = {
    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("AdChannelAnalysis")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    import org.apache.kudu.spark.kudu._
    //2、读取ODS表的数据
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      .kudu
      .createOrReplaceTempView("ods")
    //3、指标统计
    spark.sql(
      """
        |select t.channelid,
        |  sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_request_num,
        |  sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) ad_request_num,
        |  sum(case when requestmode=1 and processnode=3 then 1 else 0 end) valid_request_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1
        |   and adorderid!=0 then 1 else 0 end) bid_num,
        |  sum(case when iswin=1 and isbilling=1 and iseffective=1 and adplatformproviderid>=100000 then 1 else 0 end) bid_success_num,
        |  sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_show_num,
        |  sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) ad_click_num,
        |  sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        |  sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) ad_cost,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) ad_consumtion
        | from ods t
        | group by t.channelid
      """.stripMargin).createOrReplaceTempView("tmp")
    //3.2  根据上一步的结果，计算竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | t.channelid,
        | t.org_request_num,t.ad_request_num,t.valid_request_num,
        | t.bid_num,t.bid_success_num,t.bid_success_num/t.bid_num bid_rate,
        | t.ad_show_num,t.ad_click_num,t.media_show_num,t.media_click_num,
        | t.media_click_num/t.media_show_num media_click_rate,t.ad_cost,t.ad_consumtion
        | from tmp t
      """.stripMargin)
    //4、数据落地
    //指定表的主键
    val keys = Seq[String]("channelid")
    //指定表的分区字段
    val columns = Seq[String]("channelid")
    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)

    //指定表的schema信息
    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)
  }
}

````



### 10: AdDeviceTypeAnalysis.scala

````scala
package cn.itcast.indicators

import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的设备分布情况
  */
object AdDeviceTypeAnalysis {
  //指定数据存储表名
  val SINK_TABLE = s"ad_device_type_analysis_${DateUtils.getNow()}"
  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("AdDeviceTypeAnalysis")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    import org.apache.kudu.spark.kudu._
    //2、读取数据
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      .kudu.createOrReplaceTempView("ods")
    //3、指标统计
    spark.sql(
      """
        |select case when t.client=1 then 'android '
        |       when t.client=2 then 'ios' else 'wp' end client,
        |   t.device,
        |  sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_request_num,
        |  sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) ad_request_num,
        |  sum(case when requestmode=1 and processnode=3 then 1 else 0 end) valid_request_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1
        |   and adorderid!=0 then 1 else 0 end) bid_num,
        |  sum(case when iswin=1 and isbilling=1 and iseffective=1 and adplatformproviderid>=100000 then 1 else 0 end) bid_success_num,
        |  sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_show_num,
        |  sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) ad_click_num,
        |  sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        |  sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) ad_cost,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) ad_consumtion
        | from ods t
        | group by t.client,t.device
      """.stripMargin).createOrReplaceTempView("tmp")
    //3.2  根据上一步的结果，计算竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | t.client,t.device,
        | t.org_request_num,t.ad_request_num,t.valid_request_num,
        | t.bid_num,t.bid_success_num,t.bid_success_num/t.bid_num bid_rate,
        | t.ad_show_num,t.ad_click_num,t.media_show_num,t.media_click_num,
        | t.media_click_num/t.media_show_num media_click_rate,t.ad_cost,t.ad_consumtion
        | from tmp t
        |
      """.stripMargin)
    //4、数据落地
    //指定主键
    val keys = Seq[String]("client","device")
    //指定分区字段
    val columns = Seq[String]("client")

    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)

    //指定表的schema
    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)
  }
}

````



### 11: AdNetworkAnalysis.scala

```scala
package cn.itcast.indicators

import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的网络类型分布情况
  */
object AdNetworkAnalysis {

  //指定数据存入的表名
  val SINK_TABLE = s"ad_network_analysis_${DateUtils.getNow()}"
  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("AdNetworkAnalysis")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    import org.apache.kudu.spark.kudu._
    //2、读取ODS表
    spark.read
      .option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      .kudu
      .createOrReplaceTempView("ods")
    //3、指标统计
    spark.sql(
      """
        |select t.networkmannerid,t.networkmannername,
        |  sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_request_num,
        |  sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) ad_request_num,
        |  sum(case when requestmode=1 and processnode=3 then 1 else 0 end) valid_request_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1
        |   and adorderid!=0 then 1 else 0 end) bid_num,
        |  sum(case when iswin=1 and isbilling=1 and iseffective=1 and adplatformproviderid>=100000 then 1 else 0 end) bid_success_num,
        |  sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_show_num,
        |  sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) ad_click_num,
        |  sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        |  sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) ad_cost,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) ad_consumtion
        | from ods t
        | group by t.networkmannerid,t.networkmannername
      """.stripMargin).createOrReplaceTempView("tmp")
    //3.2  根据上一步的结果，计算竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | t.networkmannerid,t.networkmannername,
        | t.org_request_num,t.ad_request_num,t.valid_request_num,
        | t.bid_num,t.bid_success_num,t.bid_success_num/t.bid_num bid_rate,
        | t.ad_show_num,t.ad_click_num,t.media_show_num,t.media_click_num,
        | t.media_click_num/t.media_show_num media_click_rate,t.ad_cost,t.ad_consumtion
        | from tmp t
        |
      """.stripMargin)
    //4、写入kudu
    //指明表的主键
    val keys = Seq[String]("networkmannerid")
    //指明分区字段
    val columns = Seq[String]("networkmannerid")

    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)

    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)
  }
}

```



### 12: AdOperatorAnalysis.scala

````scala
package cn.itcast.indicators

import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的网络类型分布情况
  */
object AdNetworkAnalysis {

  //指定数据存入的表名
  val SINK_TABLE = s"ad_network_analysis_${DateUtils.getNow()}"
  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("AdNetworkAnalysis")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    import org.apache.kudu.spark.kudu._
    //2、读取ODS表
    spark.read
      .option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      .kudu
      .createOrReplaceTempView("ods")
    //3、指标统计
    spark.sql(
      """
        |select t.networkmannerid,t.networkmannername,
        |  sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_request_num,
        |  sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) ad_request_num,
        |  sum(case when requestmode=1 and processnode=3 then 1 else 0 end) valid_request_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1
        |   and adorderid!=0 then 1 else 0 end) bid_num,
        |  sum(case when iswin=1 and isbilling=1 and iseffective=1 and adplatformproviderid>=100000 then 1 else 0 end) bid_success_num,
        |  sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_show_num,
        |  sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) ad_click_num,
        |  sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        |  sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) ad_cost,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) ad_consumtion
        | from ods t
        | group by t.networkmannerid,t.networkmannername
      """.stripMargin).createOrReplaceTempView("tmp")
    //3.2  根据上一步的结果，计算竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | t.networkmannerid,t.networkmannername,
        | t.org_request_num,t.ad_request_num,t.valid_request_num,
        | t.bid_num,t.bid_success_num,t.bid_success_num/t.bid_num bid_rate,
        | t.ad_show_num,t.ad_click_num,t.media_show_num,t.media_click_num,
        | t.media_click_num/t.media_show_num media_click_rate,t.ad_cost,t.ad_consumtion
        | from tmp t
        |
      """.stripMargin)
    //4、写入kudu
    //指明表的主键
    val keys = Seq[String]("networkmannerid")
    //指明分区字段
    val columns = Seq[String]("networkmannerid")

    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)

    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)
  }
}

````



### 13: AdRegionAnalysis.scala

````scala
package cn.itcast.indicators

import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.spark.sql.SparkSession

/**
  * 统计广告投放的地域分布情况
  */
object AdRegionAnalysis {
  //指定数据存入表名
  val SINK_TABLE = s"ad_region_analysis_${DateUtils.getNow()}"
  def main(args: Array[String]): Unit = {
    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("AdRegionAnalysis")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    import org.apache.kudu.spark.kudu._
    //2、读取ods表数据
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      .kudu
      .createOrReplaceTempView("ods")
    //3、统计分析
    //3.1  统计原始请求数、广告请求数、有效请求数、竞价数、竞价成功数、广告成本、广告消费、展示量、点击量
    spark.sql(
      """
        |select t.province,t.city,
        |  sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_request_num,
        |  sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) ad_request_num,
        |  sum(case when requestmode=1 and processnode=3 then 1 else 0 end) valid_request_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1
        |   and adorderid!=0 then 1 else 0 end) bid_num,
        |  sum(case when iswin=1 and isbilling=1 and iseffective=1 and adplatformproviderid>=100000 then 1 else 0 end) bid_success_num,
        |  sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_show_num,
        |  sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) ad_click_num,
        |  sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        |  sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) ad_cost,
        |  sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) ad_consumtion
        | from ods t
        | group by t.province,t.city
      """.stripMargin).createOrReplaceTempView("tmp")
    //3.2  根据上一步的结果，计算竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | t.province,t.city,
        | t.org_request_num,t.ad_request_num,t.valid_request_num,
        | t.bid_num,t.bid_success_num,t.bid_success_num/t.bid_num bid_rate,
        | t.ad_show_num,t.ad_click_num,t.media_show_num,t.media_click_num,
        | t.media_click_num/t.media_show_num media_click_rate,t.ad_cost,t.ad_consumtion
        | from tmp t
        |
      """.stripMargin)

    //4、数据落地
    //指定表的主键
    val keys = Seq[String]("province","city")
    //指定表的分区字段
    val columns = Seq[String]("province")

    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)
    //指定表的schema
    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)
  }
}

````



### 14: ProviceCityAnalysis.scala

```scala
package cn.itcast.indicators

import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

//统计各省市的地域分布情况
object ProviceCityAnalysis {

  //指定kudumaster地址
  val KUDU_MASTER = ConfigUtils.KUDU_MASTER
  //指定数据读取表名
  val SOURCE_TABLE = ConfigUtils.ODS.format(DateUtils.getNow())
  //指定数据存入的表名
  val SINK_TABLE = s"province_city_analysis_${DateUtils.getNow()}"
  def main(args: Array[String]): Unit = {

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("ProviceCityAnalysis")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION)
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
      .getOrCreate()

    val kuduContext = new KuduContext(KUDU_MASTER,spark.sparkContext)
    import spark.implicits._
    import org.apache.kudu.spark.kudu._
    //2、读取ODS表数据
    spark.read.option("kudu.master",KUDU_MASTER)
      .option("kudu.table",SOURCE_TABLE)
      .kudu
      .createOrReplaceTempView("ods")
    //3、统计分析
    val result = spark.sql(
      """
        |select a.province,a.city,count(1) cn
        | from ods a
        | group by a.province,a.city
      """.stripMargin)
    //4、数据落地
    //主键字段
    val keys = Seq[String]("province","city")
    //分区字段
    val columns = Seq[String]("province")
    //指定表的schema
    val schema = result.schema
    KuduUtils.write(result,SINK_TABLE,keys,columns,kuduContext,schema)
  }
}

```



### 15: AgeTag.scala

````scala
package cn.itcast.tag

import org.apache.spark.sql.Row

/**
  * 生成年龄标签
  */
object AgeTag {

  def makeTag(row:Row)={
    var tag = Map[String,Double]()
    //1、读取年龄字段的值
    val age = row.getAs[String]("age")
    //2、生成标签
    tag = tag.+((s"AGE-${age}",1))
    //3、数据返回
    tag
  }
}

````



### 16: AppTag.scala

```scala
package cn.itcast.tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 生成标签
  */
object AppTag {

  def makeTag(row:Row,appBc:Broadcast[Map[String, String]])={
    var tags = Map[String,Double]()

    //1、取出广播变量的值
    val appIdNames: Map[String, String] = appBc.value
    //2、取出appid、appname字段的值
    val appid = row.getAs[String]("appid")
    val appName = row.getAs[String]("appname")
    //3、判断appName是否为空，如果为空，从字典文件中获取appname,如果不为空，则使用原本的appName
    val appTagName = Option(appName) match {
      case Some(name) => name
      case None=> appIdNames.getOrElse(appid,"")
    }
    //4、生成标签，给它默认的权重 1
    tags = tags.+((s"APP-${appTagName}",1))
    //5、返回标签数据
    tags
  }

}

```



### 17: BusinessaAreaTag.scala

````scala
package cn.itcast.tag

import java.sql.PreparedStatement

import ch.hsr.geohash.GeoHash
import cn.itcast.utils.JdbcUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 生成商圈标签
  */
object BusinessaAreaTag {

  /*def makeTag(row:Row)={
    var tag = Map[String,Double]()
    //1、取出经纬度
    //经度
    val longitude = row.getAs[Float]("longitude")
    //纬度
    val latitude = row.getAs[Float]("latitude")
    if(longitude!=null && latitude!=null){

      //2、根据经纬度生成geo_code
      val geoCode = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)
      //3、根据geo_code获取对应商圈列表
      //3.1  用jdbc的方式读取geo_code对应的商圈
      val areas = JdbcUtils.getAreas(geoCode)
      //4、切割商圈列表的字符串，每个商圈一个标签
      if(StringUtils.isNoneBlank(areas)){
        areas.split(",").foreach(item=>tag = tag.+((s"BA-${item}",1)))
      }
    }

    //5、数据返回
    tag

  }*/

  /*def makeTag(row:Row,statement:PreparedStatement)={
    var tag = Map[String,Double]()
    //1、取出经纬度
    //经度
    val longitude = row.getAs[Float]("longitude")
    //纬度
    val latitude = row.getAs[Float]("latitude")
    if(longitude!=null && latitude!=null){

      //2、根据经纬度生成geo_code
      val geoCode = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)
      //3、根据geo_code获取对应商圈列表
      //3.1  用jdbc的方式读取geo_code对应的商圈
      //val areas = JdbcUtils.getAreas(geoCode)
      statement.setString(1,geoCode)

      val resultSet = statement.executeQuery()

      var areas = ""
      while (resultSet.next()){
        areas = resultSet.getString("areas")
      }
      //4、切割商圈列表的字符串，每个商圈一个标签
      if(StringUtils.isNoneBlank(areas)){
        areas.split(",").foreach(item=>tag = tag.+((s"BA-${item}",1)))
      }
    }

    //5、数据返回
    tag

  }*/

  def makeTag(row:Row)={
    var tag = Map[String,Double]()
    //1、取出商圈列表
    val areas = row.getAs[String]("areas")
    //2、如果商圈列表不为空，切割，生成商圈标签
    if(StringUtils.isNoneBlank(areas)){
      areas.split(",").foreach(item=>tag = tag.+((s"BA-${item}",1)))
    }
    //3、数据返回
    tag
  }
}

````



### 18: ChannelTag.scala

````scala
package cn.itcast.tag

import org.apache.spark.sql.Row

/**
  * 生成渠道标签
  */
object ChannelTag {

  def makeTag(row:Row)={
    var tag = Map[String,Double]()
    //1、取出渠道值
    val channel = row.getAs[String]("channelid")
    //2、生成标签
    tag = tag.+((s"CN-${channel}",1))
    //3、数据返回
    tag
  }
}

````



### 19: DeviceTag.scala

```scala
package cn.itcast.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 生成设备标签
  */
object DeviceTag {

  def makeTag(row:Row,deviceBc:Broadcast[Map[String, String]])={
    var tags = Map[String,Double]()
    //1、取出广播变量值
    val device: Map[String, String] = deviceBc.value
    //2、取出【设备类型1、设备类型2、设备型号、运营商、联网方式】
    //设备型号
    val deviceType_01 = row.getAs[String]("device")
    //设备类型1
    var os:String = row.getAs[Long]("client").toString
    //设备运营商
    var isp = row.getAs[String]("ispname")
    //联网方式
    var networkName = row.getAs[String]("networkmannername")
    //设备类型
    val deviceType = row.getAs[Long]("devicetype").toString

    val deviceTypeName = deviceType match {
      case "1" => "手机"
      case "2" => "平板"
      case _ => "other"
    }
    //3、根据广播变量将设备类型的值转换为企业内部编码
    os = device.getOrElse(os,"other")
    //4、根据广播变量将运营商的值转换为企业内部编码
    isp = device.getOrElse(isp,"other")
    //5、根据广播变量将联网方式的值转换为企业内部编码
    networkName = device.getOrElse(networkName,"other")
    //6、生成设备标签
    tags = tags.+((s"OS-${os}",1))
    tags = tags.+((s"ISP-${isp}",1))
    tags = tags.+((s"NW-${networkName}",1))
    tags = tags.+((s"DT-${deviceTypeName}",1))
    tags = tags.+((s"DE-${deviceType_01}",1))
    //7、数据返回
    tags
  }
}

```



### 20: KeyWordsTag.scala

````scala
package cn.itcast.tag

import org.apache.spark.sql.Row

/**
  * 生成关键字标签
  */
object KeyWordsTag {

  def makeTag(row:Row)={
    var tag = Map[String,Double]()
    //1、取出关键字
    val keywords = row.getAs[String]("keywords")
    //2、将关键字拆分，生成标签
    keywords.split(",").foreach(item=> tag=tag.+((s"KW-${item}",1)))
    //3、数据返回
    tag
  }
}

````



### 21: RegionTag.scala

```scala
package cn.itcast.tag

import org.apache.spark.sql.Row

/**
  * 生成地域标签
  */
object RegionTag {

  def makeTag(row:Row)={
    var tag = Map[String,Double]()
    //1、取出省、城市的值
    val province = row.getAs[String]("province")
    val city = row.getAs[String]("city")
    //2、生成标签
    tag = tag.+((s"PV-${province}",1))
    tag = tag.+((s"CT-${city}",1))
    //3、数据返回
    tag
  }
}

```



### 22: SexTag.scala

```scala
package cn.itcast.tag

import org.apache.spark.sql.Row

/**
  * 生成性别标签
  */
object SexTag {

  def makeTag(row:Row)={
    var tag = Map[String,Double]()
    //1、取出性别字段值
    val sex = row.getAs[String]("sex")
    val sexName = sex match {
      case "1" => "男"
      case "0" => "女"
      case _ =>"中性"
    }
    //2、生成标签
    tag = tag.+((s"SEX-${sexName}",1))
    //3、数据返回
    tag
  }
}

```



### 23: TagProcess.scala

```scala
package cn.itcast.tag

import java.sql.{Connection, DriverManager, PreparedStatement}

import ch.hsr.geohash.GeoHash
import cn.itcast.agg.TagAgg
import cn.itcast.attenu.TagAttenu
import cn.itcast.graph.UserGraph
import cn.itcast.utils.{ConfigUtils, DateUtils, KuduUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 数据标签化
  */
object TagProcess {

  //定义商圈表名称
  val SOURCE_TABLE = "business_area"

  //指定历史表的表名
  val YESTER_DAY_TABLE = s"tag_${DateUtils.getYeasterDayString()}"
  def main(args: Array[String]): Unit = {

    /**
      * 标签字段：
      * 普通标签:  APP、设备【设备类型1、设备类型2、设备型号、运营商、联网方式】、地域、关键字、channelid、性别、年龄、商圈
      * 特殊标签:  用户消费能力、信用评价
      *
      *    用户标识
      */

    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("TagProcess")
      .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS) //------
      .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
      .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS) //---
      .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
      .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
      .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER) //-------
      .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION) //-------------
      .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION) //------------
      .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT) //-----
      .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM) //------
      .config("spark.speculation",ConfigUtils.SPARK_SPECULATION) //
      .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)//-----
      .getOrCreate()

    import spark.implicits._
    import org.apache.kudu.spark.kudu._
    //2、读取数据
    val source = spark.read
      .option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",ConfigUtils.ODS.format(DateUtils.getNow()))
      //ODS_%s
      .kudu

    //读取商圈表的数据
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",SOURCE_TABLE)
      .kudu
      .createOrReplaceTempView("business_area")


    //前置操作
    //过滤 过滤用户标识全部空的数据
    /**
      * imei手机串码mac手机MAC码idfa手机APP的广告码openudid苹果设备的识别码androidid安卓设备的识别码
      */
    val filterOds: Dataset[Row] = source.filter(
      """
        |(imei is not null and imei !='') or
        | (mac is not null and mac!='') or
        | (idfa is not null and idfa!='') or
        | (openudid is not null and openudid!='') or
        | (androidid is not null and androidid!='')
      """.stripMargin)

    //将数据注册成临时表然后与商圈表join获取经纬度对应的商圈列表
    filterOds.createOrReplaceTempView("ods")

    //自定义udf函数，通过经纬度创建geoCode
    def getGeoCode(longitude:Float,latitude:Float):String={
      GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)
    }

    //注册udf函数
    spark.udf.register("getGeoCode",getGeoCode _)

    //缓存小表
    spark.sql("cache table business_area")
    val df = spark.sql(
      """
        |select s.*,b.areas
        | from ods s left join business_area b
        | on getGeoCode(s.longitude,s.latitude) = b.geo_code
      """.stripMargin)

    //3、生成标签
    //3.1 需要将app字典文件读取之后广播出去
    val appSource: Dataset[String] = spark.read.textFile(ConfigUtils.APPID_NAME)

    val appMap: Map[String, String] = appSource.map(line => {
      val arr = line.split("##")
      (arr(0), arr(1))
    }).collect().toMap
    //广播app字典文件
    val appBc: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(appMap)
    //3.2 需要设备字典文件读取之后广播出去
    val deviceMap: Map[String, String] = spark.read.textFile(ConfigUtils.DEVICE_DIC)
      .map(line=>{
        val arr = line.split("##")
        (arr(0),arr(1))
      }).collect().toMap

    val deviceBc = spark.sparkContext.broadcast(deviceMap)
    //3.3 生成标签
    /*filterOds.rdd.map(row=>{
      //1、app标签-APP
      val appTags = AppTag.makeTag(row,appBc)
      //2、设备标签【设备类型1-、设备类型2、设备型号、运营商、联网方式】
      val deviceTag = DeviceTag.makeTag(row,deviceBc)
      //3、地域【省份、城市】
      val regionTag = RegionTag.makeTag(row)
      //4、关键字
      val keywordTag = KeyWordsTag.makeTag(row)
      // 5、channelid
      val channelTag = ChannelTag.makeTag(row)
      // 6、性别、
      val sexTag = SexTag.makeTag(row)
      // 7、年龄、
      val ageTag = AgeTag.makeTag(row)
      // 8、商圈
      val areaTag = BusinessaAreaTag.makeTag(row)
      //用户标识
      val allUserIds = getAllUserIds(row)
       //所有标签
      val allTags = appTags ++ deviceTag ++ regionTag ++ keywordTag ++ channelTag ++ sexTag ++ ageTag ++ areaTag

      (allUserIds.head,(allUserIds,allTags))
    }).repartition(1).saveAsTextFile("d:/tags")*/

    /*filterOds.rdd.mapPartitions(it=>{

      Class.forName("com.cloudera.impala.jdbc41.Driver")
      var connection:Connection = null
      var statement:PreparedStatement = null
      var result = List[(String,(List[String],Map[String,Double]))]()
      try{
        connection = DriverManager.getConnection("jdbc:impala://hadoop01:21050/default")

        statement = connection.prepareStatement("select areas from business_area where geo_code=?")

        while (it.hasNext){
          val row = it.next()
          //1、app标签-APP
          val appTags = AppTag.makeTag(row,appBc)
          //2、设备标签【设备类型1-、设备类型2、设备型号、运营商、联网方式】
          val deviceTag = DeviceTag.makeTag(row,deviceBc)
          //3、地域【省份、城市】
          val regionTag = RegionTag.makeTag(row)
          //4、关键字
          val keywordTag = KeyWordsTag.makeTag(row)
          // 5、channelid
          val channelTag = ChannelTag.makeTag(row)
          // 6、性别、
          val sexTag = SexTag.makeTag(row)
          // 7、年龄、
          val ageTag = AgeTag.makeTag(row)
          // 8、商圈
          val areaTag = BusinessaAreaTag.makeTag(row,statement)
          //用户标识
          val allUserIds = getAllUserIds(row)
          //所有标签
          val allTags = appTags ++ deviceTag ++ regionTag ++ keywordTag ++ channelTag ++ sexTag ++ ageTag ++ areaTag

          result = result.+:(allUserIds.head,(allUserIds,allTags))
        }
      }catch {
        case e:Exception=> result.iterator
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null)
          connection.close()
      }
      result.iterator
    }).repartition(1).saveAsTextFile("d:/tags")*/

    val tagRdd: RDD[(String, (List[String], Map[String, Double]))] = df.rdd.map(row=>{
      //1、app标签-APP
      val appTags = AppTag.makeTag(row,appBc)
      //2、设备标签【设备类型1-、设备类型2、设备型号、运营商、联网方式】
      val deviceTag = DeviceTag.makeTag(row,deviceBc)
      //3、地域【省份、城市】
      val regionTag = RegionTag.makeTag(row)
      //4、关键字
      val keywordTag = KeyWordsTag.makeTag(row)
      // 5、channelid
      val channelTag = ChannelTag.makeTag(row)
      // 6、性别、
      val sexTag = SexTag.makeTag(row)
      // 7、年龄、
      val ageTag = AgeTag.makeTag(row)
      // 8、商圈
      val areaTag = BusinessaAreaTag.makeTag(row)
      //用户标识
      val allUserIds = getAllUserIds(row)
      //所有标签
      val allTags = appTags ++ deviceTag ++ regionTag ++ keywordTag ++ channelTag ++ sexTag ++ ageTag ++ areaTag

      (allUserIds.head,(allUserIds,allTags))
    })//.repartition(1).saveAsTextFile("d:/tags")
    //4.统一用户识别
    val userGraph: RDD[(VertexId, (String, List[String], Map[String, Double]))] = UserGraph.graphx(tagRdd)
    //userGraph.repartition(1).saveAsTextFile("d:/graph")
	
	//5、标签聚合
    val currentData: RDD[(String, (List[String], Map[String, Double]))] = TagAgg.agg(userGraph)
	//currentData.repartition(1).saveAsTextFile("d:/currentdata")
	
    //6、读取历史数据
    val yeasterDayData: DataFrame = spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",YESTER_DAY_TABLE)
      .kudu

    //7、历史数据标签衰减
    val yeasterDay: RDD[(String, (List[String], Map[String, Double]))] = TagAttenu.attenu(yeasterDayData)

    //8、合并历史数据与当前数据
    val allData: RDD[(String, (List[String], Map[String, Double]))] = currentData.union(yeasterDay)

    //9、将合并后的数据再次进行统一用户识别
    val allGraph: RDD[(VertexId, (String, List[String], Map[String, Double]))] = UserGraph.graphx(allData)

    //10、将合并后并且进行统一用户识别的结果再次进行标签合并
    TagAgg.agg(allGraph).repartition(1).saveAsTextFile("d:/allgraph")
    /**
      * 构造历史数据
      */
    //将用户所有标识与用户所有标签转为string
   /* val result = agg.map(item=>{
      val userid =  item._1
      //得到用户所有标识
      val allUser = item._2._1
      //得到用户所有标识的字符串
      val allUserIds = allUser.mkString(",")
      // (k,v)
      val tags = item._2._2.toList.mkString(",")
      (userid,allUserIds,tags)
    }).toDF("userid","allUserIds","tags")
    //指定表的主键
    val keys = Seq[String]("userid")
    //指定分区字段
    val columns = keys

    val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)

    //指定表的schema
    val schema = result.schema
    KuduUtils.write(result,YESTER_DAY_TABLE,keys,columns,kuduContext,schema)*/
  }

  /**
    * 获取用户的所有标识
    * @param row
    */
  def getAllUserIds(row:Row)={
    var ids = List[String]()
    //1、取出用户的标识
    val imei = row.getAs[String]("imei")
    val mac = row.getAs[String]("mac")
    val idfa = row.getAs[String]("idfa")
    val openudid = row.getAs[String]("openudid")
    val androidid = row.getAs[String]("androidid")
    //2、判断用户标识是否为空，如果为空，丢掉，只保留非空的用户标识
    if(StringUtils.isNotBlank(imei)){
      ids = ids.+:(imei)
    }

    if(StringUtils.isNotBlank(mac)){
      ids = ids.+:(mac)
    }

    if(StringUtils.isNotBlank(idfa)){
      ids = ids.+:(idfa)
    }

    if(StringUtils.isNotBlank(openudid)){

      ids = ids.+:(openudid)
    }

    if(StringUtils.isNotBlank(androidid)){
      ids = ids.+:(androidid)
    }
    //3、用户标识返回
    ids
  }
}
```

### 24: DateUtils.scala

```scala
package cn.itcast.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  /**
    * 获取当前时间yyyyMMdd格式的字符串
    */
  def getNow()={
    val date = new Date()

    val formatter = FastDateFormat.getInstance("yyyyMMdd")

    formatter.format(date)
  }

  /**
    * 获取前一天的yyyyMMdd格式的字符串
    */
  def getYeasterDayString()={
    //获取今天的时间
    val date = new Date()

    //
    val calendar = Calendar.getInstance()

    calendar.setTime(date)

    //进行日期的加减法
    calendar.add(Calendar.DAY_OF_YEAR,-1)

    val formatter = FastDateFormat.getInstance("yyyyMMdd")

    formatter.format(calendar)
  }
}

```



### 25: HttpUtils.scala

```scala
package cn.itcast.utils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

object HttpUtils {

  /**
    * 发起get请求
    * @param url
    * @return
    */
  def get(url:String):String ={
    //1、创建HttpClient
    val client = new HttpClient()
    //2、创建请求方式
    val getMethod = new GetMethod(url)
    //3、发起请求
    val code: Int = client.executeMethod(getMethod)
    //4、返回结果
    if(code==200){
      getMethod.getResponseBodyAsString
    }else{
      ""
    }
  }
}

```

### 26: IPAddressUtils.java

```java
package cn.itcast.utils;

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



### 27: IPLocation.java

```java
package cn.itcast.utils;

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



### 28: JdbcUtils.scala

````java
package cn.itcast.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * jdbc帮助类
  */
object JdbcUtils {
  /**
    * 根据geo_code获取对应的商圈列表
    * @param geoCode
    */
  def getAreas(geoCode:String)={

    //1、加载驱动
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    //2、获取连接
    var connection:Connection = null

    var statement:PreparedStatement = null

    var areas = ""
    try{

      connection = DriverManager.getConnection("jdbc:impala://hadoop01:21050/default")
      //3、获取statement对象
      statement = connection.prepareStatement("select areas from business_area where geo_code=?")

      statement.setString(1,geoCode)

      //4、执行查询
      val resultSet = statement.executeQuery()
      while (resultSet.next()){
        areas = resultSet.getString("areas")
      }
    }catch {
      case e:Exception=> ""
    }finally {
      //关闭资源
      if(statement!=null)
        statement.close()
      if(connection!=null){
        connection.close()
      }
    }

    //5、返回查询结果
    areas
  }
}

````



### 29: KuduUtils.scala

```scala
package cn.itcast.utils

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * 写入数据到kudu
  */
object KuduUtils {

  def write(data:DataFrame,tableName:String,keys:Seq[String]
            ,partitionColumns:Seq[String],kuduContext:KuduContext,schema:StructType)={
    //如果表不存在，需要提前将表创建好
    if(!kuduContext.tableExists(tableName)){
      val options = new CreateTableOptions
      //设置分区规则以及分区数
      import scala.collection.JavaConverters._
      options.addHashPartitions(partitionColumns.asJava,3)
      //设置副本数
      options.setNumReplicas(1)
      //创建表
      kuduContext.createTable(tableName,schema,keys,options)
    }

    //写入数据

    //data.write.option("kudu.master","").option("kudu.table",tableName).kudu
    kuduContext.insertRows(data,tableName)
  }

}

```



### 30: Util.java

```java
package cn.itcast.utils;

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



### 31: DataTest.scala

```scala
import cn.itcast.utils.ConfigUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit.Test

import scala.util.Random

/**
  * 假设总数据条数100W条，task数量=1000， 正常来说每个task处理的数据量应该是 = 100W/1000
  *  如果出现有一个task处理的数据条数=50W，其余999个task处理的数据量比较平均 = 50W/999
  *
  *
  * 出现数据倾斜的场景:
  *     1、在join的时候，有很多数据的join的值为空值，那么这时候所有空值的数据都会分配到一个task中从而出现数据倾斜
  *         解决方案: 过滤空值
  *     2、当分区数设置过小，导致很多key聚集到一个分区从而导致数据倾斜
  *         解决方案:增大分区数
  *     3、某一个key特别多的groupBy的时候出现倾斜
  *         解决方案:局部聚合+全局聚合
  *     4、大表join小表，因为大表中某一个key的数据特别多的时候，也会出现数据倾斜
  *         解决方案:将小表广播出去，避免shuffle操作
  *     5、大表join大表的时候，由于某一个或者某几个key特别多的时候，也会出现数据倾斜
  *         解决方案: 将产生数据倾斜的key过滤出来，进行单独处理，其余没有出现数据倾斜的key照常处理
  *     6、大表join大表的时候，有很多的key数据量都比较大，那这些key都会导致数据倾斜
  *         解决方案: 将表进行扩容
  */
class DataTest extends Serializable {


  val spark = SparkSession.builder()
    .master("local[4]")
    .config("spark.sql.shuffle.partitions",5)
    .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
    .appName("test").getOrCreate()
  import spark.implicits._

  /**
    * 空值过多导致的数据倾斜
    */
  @Test
  def solution1(): Unit ={

    val student = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (1,"小狗",20,""),
      (2,"小红",21,""),
      (3,"小明",22,""),
      (9,"赵留",28,"class_02"),
      (4,"小米",23,""),
      (5,"天猫",24,""),
      (6,"李思思",25,""),
      (10,"王霸",29,"class_03"),
      (7,"王强",26,""),
      (8,"钱琪",27,"")
    )).toDF("id","name","age","class_id")


    val clazz = spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","python班"),
      ("class_02","java班"),
      ("class_03","大数据班")
    )).toDF("id","name")

    student.filter("class_id is not null and class_id !=''").createOrReplaceTempView("student")
    clazz.createOrReplaceTempView("clazz")

    spark.sql(
      """
        |select  s.id,s.name,c.name
        | from student s left join clazz c
        | on s.class_id = c.id
      """.stripMargin).rdd.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index}  data:${it.toBuffer}")
      it
    }).collect()
  }

  /**
    * 某一个key特别多的groupBy的时候出现倾斜
    */
  @Test
  def solution2(): Unit ={


    val student = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (1,"小狗",20,"class_01"),
      (2,"小红",21,"class_01"),
      (3,"小明",22,"class_01"),
      (4,"小米",23,"class_01"),
      (5,"天猫",24,"class_01"),
      (6,"李思思",25,"class_01"),
      (7,"王强",26,"class_02"),
      (8,"钱琪",27,"class_02"),
      (9,"赵留",28,"class_02"),
      (10,"王霸",29,"class_03")
    )).toDF("id","name","age","class_id")
    //注册udf函数
    spark.udf.register("prfix",prfix _)
    spark.udf.register("unprfix",unprfix _)

    student.selectExpr("id","name","age","prfix(class_id) class_id").createOrReplaceTempView("student")
    //局部聚合 - 就是将加上随机数的字段groupby
    spark.sql(
      """
        |select s.class_id,count(1) cn
        | from student s
        | group by s.class_id
      """.stripMargin).createOrReplaceTempView("tmp")

    //全局聚合 --在局部聚合基础上，去掉groupby字段的随机数之后再次groupby
    spark.sql(
      """
        |select unprfix(t.class_id),sum(t.cn)
        | from tmp t
        | group by unprfix(t.class_id)
      """.stripMargin)

  }

  /**
    * 添加随机数后缀
    * @param classId
    * @return
    */
  def prfix(classId:String):String={
    s"${classId}#${Random.nextInt(10)}"
  }

  /**
    * 去掉随机数后缀
    * @param classId
    * @return
    */
  def unprfix(classId:String):String={
    classId.split("#").head
  }

  /**
    * 大表join小表，因为大表中某一个key的数据特别多的时候，也会出现数据倾斜
    */
  @Test
  def solution3(): Unit ={


    val student = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (9,"赵留",28,"class_02"),
      (1,"小狗",20,"class_01"),
      (7,"王强",26,"class_02"),
      (2,"小红",21,"class_01"),
      (3,"小明",22,"class_01"),
      (10,"王霸1",29,"class_03"),
      (4,"小米",23,"class_01"),
      (11,"王霸2",29,"class_03"),
      (5,"天猫",24,"class_01"),
      (6,"李思思",25,"class_01"),
      (8,"钱琪",27,"class_02"),
      (12,"王霸3",29,"class_03"),
      (13,"王霸4",29,"class_03")
    )).toDF("id","name","age","class_id")


    val clazz = spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","python班"),
      ("class_02","java班"),
      ("class_03","大数据班")
    )).toDF("id","name")

    student.rdd.mapPartitionsWithIndex((index,it)=>{
      println(s"index：${index}  data:${it.toBuffer}")
      it
    }).collect()
    student.createOrReplaceTempView("student")
    clazz.createOrReplaceTempView("clazz")

    //缓存表
    spark.sql("cache table clazz")
    spark.sql(
      """
        |select s.id,s.name,c.name
        | from student s left join clazz c
        | on s.class_id=c.id
      """.stripMargin).rdd.mapPartitionsWithIndex((index,it)=>{
      println(s"index：${index}  data:${it.toBuffer}")
      it
    }).collect()

    /**
      * join之前
      * index：2  data:ArrayBuffer([4,小米,23,class_01], [11,王霸2,29,class_03], [5,天猫,24,class_01])
      * index：3  data:ArrayBuffer([6,李思思,25,class_01], [8,钱琪,27,class_02], [12,王霸3,29,class_03], [13,王霸4,29,class_03])
      * index：0  data:ArrayBuffer([9,赵留,28,class_02], [1,小狗,20,class_01], [7,王强,26,class_02])
      * index：1  data:ArrayBuffer([2,小红,21,class_01], [3,小明,22,class_01], [10,王霸1,29,class_03])
      *
      * join之后
      * index：0  data:ArrayBuffer([9,赵留,java班], [1,小狗,python班], [7,王强,java班])
      * index：1  data:ArrayBuffer([2,小红,python班], [3,小明,python班], [10,王霸1,大数据班])
      * index：3  data:ArrayBuffer([6,李思思,python班], [8,钱琪,java班], [12,王霸3,大数据班], [13,王霸4,大数据班])
      * index：2  data:ArrayBuffer([4,小米,python班], [11,王霸2,大数据班], [5,天猫,python班])
      */
  }

  /**
    * 大表join大表的时候，由于某一个或者某几个key特别多的时候，也会出现数据倾斜
    */
  @Test
  def solution4(): Unit ={

    val student = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (9,"赵留",28,"class_02"),
      (1,"小狗",20,"class_01"),
      (15,"小狗5",20,"class_01"),
      (16,"小狗6",20,"class_01"),
      (17,"小狗7",20,"class_01"),
      (18,"小狗8",20,"class_01"),
      (7,"王强",26,"class_02"),
      (2,"小红",21,"class_01"),
      (3,"小明",22,"class_01"),
      (10,"王霸1",29,"class_03"),
      (4,"小米",23,"class_01"),
      (11,"王霸2",29,"class_03"),
      (5,"天猫",24,"class_01"),
      (6,"李思思",25,"class_01"),
      (8,"钱琪",27,"class_02"),
      (12,"王霸3",29,"class_03"),
      (13,"王霸4",29,"class_03")
    )).toDF("id","name","age","class_id")


    val clazz = spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","python班"),
      ("class_02","java班"),
      ("class_03","大数据班")
    )).toDF("id","name")

    //将自定义添加随机数后缀的方法定义为udf函数
    spark.udf.register("prfix",prfix _)
    spark.udf.register("addPrefix",addPrefix _)
    //class_01的数据特别多导致join的时候 class_01的数据全部聚在一个分区导致数据倾斜
    //通过数据采样，识别是哪些key出现数据倾斜
    //student.sample(false,0.2).show
    //过滤出产生数据倾斜的key
    student.filter("class_id='class_01'").selectExpr("id","name","age","prfix(class_id)  class_id")
      .createOrReplaceTempView("student_solution")

    /**
      * (1,"小狗",20,"class_01#1"),
      * (15,"小狗5",20,"class_01#2"),
      * (16,"小狗6",20,"class_01#3"),
      * (17,"小狗7",20,"class_01#6"),
      * (18,"小狗8",20,"class_01#2"),
      * (2,"小红",21,"class_01#3"),
      * (3,"小明",22,"class_01#5"),
      * (4,"小米",23,"class_01#8"),
      * (5,"天猫",24,"class_01#9"),
      * (6,"李思思",25,"class_01#1")
      *
      * (class_01#1,"python班")
      * (class_01#0,"python班")
      * (class_01#2,"python班")
      * (class_01#3,"python班")
      * (class_01#4,"python班")
      * (class_01#5,"python班")
      * (class_01#6,"python班")
      * (class_01#7,"python班")
      * (class_01#8,"python班")
      * (class_01#9,"python班")
      */
    //没有产生数据倾斜的key的数据
    student.filter("class_id!='class_01'").createOrReplaceTempView("student_other")
    //筛选出student表中出现倾斜的key的数据
    val data: Dataset[Row] = clazz.filter("id='class_01'")
    //筛选出student表中没有出现倾斜的key的数据
    clazz.filter("id!='class_01'").createOrReplaceTempView("clazz_other")
    //对表进行扩容，不然与student_solution表关联不上
    capacity(data).createOrReplaceTempView("clazz_solution")
    //对于没有产生数据倾斜的key的数据正常处理即可
    spark.sql(
      """
        |select  s.id,s.name,c.name c_name
        | from student_other s left join clazz_other c
        | on s.class_id = c.id
      """.stripMargin).createOrReplaceTempView("tmp1")
    //对于产生数据倾斜的key的数据单独处理
    spark.sql(
      """
        |select s.id,s.name,c.name c_name
        | from student_solution s left join clazz_solution c
        | on s.class_id = c.id
      """.stripMargin).createOrReplaceTempView("tmp2")


    spark.sql(
      """
        |select a.id,a.name,a.c_name from tmp1 a
        |union
        |select b.id,b.name,b.c_name from tmp2 b
      """.stripMargin).show

  }

  /**
    * 对dataFrame进行扩容
    * @param data
    */
  def capacity(data:DataFrame)={
    //创建空的dataFrame
    var emptiy = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],data.schema)

    for (i<- 0 until(10)){
      emptiy = emptiy.union(data.selectExpr(s"addPrefix(id,${i}) id","name"))
    }

    emptiy
  }

  /**
    * 添加一个指定的后缀
    * @param classId
    * @param i
    */
  def addPrefix(classId:String,i:Int)={
    s"${classId}#${i}"
  }

  /**
    * 大表join大表的时候，有很多的key数据量都比较大，那这些key都会导致数据倾斜
     */
  def solution5(): Unit ={

    //将自定义添加随机数后缀的方法定义为udf函数
    spark.udf.register("prfix",prfix _)
    spark.udf.register("addPrefix",addPrefix _)

    val student = spark.sparkContext.parallelize(Seq[(Int,String,Int,String)](
      (9,"赵留",28,"class_02"),
      (1,"小狗",20,"class_01"),
      (15,"小狗5",20,"class_01"),
      (16,"小狗6",20,"class_01"),
      (17,"小狗7",20,"class_01"),
      (18,"小狗8",20,"class_01"),
      (7,"王强",26,"class_02"),
      (2,"小红",21,"class_01"),
      (3,"小明",22,"class_01"),
      (10,"王霸1",29,"class_03"),
      (4,"小米",23,"class_01"),
      (11,"王霸2",29,"class_03"),
      (5,"天猫",24,"class_01"),
      (6,"李思思",25,"class_01"),
      (8,"钱琪",27,"class_02"),
      (12,"王霸3",29,"class_03"),
      (13,"王霸4",29,"class_03")
    )).toDF("id","name","age","class_id")
      .selectExpr("id","name","age","prfix(class_id) class_id")


    val clazz = spark.sparkContext.parallelize(Seq[(String,String)](
      ("class_01","python班"),
      ("class_02","java班"),
      ("class_03","大数据班")
    )).toDF("id","name")

    //大表join大表的时候，如果要对整张表进行扩容，一般是扩容10倍以内
    val clazzNew = capacity(clazz)

    clazzNew.createOrReplaceTempView("clazz")
    student.createOrReplaceTempView("student")

    spark.sql(
      """
        |select s.id,s.name,c.name
        | from student s left join clazz c
        | on s.class_id = c.id
      """.stripMargin)
  }

}

```



### 32: KuduTest.scala

```scala
import cn.itcast.utils.{ConfigUtils, DateUtils}
import com.typesafe.config.ConfigUtil
import org.apache.spark.sql.SparkSession

object KuduTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()

    import org.apache.kudu.spark.kudu._

    spark.read.option("kudu.master","hadoop01:7051,hadoop02:7051,hadoop03:7051")
      .option("kudu.table",s"tag_${DateUtils.getYeasterDayString()}")
      .kudu
      .show()

   /* val kuduContext = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)
    kuduContext.deleteTable(s"ODS_${DateUtils.getNow()}")*/
  }
}

```



### 33: MyGraphx.scala

````scala
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MyGraphx {

  def main(args: Array[String]): Unit = {

    //图 有 点 与边 构成
    /**
      * 点:
      *   1 张三 18
      *   2 李四 19
      *   3 王五 20
      *   4赵六215韩梅梅226李雷237小明249tom2510jerry2611ession27
      *
      *  边：
      *   1 1362136313641365136415851586158715891771017711177
      */
    val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()
    //构建点
    val vertices: RDD[(Long, (String, Int))] = spark.sparkContext.parallelize(Seq[(Long,(String,Int))](
      (1,("张三",18)),
      (2,("李四",19)),
      (3,("王五",20)),
      (4,("赵六",21)),
      (5,("韩梅梅",22)),
      (6,("李雷",23)),
      (7,("小明",24)),
      (9,("tom",25)),
      (10,("jerry",26)),
      (11,("1ession",27))
    ))

    //构建边
    val edge: RDD[Edge[Int]] = spark.sparkContext.parallelize(Seq[Edge[Int]](
      Edge(1,133,0),
      Edge(2,133,0),
      Edge(3,133,0),
      Edge(4,133,0),
      Edge(4,188,0),
      Edge(5,133,0),
      Edge(6,188,0),
      Edge(7,188,0),
      Edge(9,155,0),
      Edge(10,155,0),
      Edge(11,155,0)
    ))
    //创建图
    val graph = Graph(vertices,edge)
    //查看点
    //graph.vertices.foreach(println(_))
    //查看边
    //graph.edges.foreach(println(_))
    //创建连通图
    val connected = graph.connectedComponents()
    //查看连通图的点
    /**
      * (6,1)
      * (3,1)
      * (4,1)
      * (1,1)
      * (188,1)
      * (133,1)
      * (7,1)
      * (5,1)
      * (2,1)
      *
      * (9,9)
      * (155,9)
      * (11,9)
      * (10,9)
      */
    //[6,3,4,1,5,2,7,133,188]
    //[9,10,11,155]
    //connected.vertices.foreach(println(_))
    val result: RDD[(VertexId, Iterable[VertexId])] = connected.vertices.map(item => (item._2, item._1))
      .groupByKey()

    /**
      * (9,CompactBuffer((9,"aa",20), (10,"bb",30), (11,"cc",40)))
      * (1,CompactBuffer(6, 3, 4, 133, 1, 7, 188, 5, 2))
      */
    //(id,aggid) join (id,(name,age)) => (id,(aggid,(name,age)))
    connected.vertices.join(vertices)
      .map {
        case (id,(aggid,(name,age))) => (aggid,(id,name,age))
      }.groupByKey().foreach(println(_))



  }
}

````



### 34: pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>cn.itcast</groupId>
    <artifactId>dmp_class_09</artifactId>
    <version>1.0-SNAPSHOT</version>


    <repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
    </repositories>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <scala.version>2.11.8</scala.version>
        <scala.v>2.11</scala.v>
        <hadoop.version>2.6.1</hadoop.version>
        <spark.version>2.2.0</spark.version>
        <kudu.version>1.6.0-cdh5.14.0</kudu.version>
        <elasticsearch.verion>6.0.0</elasticsearch.verion>
    </properties>

    <dependencies>
        <!-- 导入scala依赖-->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <!-- 导入hadoop依赖-->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
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

        <!--导入sparkcore依赖-->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.v}</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <!--导入sparksql依赖-->
        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.v}</artifactId>
            <version>${spark.version}</version>
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

        <!-- 导入加载配置文件的依赖-->
        <dependency>
            <groupId>com.typesafe</groupId>
            <artifactId>config</artifactId>
            <version>1.2.1</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
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
</project>
```

