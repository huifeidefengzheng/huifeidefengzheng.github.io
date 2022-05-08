---
typora-root-url: image
---

[TOC]

## 2.6：根据传递的IP解析出经纬度、地址

在数据传递过程中并没有携带当前IP的经纬度，所以需要根据传递来的IP解析出经纬度IP所在的经纬度以及所在的省份-城市

### 2.6.1：开发App驱动

```scala
import es.EsProcess
import etl.ParseIp
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import pro._
import utils.ConfigUtils

object Application {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .appName("app")
        .master("local[*]")
        .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
        .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
        .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
        .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
        .config("spark.broadcast.compress",ConfigUtils.SPARK_BROADCAST_COMPRESS)
        .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
        .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
        .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
        .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
        .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
        .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
        .config("spark.speculation.flag",ConfigUtils.SPARK_SERIALIZER)
        .config("es.nodes",ConfigUtils.ES_NODES)
        .config("es.port",ConfigUtils.ES_PORT)
        .config("es.index.auto.create",ConfigUtils.ES_INDEX_AUTO_CREATE)
        .config("es.http.timeout",ConfigUtils.ES_HTTP_TIMEOUT)
        //.config("cluster.name","elasticsearch")
        .getOrCreate()
      
      //2.读取数据
      val source = spark.read.json("data/data.json")
      //3.处理数据
      //3.1列裁剪，只取出ip
      val  ips  = source.selectExpr("ip")
      //3.2去重，根据ip去重
      val distinctIp = ips.distinct()
      //3.3解析ip，获取经纬度，省份，城市
      distinctIp.rdd.map(row=>{
          //获取ip
          val ip = row.getAs[String]("ip")
          
          val lookupService = new LookupService(ConfigUtils.GEOLITECIY_DAY)
          val location = lookupService.getLocation(ip)
           //经度
      val longitude = location.longitude
      //纬度
      val latitude = location.latitude
      //3、省份、城市
      val iPAddressUtils = new IPAddressUtils
      val iPLocation = iPAddressUtils.getregion(ip)
      //省份
      val province = iPLocation.getRegion
      //城市
      val city= iPLocation.getCity
      //4、数据返回
      (ip,longitude,latitude,province,city)
    }).toDF("ip","longitude","latitude","province","city")
      .createOrReplaceTempView("ip_info")
      })

    val context = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)
    //TODO 解析IP获取经纬度、省市
    ParseIp.process(spark,context)

  }
}
```

### 2.6.3：开发ParseIp工具类

```scala
package etl

import `trait`.Process
import com.maxmind.geoip.{Location, LookupService}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import utils._

/**
  * 解析ip,生成经纬度、省市
  */
object ParseIp extends Process{
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    */

  //定义数据保存表ODS
  val SINK_TABLE = s"ODS_${DateUtils.getNow()}"
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、获取数据
    spark.read.json(ConfigUtils.DATA_PATH).createOrReplaceTempView("t_data_info")
    //2、获取所有的ip,过滤掉为空的ip
    val ipDF = spark.sql("select ip from t_data_info where ip is not null and ip!=''")
    val ips = ipDF.distinct().rdd

    import spark.implicits._
    //3、解析经纬度、省市
    ips.map(row=>{
      //获取ip
      val ip = row.getAs[String]("ip")

      //解析经纬度
      val service: LookupService = new LookupService(ConfigUtils.GeoLiteCity)
      //ip所处位置
      val location: Location = service.getLocation(ip)
      //经度
      val longitude: Float = location.longitude
      //纬度
      val latitude: Float = location.latitude
      //获取ip地址
      val address = new IPAddressUtils()

      val region: IPLocation = address.getregion(ip)
      //省份
      val proviceName: String = region.getRegion
      //城市
      val city: String = region.getCity

      (ip,longitude,latitude,proviceName,city)
    }).toDF("ip","longitude","latitude","proviceName","city")
      .createOrReplaceTempView("t_ip_info")
    //4、补充原始数据
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
        |b.proviceName,
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
        | from t_data_info a left join t_ip_info b
        | on a.ip = b.ip
      """.stripMargin)
    //5、数据写入ods
    val schema = result.schema
    //设置表的属性
    val options = new CreateTableOptions()
    //设置表的分区策略 分区字段 分区个数  副本数
    val columns = Seq[String]("ip")
    import scala.collection.JavaConverters._
    options.addHashPartitions(columns.asJava,3)

    options.setNumReplicas(3)

    //设置主键
    val keys = columns
    KuduUtils.writeToKudu(kuduContext,schema,options,SINK_TABLE,result,keys)
  }
}
```

### 2.6.4：开发DateUtils工具类

```scala
package utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期生成帮助类
  */
object DateUtils {

  /**
    * 获取当天日志yyyyMMdd格式的字符串
    * @return
    */
  def getNow():String={

    //20190615
    val date = new Date()

    val format = FastDateFormat.getInstance("yyyyMMdd")

    format.format(date)
  }

  /**
    * 获取昨天日期的yyyyMMdd格式字符串
    * @return
    */
  def getYesterDay():String ={
    val date = new Date()

    val calendar = Calendar.getInstance()

    calendar.setTime(date)

    calendar.add(Calendar.DAY_OF_YEAR,-1)

    val format = FastDateFormat.getInstance("yyyyMMdd")

    format.format(calendar)
  }
}
```

### 2.6.5：执行查看结果

#### 2.6.5.1：启动kudu

**注意：需要在每一台机器操作**

同步时间：

```shell
/etc/init.d/ntpd restart
```

启动：

```shell
sudo service kudu-master start
sudo service kudu-tserver start
```

#### 2.6.8.2：启动impala

启动mysql：

```shell
service mysqld start
```

启动HDFS：

```shell
start-dfs.sh
```

启动hive的元数据服务：

```shell
cd  /export/servers/hive
nohup bin/hive --service metastore &
```

启动impala：

hadoop01执行：

```shell
service impala-state-store start
service impala-catalog start
service impala-server start
```

hadoop02和hadoop03执行：

```shell
service impala-server start
```

#### 2.6.8.3：查看hadoop01:8051中的kudu，是否生成新表

![image-20181030104659549](image-20181030104659549.png)

#### 2.6.8.4：将kudu中的数据作为impala的外部表，查看数据写入kudu

![image-20181030104836991](image-20181030104836991.png)

（1）：将kudu表作为impala外部表，通过查询条数验证数据是否成功写入

```sql
 CREATE EXTERNAL TABLE `ODS_20200316` STORED AS KUDU
TBLPROPERTIES(
 'kudu.table_name' = 'ODS_20200316',
 'kudu.master_addresses' = 'node01:7051,node02:7051,node03:7051');
 
 
Query: create EXTERNAL TABLE `ODS20181030` STORED AS KUDU
TBLPROPERTIES(
    'kudu.table_name' = 'ODS20181030',
    'kudu.master_addresses' = 'node01:7051,node:7051,node03:7051')

```

(2)：查看数据条数，验证数据是否进入kudu

```sql
[angel1:21000] > select count(1) from ods20181030;
Query: select count(1) from ods20181030
Query submitted at: 2018-10-30 10:49:13 (Coordinator: http://angel1:25000)
Query progress can be monitored at: http://angel1:25000/query_plan?query_id=ec4d0b8fdb7bb7e1:32e5670200000000
+----------+
| count(1) |
+----------+
| 1000     |
+----------+
Fetched 1 row(s) in 5.20s
```

