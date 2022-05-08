---
title: 01code
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - DMP-6
---

## src/main/ java/resources/dev

### application.conf

dmp_class06/src/main/resources/dev/application.conf

```conf
#配置spark相关的参数
spark.worker.timeout="500"
spark.rpc.askTimeout="600s"
spark.network.timeout="600s"
spark.cores.max="10"
spark.task.maxFailures="5"
spark.speculation="true"
spark.driver.allowMutilpleContext="true"
spark.serializer="org.apache.spark.serializer.KryoSerializer"
spark.buffer.pageSize="8m"


#指定kudumaster地址
kudu.master="node1:7051,node2:7051,node3:7051"
```

#### scala/cn/xhchen/dmp

### tools

##### GlobalConfigUtils.scala

dmp_class06/src/main/scala/cn/xhchen/dmp/tools/GlobalConfigUtils.scala

```scala
package cn.xhchen.dmp.tools

import com.typesafe.config.{Config, ConfigFactory}

//todo: 加载配置文件application.conf，获取对应的内容
object GlobalConfigUtils {

       //默认就是加载这个配置文件application.conf
      private val config: Config = ConfigFactory.load()

  /**
spark.worker.timeout="500"
spark.rpc.askTimeout="600s"
spark.network.timeout="600s"
spark.cores.max="10"
spark.task.maxFailures="5"
spark.speculation="true"
spark.driver.allowMutilpleContext="true"
spark.serializer="org.apache.spark.serializer.KryoSerializer"
spark.buffer.pageSize="8m"
    */

  def sparkWorkerTimeout=config.getString("spark.worker.timeout")
  def sparkRpcAskTimeout=config.getString("spark.rpc.askTimeout")
  def sparkNetworkTimeout=config.getString("spark.network.timeout")
  def sparkCoresMax=config.getString("spark.cores.max")
  def sparkTaskMaxFailures=config.getString("spark.task.maxFailures")
  def sparkSpeculation=config.getString("spark.speculation")
  def sparkDriverAllowMutilpleContext=config.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer=config.getString("spark.serializer")
  def sparkBufferPageSize=config.getString("spark.buffer.pageSize")



  //获取kudumaster地址
    def kuduMaster=config.getString("kudu.master")

//   def getKuduMaster():String={
//     val master: String = config.getString("kudu.master")
//     master
//   }

}
```

##### DmpMain.scala

dmp_class06/src/main/scala/cn/xhchen/dmp/DmpMain.scala

```scala
package cn.xhchen.dmp

import cn.xhchen.dmp.tools.GlobalConfigUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//todo: 它是整个项目的程序的执行入口，后期开发大量的处理逻辑
object DmpMain {

  def main(args: Array[String]): Unit = {
    /**
spark.worker.timeout="500"
spark.rpc.askTimeout="600s"
spark.network.timeout="600s"
spark.cores.max="10"
spark.task.maxFailures="5"
spark.speculation="true"
spark.driver.allowMutilpleContext="true"
spark.serializer="org.apache.spark.serializer.KryoSerializer"
spark.buffer.pageSize="8m"
      */

        //1、创建SparkConf对象
        val sparkConf: SparkConf = new SparkConf()
                                            .setAppName("DmpMain")
                                            .setMaster("local[6]")
                                            .set("spark.worker.timeout", GlobalConfigUtils.sparkWorkerTimeout)
                                            .set("spark.rpc.askTimeout", GlobalConfigUtils.sparkRpcAskTimeout)
                                            .set("spark.network.timeout", GlobalConfigUtils.sparkNetworkTimeout)
                                            .set("spark.cores.max", GlobalConfigUtils.sparkCoresMax)
                                            .set("spark.task.maxFailures", GlobalConfigUtils.sparkTaskMaxFailures)
                                            .set("spark.speculation", GlobalConfigUtils.sparkSpeculation)
                                            .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.sparkDriverAllowMutilpleContext)
                                            .set("spark.serializer", GlobalConfigUtils.sparkSerializer)
                                            .set("spark.buffer.pageSize", GlobalConfigUtils.sparkBufferPageSize)
       //2、构建SparkSession对象
          val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

       //3、构建SparkContext对象
          val sc: SparkContext = sparkSession.sparkContext
          sc.setLogLevel("warn")

       //4、构建KuduContext对象
           val kuduContext = new KuduContext(GlobalConfigUtils.kuduMaster,sc)

       //5、大量的处理逻辑



  }
}
```

##### pom.xml

dmp_class06/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>cn.xhchen</groupId>
    <artifactId>dmp_class06</artifactId>
    <version>1.0-SNAPSHOT</version>

    <repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
    </repositories>

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

        <!-- 导入加载配置文件的依赖-->
        <dependency>
            <groupId>com.typesafe</groupId>
            <artifactId>config</artifactId>
            <version>1.2.1</version>
        </dependency>

    </dependencies>
      <!--为了区分不同的运行环境   开发、测试、生产环境-->
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

# 附件

##### ContantsSQL.scala

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/ContantsSQL.scala

```scala
package com.dmp.tools

object ContantsSQL {

  //1：初始化，将经纬度和地域省市merge合并到ods中
  lazy val odssql = "select " +
    "ods.ip ," +
    "ods.sessionid," +
    "ods.advertisersid," +
    "ods.adorderid," +
    "ods.adcreativeid," +
    "ods.adplatformproviderid" +
    ",ods.sdkversion" +
    ",ods.adplatformkey" +
    ",ods.putinmodeltype" +
    ",ods.requestmode" +
    ",ods.adprice" +
    ",ods.adppprice" +
    ",ods.requestdate" +
    ",ods.appid" +
    ",ods.appname" +
    ",ods.uuid" +
    ",ods.device" +
    ",ods.client" +
    ",ods.osversion" +
    ",ods.density" +
    ",ods.pw" +
    ",ods.ph" +
    ",location.longitude as long" +
    ",location.latitude as lat" +
    ",location.province as provincename" +
    ",location.city as cityname" +
    ",ods.ispid, ods.ispname" +
    ",ods.networkmannerid, ods.networkmannername, ods.iseffective, ods.isbilling" +
    ",ods.adspacetype, ods.adspacetypename, ods.devicetype, ods.processnode, ods.apptype" +
    ",ods.district, ods.paymode, ods.isbid, ods.bidprice, ods.winprice, ods.iswin, ods.cur" +
    ",ods.rate, ods.cnywinprice, ods.imei, ods.mac, ods.idfa, ods.openudid,ods.androidid" +
    ",ods.rtbprovince,ods.rtbcity,ods.rtbdistrict,ods.rtbstreet,ods.storeurl,ods.realip" +
    ",ods.isqualityapp,ods.bidfloor,ods.aw,ods.ah,ods.imeimd5,ods.macmd5,ods.idfamd5" +
    ",ods.openudidmd5,ods.androididmd5,ods.imeisha1,ods.macsha1,ods.idfasha1,ods.openudidsha1" +
    ",ods.androididsha1,ods.uuidunknow,ods.userid,ods.iptype,ods.initbidprice,ods.adpayment" +
    ",ods.agentrate,ods.lomarkrate,ods.adxrate,ods.title,ods.keywords,ods.tagid,ods.callbackdate" +
    ",ods.channelid,ods.mediatype,ods.email,ods.tel,ods.sex,ods.age " +
    "from ods left join location on ods.ip=location.ip where ods.ip is not null"


  //2：初始化，统计地域分布数量情况
  lazy val locationsql ="select provincename,cityname, count(*) as num from  ods group by provincename,cityname"


  //3、初始化 统计广告投放地域分布情况临时表
  lazy val adAnalysisSql_temp="select " +
    "provincename , " +
    "cityname , sum(case when requestmode =1 and processnode  >=1 then 1 else 0 end )  originalRequest , " +
    "sum(case when requestmode =1 and processnode  >=2 then 1 else 0 end )  validRequest , " +
    "sum(case when requestmode =1 and processnode  =3 then 1 else 0 end) adRequest , " +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling =1 and isbid=1 and adorderid !=0 then 1 else 0 end )  bidsNum ," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling =1 and iswin=1 and adorderid !=0 then 1 else 0 end )  bidsus ," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions ," +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks ," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum , " +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum , " +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end )  adCost ," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end)  adConsumption  " +
    "from ods  group by provincename,cityname"

  //统计广告投放地域分布情况真实表
    lazy val adAnalysisSql="select  " +
      "provincename , " +
      "cityname, " +
      "originalRequest , " +
      "validRequest , " +
      "adRequest , " +
      "bidsNum , " +
      "bidsus , " +
      "bidsus/bidsNum bidsusRat , " +
      "adImpressions , " +
      "adClicks , " +
      "adClicks/adImpressions adClicksRat , " +
      "adCost , " +
      "adConsumption " +
      "from locationTemp where bidsNum !=0 and adImpressions !=0"


  //4、统计广告投放app的分布情况临时表
  lazy val appAnalysis_temp= "select " +
                            "appid , " +
                            "appname," +
                            "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) originalRequest, " +
                            "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) validRequest,  " +
                            "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) adRequest,  " +
                            "sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 and adplatformproviderid >=100000 then 1 else 0 end) bidsNum," +
                            "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
                            "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) mediumDisplayNum, " +
                            "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) mediumClickNum " +
                            "from ods group by appid, appname"
  //统计广告投放app的分布情况事实表
  lazy val appAnalysis= "select " +
                            "appid," +
                            "appname," +
                            "originalRequest," +
                            "validRequest," +
                            "adRequest," +
                            "bidsNum," +
                            "bidsSus," +
                            "bidsSus/bidsNum bidsSusRat," +
                            "mediumDisplayNum," +
                            "mediumClickNum, " +
                            "mediumClickNum/mediumDisplayNum mediumClickRat" +
                            " from appAnalysis where bidsNum !=0 and mediumDisplayNum !=0"


  //5、统计广告投放手机设备分布情况临时表
  lazy val deviceAnalysis_temp= "select case client " +
    "when 1 then 'ios' " +
    "when 2 then 'android' " +
    "when 3 then 'wp' " +
    "else 'other' end as client," +
    "device," +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by client,device"

  //统计广告投放手机设备分布情况事实表
  lazy val deviceAnalysis= "select " +
                                "client," +
                                "device," +
                                "originalRequest," +
                                "validRequest," +
                                "adRequest," +
                                "bidsNum," +
                                "bidsSus," +
                                "bidsSus/bidsNum bidsSusRat," +
                                "mediumDisplayNum," +
                                "mediumClickNum, " +
                                "mediumClickNum/mediumDisplayNum mediumClickRat" +
                                " from deviceAnalysis where bidsNum !=0 and mediumDisplayNum !=0"

  //6、统计广告投放网络类型分布情况临时表
  lazy val networkAnalysis_temp= "select networkmannerid, " +
    "networkmannername, " +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by networkmannerid,networkmannername"

  //统计广告投放网络类型分布情况事实表
  lazy val networkAnalysis= "select " +
    "networkmannerid," +
    "networkmannername," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from networkAnalysis where bidsNum !=0 and mediumDisplayNum !=0"


  //7、统计广告投放网络运营商分布情况临时表
  lazy val ispAnalysis_temp= "select  " +
    "ispname," +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by ispname"

  //统计广告投放网络运营商情况事实表
  lazy val ispAnalysis= "select " +
    "ispname," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from ispAnalysis where bidsNum !=0 and mediumDisplayNum !=0"


  //8、统计广告投放渠道分布情况临时表
  lazy val channelAnalysis_temp= "select  " +
    "channelid," +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by channelid"

  //统计广告投放网络运营商情况事实表
  lazy val channelAnalysis= "select " +
    "channelid," +
    "originalRequest," +
    "validRequest," +
    "adRequest," +
    "bidsNum," +
    "bidsSus," +
    "bidsSus/bidsNum bidsSusRat," +
    "mediumDisplayNum," +
    "mediumClickNum, " +
    "mediumClickNum/mediumDisplayNum mediumClickRat" +
    " from channelAnalysis where bidsNum !=0 and mediumDisplayNum !=0"

  //9、商圈库：过滤非中国的ip
      ////需要过滤非中国的ip  73<long<136   3 <lat <54
    lazy  val filter_non_china="select distinct long,lat from ods where long >73 and long <136 and lat >3 and lat < 54"

 //10、提前过滤掉不符合规范的数据集
    lazy  val none_empty="select * from ods where " +
                                    "imei !='' or imeimd5 !='' or imeisha1 !='' " +
                                    "or mac !='' or macmd5 !='' or macsha1 !='' " +
                                    "or idfa !='' or idfamd5 !='' or idfasha1 !=''  " +
                                    "or openudid !='' or openudidmd5 !='' or openudidsha1 !='' " +
                                    "or androidid !='' or androididmd5 !='' or androididsha1 !=''"
}
```

##### ContantsSchema.scala

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/ContantsSchema.scala

```scala
package com.dmp.tools

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}

import scala.collection.JavaConverters._

//todo:定义表的schema的类
object ContantsSchema {
  //1、ods表的schema
  lazy val odsSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ip", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("sessionid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("advertisersid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adorderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adcreativeid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformproviderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("sdkversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformkey", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("putinmodeltype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("requestmode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adppprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("requestdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("client", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("osversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("density", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("pw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ph", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("long", Type.STRING).nullable(false).build(), //TODO
      new ColumnSchemaBuilder("lat", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("ispid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iseffective", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbilling", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetypename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("devicetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("processnode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("apptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("district", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("paymode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("winprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("iswin", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("cur", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("cnywinprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("imei", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mac", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfa", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androidid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbprovince", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbcity", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbdistrict", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbstreet", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("storeurl", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("realip", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("isqualityapp", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidfloor", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("aw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ah", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("imeimd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfamd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("imeisha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfasha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuidunknow", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("userid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("initbidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adpayment", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("agentrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("lomarkrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adxrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("title", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("keywords", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tagid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("callbackdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mediatype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("email", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tel", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("sex", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("age", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //2、locationSchema表的schema
  lazy val locationSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("num", Type.INT64).nullable(false).build()).asJava
    new Schema(columns)
  }

  //3: 广告投放地域分布情况表的schema
  lazy val adlocationSchema:Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("cityname" , Type.STRING).nullable(false).key(true).build() ,
      new ColumnSchemaBuilder("originalRequest" , Type.INT64).nullable(false).build() , //原始请求数
      new ColumnSchemaBuilder("validRequest" , Type.INT64).nullable(false).build() , //有效请求数
      new ColumnSchemaBuilder("adRequest" , Type.INT64).nullable(false).build() , //广告请求数
      new ColumnSchemaBuilder("bidsNum" , Type.INT64).nullable(false).build() , //竞价数
      new ColumnSchemaBuilder("bidsus" , Type.INT64).nullable(false).build() , //竞价成功数
      new ColumnSchemaBuilder("bidsusRat" , Type.DOUBLE).nullable(false).build() , //竞价成功率
      new ColumnSchemaBuilder("adImpressions" , Type.INT64).nullable(false).build() , //展示数
      new ColumnSchemaBuilder("adClicks" , Type.INT64).nullable(false).build() , //点击数
      new ColumnSchemaBuilder("adClicksRat" , Type.DOUBLE).nullable(false).build() , //点击率
      new ColumnSchemaBuilder("adCost" , Type.DOUBLE).nullable(false).build() , //广告成本
      new ColumnSchemaBuilder("adConsumption" , Type.DOUBLE).nullable(false)build() //广告消费
    ).asJava
    new Schema(columns)
  }



  //4、广告投放app分布情况的schema
  lazy val appAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //5、广告投放手机设备分布情况的schema
  lazy val deviceAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("client", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //6、广告投放网络类型分布情况的schema
  lazy val networkAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //7、广告投放网络运营商分布情况的schema
  lazy val ispAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  //8、广告投放渠道分布情况的schema
  lazy val channelAnalysisSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSus", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidsSusRat", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("mediumDisplayNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediumClickRat", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //9、商圈库表的schema
  lazy val businessAreaSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("geoHashCode", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("trade", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  //10、标签表的schema
  lazy val tagsSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("userids", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("tags", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }
}
```

##### DateUtils.scala

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/DateUtils.scala

```scala
package com.dmp.tools

import java.util.{Calendar, Date}

import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils

//处理时间的工具类
object DateUtils {

  /**
    * 获取当天的时间   yyyyMMdd
    */
  def getNowDate():String={
        val date = new Date()
         //todo:SimapleDateFormat是线程不安全，使用FastDateFormat
       val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
       val now: String = format.format(date)
        //yyyy-MM-dd HH:mm:ss  --------------->yyyyMMdd
       val time: Option[String] = processTime(now)
    
        time.getOrElse("sorry no time")
  }

  /**
    * 时间处理  yyyy-MM-dd HH:mm:ss  --------------->yyyyMMdd
    * @param time
    * @return
    */
  def processTime(time: String):Option[String]={
    if(StringUtils.isNotEmpty(time)){
      val array: Array[String] = time.split(" ")
      if(array.length>1){
          Some(array(0).replace("-",""))
      }else{
         None
      }
    }
    else{
      None
    }
  }

  /**
    * 获取当天昨天日期
    *
    * */
  def getYestday():String = {
    val dateFormat = FastDateFormat.getInstance("yyyyMMdd")
    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE,-1)
    calendar.getTime
    val yestDay = dateFormat.format(calendar.getTime)
    yestDay
  }

}
```

##### GlobalConfigUtils.scala

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/GlobalConfigUtils.scala

```scala
package com.dmp.tools

import com.typesafe.config.ConfigFactory

/**
  * 配置工具类，读取配置文件相关参数
  */
class GlobalConfigUtils {
  def config=ConfigFactory.load()
  //开始加载spark相关的配置参数
  def sparkWorkTimeOut=config.getString("spark.worker.timeout")
  def sparkRpcAskTimeOut=config.getString("spark.rpc.askTimeout")
  def sparkNetWorkTimeOut=config.getString("spark.network.timeout")
  def sparkCoreMax=config.getString("spark.cores.max")
  def sparkTaskMaxFailures=config.getString("spark.task.maxFailures")
  def sparkDriverAllowMutilpleContext=config.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer=config.getString("spark.serializer")
  def sparkBufferPageSize=config.getString("spark.buffer.pageSize")

  def clusterName=config.getString("cluster.name")
  def esIndexAutoCreate=config.getString("es.index.auto.create")
  def esNodes=config.getString("esNodes")
  def esPort=config.getString("es.port")
  def esIndexMissing=config.getString("es.index.reads.missing.as.empty")
  def esNodesDiscovery=config.getString("es.nodes.discovery")
  def esHttpTimeOut=config.getString("es.http.timeout")
  //获取kudumaster地址
  def kuduMaster=config.getString("kudu.master")
   //获取文件路径的方法
  def filePath=config.getString("file.path")

  //获取解析ip地址的文件
  def geoLiteCity=config.getString("geoLiteCity")
  //获取解析ip的省份城市文件
  def IP_FILE=config.getString("IP_FILE")
  def INSTALL_DIR=config.getString("INSTALL_DIR")
  //获取ods表名
  def db_ods=config.getString("DB.ODS.PREFIX")+DateUtils.getNowDate()
  def db_ods_prefix=config.getString("DB.ODS.PREFIX")

  //获取历史标签数据集
  def getTags=config.getString("DB.TAGS.PREFIX")
  //获取地域数量分布情况
  def processLocation=config.getString("processLocation")
  //获取广告投放地域分布情况
  def adLocationAnalysis=config.getString("adLocationAnalysis")
  //获取广告投放app分布情况
  def appAnalysis = config.getString("appAnalysis")
  //获取广告投放手机设备分布情况
  def deviceAnalysis=config.getString("deviceAnalysis")

  //获取广告投放网络类型分布情况
  def networkAnalysis=config.getString("networkAnalysis")

  //获取广告投放网络运营商分布情况
  def ispAnalysis=config.getString("ispAnalysis")

  //获取广告投放渠道分布情况
  def channelAnalysis=config.getString("channelAnalysis")

  //获取高德key
  def getKey=config.getString("key")

  //商圈库
  def tradeDB=config.getString("tradeDB")

  //appid的字典文件
  def getAppIdPath=config.getString("app.path")
  def getDevicePath=config.getString("device.path")

  //impala配置参数
  def jdbcDriver=config.getString("jdbc.driver")
  def connectUrl=config.getString("connect.url")

  //标签衰减系数
  def getCoeff=config.getString("coeff")

}

object GlobalConfigUtils extends GlobalConfigUtils {



}
```

##### IpUtils.scala

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/IpUtils.scala

```scala
package com.dmp.tools

import com.dmp.bean.Location
import com.dmp.tools.iplocation.{IPAddressUtils, IPLocation}
import com.maxmind.geoip
import com.maxmind.geoip.LookupService
import org.apache.spark.rdd.RDD

/**
  * 解析ip地址的工具类
  */
object IpUtils {
  val geoLiteCity=GlobalConfigUtils.geoLiteCity

//把ip地址转换成一个实体类对象
  //要根据GeoLiteCity.dat 解析出ip对应的经纬度
  //跟根据qqwry.dat，解析出ip对应的省份城市

  def parseIp2Bean(rdd:RDD[String]):RDD[Location]={
    //根据ip地址解析经纬度
   //遍历  获取ip对应的经度和维度，省份和城市
    val result: RDD[Location] = rdd.mapPartitions(iter => {
      val lookupService = new LookupService(geoLiteCity)
      iter.map(ip => {
          val location: geoip.Location = lookupService.getLocation(ip)
          //经度
          val longitude: Float = location.longitude
          //维度
          val latitude: Float = location.latitude

          //使用纯真数据库qqwry获取省份和城市
          val addressUtils = new IPAddressUtils

          val region= addressUtils.getregion(ip)

          //省
          val province: String = region.getRegion
          //市
          val city: String = region.getCity

          //返回
          Location(ip, longitude.toString, latitude.toString, province, city)

      })
    })
    result
  }
}
```

##### ParseJson.java

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/ParseJson.java

```java
package com.dmp.tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dmp.bean.BusinessArea;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class ParseJson {
    public static String parseJsonStr(String json){
        JSONObject jsonObject = JSON.parseObject(json);

        /**
         <regeocode>
             <addressComponent>
                 <businessAreas type="list">
                     <businessArea>
                         <location>116.29522008325625,39.99426090286774</location>
                         <name>颐和园</name>
                         <id>110108</id>
                     </businessArea>
                 </businessAreas>
             </addressComponent>
         </regeocode>
         */

        JSONObject regeocode = (JSONObject)jsonObject.get("regeocode");
        JSONObject addressComponent = (JSONObject) regeocode.get("addressComponent");
        JSONArray jsonArray = addressComponent.getJSONArray("businessAreas");

        List<BusinessArea> businessAreas = JSON.parseArray(jsonArray.toString(), BusinessArea.class);

        String flag="blank";
        if(businessAreas!=null && businessAreas.size() >0) {
            StringBuffer sb = new StringBuffer();
            for (BusinessArea businessArea : businessAreas) {
                  if(businessArea !=null) {
                      sb.append(businessArea.getName() + ":");  //软件园:上地:
                  }
            }

            //去掉最后一个字符：
            if(StringUtils.isNotBlank(sb.toString())){
                String data = sb.toString();
                flag= data.substring(0,data.length()-1);
            }

        }

        return flag;

    }
}
```

##### application.conf

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/application.conf

```conf
#配置spark相关的参数
spark.worker.timeout="500"
spark.rpc.askTimeout="600s"
spark.network.timeout="600s"
spark.cores.max="10"
spark.task.maxFailures="5"
spark.speculation="true"
spark.driver.allowMutilpleContext="true"
spark.serializer="org.apache.spark.serializer.KryoSerializer"
spark.buffer.pageSize="8m"


#配置es相关参数
cluster.name=myes
es.index.auto.create="true"
//不能写es.nodes 否则报错 es.nodes has type OBJECT rather than STRING
esNodes="192.168.200.100"
es.port="9200"
#索引是否可以为空
es.index.reads.missing.as.empty="true"
#节点是否自动发现集群
es.nodes.discovery="true"
#超时时间
es.http.timeout="200000"

#kudumaster的地址
kudu.master="node1:7051,node2:7051,node3:7051"

#数据文件路径
file.path="C:\\Users\\lisha\\Desktop\\dmp项目\\dmp_day02\\数据\\data.json"

#指定解析ip相关的文件
geoLiteCity="E:\\workspace\\dmp_demo\\processData\\src\\main\\resources\\GeoLiteCity.dat"
IP_FILE="qqwry.dat"
INSTALL_DIR="E:\\workspace\\dmp_demo\\processData\\src\\main\\resources"

#添加ods前缀
DB.ODS.PREFIX="ODS"

#添加历史标签数据集前缀
DB.TAGS.PREFIX="DTP"

#统计地域数量的分布情况表
processLocation="processLocation"

#统计广告投放地域分布情况表
adLocationAnalysis="adLocationAnalysis"

#统计广告投放app的分布情况
appAnalysis="appAnalysis"

#统计广告投放手机设备的分布情况
deviceAnalysis="deviceAnalysis"

#统计广告投放网络类型的分布情况
networkAnalysis="networkAnalysis"

#统计广告投放网络运营商的分布情况
ispAnalysis="ispAnalysis"

#统计广告投放渠道的分布情况
channelAnalysis="channelAnalysis"

#添加高德地图的key
key="5f23433388e600bfc5008c1319373676"

#商圈库
tradeDB="tradeDB"

#appid的字典文件
app.path="E:\\workspace\\dmp_demo\\processData\\src\\main\\resources\\appID_name"
device.path="E:\\workspace\\dmp_demo\\processData\\src\\main\\resources\\devicedic"

#配置impala的地址
jdbc.driver="com.cloudera.impala.jdbc41.Driver"
connect.url="jdbc:impala://node3:21050/default;auth=noSasl"


#配置标签衰减的权重系数
coeff="0.92"
```

##### log4j.properties

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/工具/log4j.properties

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

#### 目录：iplocation

##### IPAddressUtils.java

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/iplocation/IPAddressUtils.java

```java
package com.dmp.tools.ips.iplocation;

/**
 * Created by angel；
 */
import com.dmp.tools.GlobalConfigUtils;
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
    private String IP_FILE= new GlobalConfigUtils().IP_FILE();
    /**
     * 纯真IP数据库保存的文件夹
     */
    private String INSTALL_DIR=new GlobalConfigUtils().INSTALL_DIR();

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

##### IPLocation.java

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/iplocation/IPLocation.java

```java
package com.dmp.tools.ips.iplocation;

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

##### Util.java

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/iplocation/Util.java

```java
package com.dmp.tools.ips.iplocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;


/**
 * 工具类，提供IP字符串转数组的方法
 */
public class Util {
    private static final Logger log = LoggerFactory.getLogger(Util.class);
    private static StringBuilder sb = new StringBuilder();

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

##### sql语句.sql

/Users/xhchen/Downloads/大数据方向/就业班/广告系统dmp项目/dmp_day02/资料/sql语句.sql

```sql
--1：初始化，将经纬度和地域省市merge合并到ods中
  lazy val odssql = "select " +
    "ods.ip ," +
    "ods.sessionid," +
    "ods.advertisersid," +
    "ods.adorderid," +
    "ods.adcreativeid," +
    "ods.adplatformproviderid" +
    ",ods.sdkversion" +
    ",ods.adplatformkey" +
    ",ods.putinmodeltype" +
    ",ods.requestmode" +
    ",ods.adprice" +
    ",ods.adppprice" +
    ",ods.requestdate" +
    ",ods.appid" +
    ",ods.appname" +
    ",ods.uuid" +
    ",ods.device" +
    ",ods.client" +
    ",ods.osversion" +
    ",ods.density" +
    ",ods.pw" +
    ",ods.ph" +
    ",location.longitude as long" +
    ",location.latitude as lat" +
    ",location.province as provincename" +
    ",location.city as cityname" +
    ",ods.ispid, ods.ispname" +
    ",ods.networkmannerid, ods.networkmannername, ods.iseffective, ods.isbilling" +
    ",ods.adspacetype, ods.adspacetypename, ods.devicetype, ods.processnode, ods.apptype" +
    ",ods.district, ods.paymode, ods.isbid, ods.bidprice, ods.winprice, ods.iswin, ods.cur" +
    ",ods.rate, ods.cnywinprice, ods.imei, ods.mac, ods.idfa, ods.openudid,ods.androidid" +
    ",ods.rtbprovince,ods.rtbcity,ods.rtbdistrict,ods.rtbstreet,ods.storeurl,ods.realip" +
    ",ods.isqualityapp,ods.bidfloor,ods.aw,ods.ah,ods.imeimd5,ods.macmd5,ods.idfamd5" +
    ",ods.openudidmd5,ods.androididmd5,ods.imeisha1,ods.macsha1,ods.idfasha1,ods.openudidsha1" +
    ",ods.androididsha1,ods.uuidunknow,ods.userid,ods.iptype,ods.initbidprice,ods.adpayment" +
    ",ods.agentrate,ods.lomarkrate,ods.adxrate,ods.title,ods.keywords,ods.tagid,ods.callbackdate" +
    ",ods.channelid,ods.mediatype,ods.email,ods.tel,ods.sex,ods.age " +
    "from ods left join location on ods.ip=location.ip where ods.ip is not null"



--2：统计地域分布数量情况
  lazy val locationsql ="select provincename,cityname, count(*) as num from  ods group by provincename,cityname"


--3：统计广告投放的地域分布情况临时表
  lazy val adAnalysisSqlTmp="select provincename,cityname,sum(case when requestmode =1 and processnode  >=1 then 1 else 0 end ) originalRequest,sum(case when requestmode =1 and processnode  >=2 then 1 else 0 end ) validRequest,sum(case when requestmode =1 and processnode  =3 then 1 else 0 end) adRequest,sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling =1 and isbid=1 and adorderid !=0 then 1 else 0 end ) bidsNum,sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling =1 and iswin=1 and adorderid !=0 then 1 else 0 end ) bidsus,sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions,sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks,sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum,sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum,sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost,sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption  from ods  group by provincename,cityname;"

--  统计广告投放的地域分布情况事实表
  lazy val adAnalysisSql="select  " +
      "provincename," +
      "cityname," +
      "originalRequest," +
      "validRequest," +
      "adRequest," +
      "bidsNum," +
      "bidsus, " +
      "bidsus/bidsNum bidsusRat," +
      "adImpressions," +
      "adClicks," +
      "adClicks/adImpressions adClicksRat," +
      "adCost," +
      "adConsumption " +
      "from locationTemp where bidsNum !=0 and adImpressions !=0"

--4：统计广告投放app分布情况临时表
lazy val appAnalysis_temp= "select " +    
							"appid , " +    
							"appname," +    
							"sum(case when requestmode<=2 and processnode =1 then 1 else 0 end) originalRequest, " +
							"sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) validRequest,  " +    
							"sum(case when requestmode=1 and processnode =3 then 1 else 0 end) adRequest,  " +    
							"sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 and adplatformproviderid >=100000 then 1 else 0 end) bidsNum," +    
							"sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +    
							"sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) mediumDisplayNum, " +
							"sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) mediumClickNum " +    
							"from ods group by appid, appname"

-- 统计广告投放app分布情况事实表
  lazy val appAnalysis= "select " +
                            "appid," +
                            "appname," +
                            "OriginalRequest," +
                            "ValidRequest," +
                            "adRequest," +
                            "bidsNum," +
                            "bidsSus," +
                            "bidsSus/bidsNum bidsSusRat," +
                            "MediumDisplayNum," +
                            "MediumClickNum, " +
                            "MediumClickNum/MediumDisplayNum mediumClickRat" +
                            " from appAnalysis where bidsNum !=0 and MediumDisplayNum !=0"


 --5、统计广告投放手机设备情况临时表
  lazy val deviceAnalysis_temp= "select case client " +
    "when 1 then 'ios' " +
    "when 2 then 'android' " +
    "when 3 then 'wp' " +
    "else 'other' end as client," +
    "device," +
    "sum(case when requestmode <=2 and processnode =1  then 1 else 0 end) originalRequest," +
    "sum(case when requestmode >=1 and processnode >=2 then 1 else 0 end) validRequest," +
    "sum(case when requestmode =1  and processnode =3  then 1 else 0 end) adRequest," +
    "sum(case when adplatformproviderid >= 100000 and iseffective =1 and isbilling =1 and isbid =1 and adorderid !=0 then 1 else 0 end)  bidsNum," +
    "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adplatformproviderid >=100000 then 1 else 0 end) bidsSus," +
    "sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )  adImpressions, " +
    "sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )  adClicks," +
    "sum(case when requestmode =2 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumDisPlayNum," +
    "sum(case when requestmode =3 and iseffective =1 and isbilling =1 then 1 else 0 end )  mediumClickNum," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*winprice/1000 else 0 end ) adCost," +
    "sum(case when adplatformproviderid >=100000  and iseffective =1 and isbilling=1 and iswin =1 and adorderid >200000 and adcreativeid > 200000  then 1*adpayment/1000 else 0 end) adConsumption   " +
    "from ods group by client,device"
  											
 --统计广告投放手机设备情况事实表
  lazy val deviceAnalysis= "select " +
                                "client," +
                                "device," +
                                "originalRequest," +
                                "validRequest," +
                                "adRequest," +
                                "bidsNum," +
                                "bidsSus," +
                                "bidsSus/bidsNum bidsSusRat," +
                                "mediumDisplayNum," +
                                "mediumClickNum, " +
                                "mediumClickNum/mediumDisplayNum mediumClickRat" +
                                " from deviceAnalysis where bidsNum !=0 and mediumDisplayNum !=0"
```


