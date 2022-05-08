---
typora-root-url: image

---



## 第二章：开发DMP数据管理平台

### 2.1：项目架构介绍

![1566214788124](F:/magic-book/hadoop/DMP-project/DMP/assets/1566214788124.png)



存储层：KUDU

计算层：spark、Graphx

快速查询层：impala

展示层：WEB



### 2.2：数据处理流程：

![1566214846725](F:/magic-book/hadoop/DMP-project/DMP/assets/1566214846725.png)



### 2.3：数据字段介绍

| 字段                 | 解释                                                         |
| -------------------- | ------------------------------------------------------------ |
| IP                   | 设备的真实IP                                                 |
| sessionid            | 会话标识                                                     |
| advertisersid        | 广告主ID                                                     |
| adorderid            | 广告ID                                                       |
| adcreativeid         | 广告创意ID( >= 200000 : dsp ,  < 200000 oss)                 |
| adplatformproviderid | 广告平台商ID(>= 100000: rtb  , < 100000 : api )              |
| sdkversionnumber     | SDK版本号                                                    |
| adplatformkey        | 平台商key                                                    |
| putinmodeltype       | 针对广告主的投放模式,1：展示量投放 2：点击量投放             |
| requestmode          | 数据请求方式（1:请求、2:展示、3:点击）                       |
| adprice              | 广告价格                                                     |
| adppprice            | 平台商价格                                                   |
| requestdate          | 请求时间,格式为：yyyy-m-dd hh:mm:ss                          |
| appid                | 应用id                                                       |
| appname              | 应用名称                                                     |
| uuid                 | 设备唯一标识，比如imei或者androidid等                        |
| device               | 设备型号，如htc、iphone                                      |
| client               | 设备类型 （1：android 2：ios 3：wp）windowphone              |
| osversion            | 设备操作系统版本，如4.0                                      |
| density              | 设备屏幕的密度 android的取值为0.75、1、1.5,ios的取值为：1、2 |
| pw                   | 设备屏幕宽度                                                 |
| ph                   | 设备屏幕高度                                                 |
| longitude            | 设备所在经度                                                 |
| lat                  | 设备所在纬度                                                 |
| provincename         | 设备所在省份名称                                             |
| cityname             | 设备所在城市名称                                             |
| ispid                | 运营商id                                                     |
| ispname              | 运营商名称                                                   |
| networkmannerid      | 联网方式id                                                   |
| networkmannername    | 联网方式名称                                                 |
| iseffective          | 有效标识（有效指可以正常计费的）(0：无效 1：有效)            |
| isbilling            | 是否收费（0：未收费 1：已收费）                              |
| adspacetype          | 广告位类型（1：banner 2：插屏 3：全屏）                      |
| adspacetypename      | 广告位类型名称（banner、插屏、全屏）                         |
| devicetype           | 设备类型（1：手机 2：平板）                                  |
| processnode          | 流程节点（1：请求量kpi 2：有效请求 3：广告请求）             |
| apptype              | 应用类型id                                                   |
| district             | 设备所在县名称                                               |
| paymode              | 针对平台商的支付模式，1：展示量投放(CPM) 2：点击量投放(CPC)  |
| isbid                | 是否rtb                                                      |
| bidprice             | rtb竞价价格                                                  |
| winprice             | rtb竞价成功价格                                              |
| iswin                | 是否竞价成功                                                 |
| cur                  | values:usd\|rmb等                                            |
| rate                 | 汇率                                                         |
| cnywinprice          | rtb竞价成功转换成人民币的价格                                |
| imei                 | 手机串码                                                     |
| mac                  | 手机MAC码                                                    |
| idfa                 | 手机APP的广告码                                              |
| openudid             | 苹果设备的识别码                                             |
| androidid            | 安卓设备的识别码                                             |
| rtbprovince          | rtb 省                                                       |
| rtbcity              | rtb 市                                                       |
| rtbdistrict          | rtb 区                                                       |
| rtbstreet            | rtb 街道                                                     |
| storeurl             | app的市场下载地址                                            |
| realip               | 真实ip                                                       |
| isqualityapp         | 优选标识                                                     |
| bidfloor             | 底价                                                         |
| aw                   | 广告位的宽                                                   |
| ah                   | 广告位的高                                                   |
| imeimd5              | imei_md5                                                     |
| macmd5               | mac_md5                                                      |
| idfamd5              | idfa_md5                                                     |
| openudidmd5          | openudid_md5                                                 |
| androididmd5         | androidid_md5                                                |
| imeisha1             | imei_sha1                                                    |
| macsha1              | mac_sha1                                                     |
| idfasha1             | idfa_sha1                                                    |
| openudidsha1         | openudid_sha1                                                |
| androididsha1        | androidid_sha1                                               |
| uuidunknow           | uuid_unknow  UUID密文                                        |
| userid               | 平台用户id                                                   |
| iptype               | 表示ip库类型，1为点媒ip库，2为广告协会的ip地理信息标准库，默认为1 |
| initbidprice         | 初始出价                                                     |
| adpayment            | 转换后的广告消费（保留小数点后6位）                          |
| agentrate            | 代理商利润率                                                 |
| lomarkrate           | 代理利润率                                                   |
| adxrate              | 媒介利润率                                                   |
| title                | 标题                                                         |
| keywords             | 关键字                                                       |
| tagid                | 广告位标识(当视频流量时值为视频ID号)                         |
| callbackdate         | 回调时间 格式为:YYYY/mm/dd hh:mm:ss                          |
| channelid            | 频道ID                                                       |
| mediatype            | 媒体类型                                                     |
| email                | 用户email                                                    |
| tel                  | 用户电话号码                                                 |
| sex                  | 用户性别                                                     |
| age                  | 用户年龄                                                     |

