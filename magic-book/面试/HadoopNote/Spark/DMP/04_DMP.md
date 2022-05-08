---
title: 04_DMP
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - DMP
---
DMP (Data Management Platform)

.导读
整个课程的内容大致分为如下两个部分

* 业务介绍
* 技术实现

对于业务介绍, 比较困难的是理解广告交易过程中各个参与者是干什么的

对于技术实现, 大致就是如下两个步骤

. 报表
. 标签化

报表显而易见, 就是查看数据的组成, 查看数据的图形直观特征

标签化是整个项目的目的, 最终其实就要根据标签筛选用户, 但是对于标签化还是有很多东西要做的, 如下

* 商圈库
* 打标签
* 统一用户识别
* 标签合并 & 衰减
* 历史合并

## 1. 项目介绍

[caption=""]
.导读
. 背景介绍
. `DMP` 的作用和实现方式
. 技术方案

### 1.1. 广告业务背景

[caption=""]
.导读
互联网管广告发展至今, 产生了很多非常复杂的概念, 其中环环交错, 不容易理清, 这一章节的主要目的就是尽可能的理清楚整体上的流程, 各个环节的作用

`Step 1`: 广告主, 广告商, 媒体::

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613150210.png)

* 广告主

简单来说就是要发广告的机构和个人

* 广告商

广告商是中介, 对接广告主和媒体, 广告主告诉广告商我要发广告, 广告商找到媒体进行谈判

* 媒体

比如说微博, 腾讯, 美团这样的应用和网站, 就是媒体, 它们具有广告展示的位置, 用户在使用这些服务的同时会看到各样的广告

* 受众

普通的用户, 在享受免费的服务的同时, 被动的接受广告

但是受众是有不同类型的, 可以由标签来表示, 比如说白领, 女性, `20 - 30` 岁等

`Step 2`: 小媒体和广告网络::

刚才的结构中有一个非常明显的问题

* 小媒体有很多

不只有微博腾讯这些媒体, 还有很多其它的垂直小媒体, 比如说一些软件网站, 一些小型的App, 甚至前阵子比较流行的游戏消灭病毒等, 都是小型的媒体

* 广告主倾向于让更多人看到广告

广告主就倾向于让更多人看到广告, 而且也为了避免麻烦, 所以会找一些大型的媒体来谈合作

但是往往一些小媒体因为更加垂直, 其用户可能更加的精准, 购买意愿也非常好

* 小媒体的议价能力非常有限

虽然小媒体有小媒体的好处, 但是小媒体太过零散, 如果只是一个小媒体的话, 很难去洽谈出一个比较好的合作

所以小媒体也是要赚钱的, 这个领域其实是一个很大的盘子, 一定会有人为小媒体提供服务, 这种产品, 我们称之为 `AdNetwork`, 广告网络

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613162139.png)

`AdNetwork` 提供如下的服务

* 为广告主提供统一的界面
* 联络多家媒体, 行成为统一的定价从而销售

`Step 3`: `AdExchange`::

虽然有 `AdNetwork` 的引入, 但是很快又会有新的问题

* `AdNetwork` 不止一家

就如同会有很多小媒体, 广告主不知道如何选择一样, `AdNetwork` 是一种商业模式, 也会有很多玩家, 广告主依然面临这种选择困难

* 小媒体们会选择不同的 `AdNetwork`

每个 `AdNetwork` 之间, 定价策略可能不同, 旗下的小媒体也可能不同, 其实最终广告主是要选择一个靠谱的网站来进行广告展示的, 那么这里就存在一些信息不对称, 如何选择靠谱的 `AdNetwork` 从而选择靠谱的媒体呢

* `AdNetwork` 之间可能存在拆借现象

某一个 `AdNetwork` 可能会有一个比较好的资源, 但是一直没卖出去, 而另外一个 `AdNetwork` 可能恰好需要用到这个资源, 所以 `AdNetwork` 之间可能会有一些拆借显现, 这就让这个时长愈加混乱

* 媒体可能对 `AdNetwork` 的定价策略颇有微词

`AdNetwork` 背后有很多媒体, 但是整个定价策略是由 `AdNetwork` 来制定的, 虽然 `AdNetwork` 往往是非常精密的计算模型, 但是媒体依然可能会感觉自己没有赚到钱

所以滋生了另外一种业务, 叫做 `AdExchange`, 广告交易平台, 从而试图去从上层再统一一下

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613165151.png)

所以, `AdExchange` 虽然看起来和 `AdNetwork` 非常类似, 但是本质上是不同的, 其有以下特点

* `AdExchange` 不仅会联系 `AdNetwork`, 也会联系一下小媒体
* 甚至有时候 `AdNetwork` 也会找 `AdExchange` 发布广告需求
* `AdExchange` 会提供实时的交易定价, 弥补了 `AdNetwork` 独立定价的弊端

`Step 4`: `RTB` 实时竞价::

本节并不是针对 `AdExchange` 的缺陷引入新的话题, 而是针对 `AdExchange` 中的一个定价特点进行详细的说明

`AdExchange` 和 `AdNetwork` 最大的不同可能要数 `AdExchange` 的定价方式了, `AdExchange` 的定价方式是一种事实的定价方式, 其实非常类似于股票的撮合交易

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613170128.png)

整个过程的步骤大致如下:

. 媒体发起广告请求给 `RTB` 系统, 请求广告进行展示

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613171716.png)

. 广告主根据自己需求决定是否竞价, 以及自己的出价

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613171747.png)

. 会有多个广告主同时出价, 价高者得

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613171854.png)

这样, `RTB` 就能尽可能的让广告的展示价格更透明更公平, `AdExchange` 得到自己响应的佣金, 媒体得到最大化的广告费, 看起来皆大欢喜, 但是真的是这样吗?

`Step 5`: 广告主如何竞价?::

一切看起来都很好, 如果你站在媒体角度的话, 但是如果你站在广告主的角度上来看, 广告主可能会有两种抱怨

* 广告主并不是专业从业者

广告主可能会觉得, 你跟我闹呢, 我知道不知道怎么出价你心里每一点数?

确实, 作为金主, 不能太过为难他们, 每次交易都让广告主出价, 无异于逼迫广告主转投他家

* 广告主的诉求是投放广告给恰好有需求的人, 而不是看起来好像很酷的媒体

我们讨论到现在, 所有的假设都是基于广告主知道自己该找什么样的媒体投放什么样的广告, 这种假设明显是不成立的, 如果考虑广告主的诉求, 其非常简单, 在同等价格内, 广告效果要好, 所以广告主更关心的事情是你是否让合适的人看到了这些广告

所以, `DSP` 应运而生, `DSP` 全称叫做需求方平台, 主要负责和 `AdExchange` 交互, 辅助广告主进行实时竞价

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613173747.png)

* `DSP` 帮助广告主进行 `RTB` 中的出价
* `DSP` 不仅只是出价, `DSP` 帮助广告主全面的进行广告服务, 例如广告主只需要告诉 `DSP` 自己对什么类型的受众感兴趣, `DSP` 会帮助广告主进行受众筛选

`Step 6`: `DMP`::

`DSP` 最重要的特性是, 能够帮助广告主筛选客户, 换句话说, `DSP` 出现之前广告主针对媒体上的广告位进行广告投放, `DSP` 出现之后, 广告主针对自己想要的目标受众投放广告, 这几乎是一个质的效率提升

广告主现在可以针对一些受众的标签来进行广告投放了, 比如说, 一个广告主是卖化妆品的, 他要投放广告给有如下三个标签的用户们, `20` 岁上下, 女性, 时尚人士, 现在就可以针对这三个标签来告诉 `DSP` 如何筛选用户了

但是 `DSP` 如何进行用户识别呢? `DSP` 如何知道谁是 `20` 岁上下, 女性, 时尚人士? `DSP` 可以自己做, 也可以依赖于第三方. 这个标签化的数据管理项目, 就叫做 `DMP`, 全称叫做 `Data Management Platform`, 即数据管理平台.

DMP 所负责的内容非常重要的有两点

* 收集用户数据

常见的收集方式主要有两种

* 通过自身的服务和程序进行收集, 例如微博和腾讯有巨大的用户量, 他们自己就可以针对自己的用户进行分析
* 通过合作而来的一些数据, 这部分在合规范围内, 一般大型的网站或者 `App` 会通过一些不会泄漏用户隐私的 `ID` 来标识用户, 给第三方 `DMP` 合作使用
* 通过一些不正当的手段获得, 例如说在某网站上传伪装成图片的脚本, 从而获取本网站的用户 `Cookie`, 这部分涉及一些黑产, 不再详细说明, `315` 晚会也曾经报道过

* 为用户打上标签

`DSP` 主要通过标签筛选用户, 所以 `DMP` 要通过一些大数据的工具来将用户数据打上标签, 这部分其实挺难, 有可能要涉及一些机器学习的算法, 或者图计算

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613180723.png)

`Ps.` 整个链条中的参与者::

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613183606.png)

### 1.2. 技术方案

[caption=""]
.导读
. `DMP` 的主要任务
. 技术方案

技术方案::

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613192400.png)

从目的上看, `DMP` 系统可能会有如下的事情要做:

* 通过可视化和笔记工具进行数据分析和测试
一般会使用 `Zeppelin` 等工具进行测试和数据探索
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190614014030.png)

* 向 `DSP` 提供数据服务

一般对外提供数据, 是以接口的形式提供的, 例如提供一个 `Http GET` 接口给 `DSP`, `DSP` 可以调用这个接口实现查询

* 接口一般使用 `Spring` 之类的框架编写 `Http` 服务实现
* 这种接口在访问数据库的时候, 就是 `OLTP` 形式了, 要尽快的获取数据, `Kudu` 和 `HBase` 较为合适

* 通过可视化内部展示运营数据

在运营过程中, 产品方可能需要时刻监控运营的一些指标, 例如注册率, 使用率, 接口调用次数等

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190614014138.png)

从工程的视角上来看, `DMP` 的工程分为如下几个部分:
[cols="2,25,~"]
|===
| 工程 | 类型 | 作用

| `dmp` | `IDEA Project` | `DMP` 项目的主工程, 编写具体的代码
| `dmp_report` | `Zeppelin Notebook` | `Zeppeline` 的一个笔记, 负责展示 `DMP` 进行数据探索和分析时所产生的报表
| `dmp_analysis` | `Zeppelin Notebook` | `Zeppeline` 的一个笔记, 负责进行 `DMP` 数据的探索, 从中不断试探发现经验
|===

真实开发的时候可能遵循如下步骤:
[cols="8,8,15,20,~"]
|===
| 序号 | 环境 | 存储 | 对应工程 | 描述

| `1` | 测试 | `Kudu` 测试 | `dmp_analysis` | 先对数据进行探索, 得出规律
| `2` | 测试 | `Kudu` 测试 | `dmp_report` | 归纳数据特征和规律, 通过报表展示
| `3` | 生产 | `Kudu` 生产 | `IDEA dmp project` | 对数据充分理解后, 编写代码进行 `ETL` 操作, 并使用 `Oozie` 等工具进行调度执行, 处理过的数据落地到 `Kudu` 表中
| `4` | 生产 | `Kudu` 生产 | `IDEA dmp project` | 生产中的数据已经经过清洗, 此时可以编写代码进行标签库等一系列的数据分析和挖掘任务, 并将结果落地到 `ElasticSearch` 中, 向 `DSP` 提供服务
| `5` | 生产 | `Kudu` 生产 | `IDEA dmp project` | 在运营过程中, 会产生一些运营指标, 可以针对运营指标进行数据分析和可视化, 提供给产品部分追踪运营状况. 此步骤应该通过 `EChats` 等工具在后台系统中进行可视化, 使用 `SQL` 来分析数据
|===

[NOTE]

* 数据会不断的来, 所以数据的清洗和 `ETL` 过程是不断重复进行的
* `ETL` 会不断的产生新的洁净数据, 所以标签库也是不断重复进行的
* 运营不断继续, `ETL` 也不断继续, 所以报表也要周期性提供
* 根据任务的情况和目的不同, 这个周期有可能是 `1` 天, `12` 小时, `1` 个月等

`DMP` 的主要任务::

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/1554645748023.png)

在我们这个学习项目中, 大家只需要了解围绕这个 DMP 有什么样的项目, 以及整体的过程和团队需要做的事情, 但是我们还是要把主要目标放在核心业务上, 我们在整个项目中将学习到如下一些内容

* 报表生成
* 标签化以及标签化相关的一系列处理

数据集生成::

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613210828.png)

这份数据集生成的步骤有如下

. 收集数据

数据来自于以往的竞价记录和收集到的用户数据

竞价记录来自于以往的交易

用户数据有可能来自第三方, 也有可能是自己收集(可能性比较小)
合并多个数据源的数据

因为在进行针对 DMP 的数据分析时, 需要用到用户的数据来判定用户的喜好, 也需要竞价数据来判定价格是否合适, 所以需要将这两部分数据合并起来, 再进行数据处理和分析

数据集概况::

数据集是一个 JSON Line 文件, 其中有三千条数据, 每一条数据都是一个独立的 JSON 字符串, 大概长如下样子

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190613212015.png)

[cols="3,~"]
|===
| 字段 | 解释

| `IP`                   a| 设备的真实 `IP`
| `sessionid`            a| 会话标识
| `advertisersid`        a| 广告主 `ID`
| `adorderid`            a| 广告 `ID`

| `adcreativeid`         a|
广告创意 `ID`

* `>= 200000` : `DSP`
* `<  200000` : `OSS`

| `adplatformproviderid` a|
广告平台商 `ID`

* `>= 100000` : `rtb`
* `< 100000` : `api`

| `sdkversionnumber`     a| `SDK` 版本号
| `adplatformkey`        a| 平台商 `Key`

| `putinmodeltype`       a|
针对广告主的投放模式

* `1` : 展示量投放
* `2` : 点击量投放

| `requestmode`          a|
数据请求方式

* `1` : 请求
* `2` : 展示
* `3` : 点击

| `adprice`              a| 广告价格
| `adppprice`            a| 平台商价格
| `requestdate`          a| 请求时间, 格式为 `yyyy-m-dd hh:mm:ss`
| `appid`                a| 应用 `ID`
| `appname`              a| 应用名称
| `uuid`                 a| 设备唯一标识, 比如 `IMEI` 或者 `AndroidID` 等
| `device`               a| 设备型号, 如 `Huawei`, `iPhone`
| `client`               a|
设备类型

* `1` : `Android`
* `2` : `iOS`
* `3` : `WP`

| `osversion`            a| 设备操作系统版本, 如 `4.0`
| `density`              a| 备屏幕的密度

* `Android` 的取值为 `0.75`, `1`, `1.5`
* `iOS` 的取值为 `1`, `2`

| `pw`                   a| 设备屏幕宽度
| `ph`                   a| 设备屏幕高度
| `longitude`            a| 设备所在经度
| `lat`                  a| 设备所在纬度
| `provincename`         a| 设备所在省份名称
| `cityname`             a| 设备所在城市名称
| `ispid`                a| 运营商 `ID`
| `ispname`              a| 运营商名称
| `networkmannerid`      a| 联网方式 `ID`

. `4G`
. `3G`
. `2G`
. `OperatorOther`

| `networkmannername`    a| 联网方式名称
| `iseffective`          a|
是否可以正常计费

* `0` : 不行
* `1` : 可以

| `isbilling`            a|
是否收费

* `0` : 未收费
* `1` : 已收费

| `adspacetype`          a|
广告位类型

* `1` : `Banner`
* `2` : 插屏
* `3` : 全屏

| `adspacetypename`      a| 广告位类型名称, 如 `Banner`, 插屏, 全屏
| `devicetype`           a|
设备类型（1：手机 2：平板）

* `1` : 手机
* `2` : 平板

| `processnode`          a|
流程节点

* `1` : 请求量 `KPI`
* `2` : 有效请求
* `3` : 广告请求

| `apptype`              a| 应用类型 `ID`
| `district`             a| 设备所在县名称
| `paymode`              a|
针对平台商的支付模式

* `1` : 展示量投放, CPM
* `2` : 点击量投放, CPC

| `isbid`                a| 是否是 `RTB`
| `bidprice`             a| `RTB` 竞价价格
| `winprice`             a| `RTB` 竞价成功价格
| `iswin`                a| 是否竞价成功
| `cur`                  a| 结算币种, `USD`, `RMB` 等
| `rate`                 a| 汇率
| `cnywinprice`          a| `RTB` 竞价成功转换成人民币的价格
| `imei`                 a| 手机串码
| `mac`                  a| 手机 `MAC` 地址
| `idfa`                 a| 手机 `APP` 的广告码
| `openudid`             a| 苹果设备的识别码
| `androidid`            a| 安卓设备的识别码
| `rtbprovince`          a| `RTB` 省
| `rtbcity`              a| `RTB` 市
| `rtbdistrict`          a| `RTB` 区
| `rtbstreet`            a| `RTB` 街道
| `storeurl`             a| `APP` 的市场下载地址
| `realip`               a| 真实 `IP`
| `isqualityapp`         a| 优选标识
| `bidfloor`             a| 底价
| `aw`                   a| 广告位的宽
| `ah`                   a| 广告位的高
| `imeimd5`              a| `IMEI` 的 `MD5` 值
| `macmd5`               a| `MAC` 的 `MD5` 值
| `idfamd5`              a| `IDFA` 的 `MD5` 值
| `openudidmd5`          a| `OpenUDID` 的 `MD5` 值
| `androididmd5`         a| `AndroidID` 的 `MD5` 值
| `imeisha1`             a| `IMEI` 的 `SHA-1` 值
| `macsha1`              a| `MAC` 的 `SHA-1` 值
| `idfasha1`             a| `IDFA` 的 `SHA-1` 值
| `openudidsha1`         a| `OpenUDID` 的 `SHA-1` 值
| `androididsha1`        a| `AndroidID` 的 `SHA-1` 值
| `uuidunknow`           a| `UUID` 的密文
| `userid`               a| 平台用户 `ID`
| `iptype`               a|
表示 `IP` 库类型

* `1` : 为点媒 `IP` 库
* `2` : 为广告协会的 `IP` 地理信息标准库

默认为1

| `initbidprice`         a| 初始出价
| `adpayment`            a| 转换后的广告消费 (保留小数点后 `6` 位)
| `agentrate`            a| 代理商利润率
| `lomarkrate`           a| 代理利润率
| `adxrate`              a| 媒介利润率
| `title`                a| 标题
| `keywords`             a| 关键字
| `tagid`                a| 广告位标识 (当视频流量时值为视频 `ID` 号)
| `callbackdate`         a| 回调时间, 格式为 `yyyy/MM/dd hh:mm:ss`
| `channelid`            a| 频道 `ID`
| `mediatype`            a| 媒体类型
| `email`                a| 用户邮箱
| `tel`                  a| 用户电话号码
| `sex`                  a| 用户性别
| `age`                  a| 用户年龄
|===

## 2. 工程创建和框架搭建

[caption=""]
.导读
. 创建工程
. 搭建框架
. 建立配置框架

`Step 1`: 创建工程::

已经到最后一个阶段了, 不再详细说工程如何创建了, 看一下步骤即可:
. 在 `IDEA` 创建 `Maven` 工程, 选择存储位置
. 工程命名为 `dmp`, 注意工程名一般小写, 大家也可以采用自己喜欢的命名方式, 在公司里, 要采用公司的习惯
. 导入 `Maven` 依赖
. 创建 `Scala` 代码目录
. 创建对应的包们

创建需要的包:
[cols="30,~"]
|===
| 包名 | 描述

| `com.itheima.dmp.etl` | 放置数据转换任务
| `com.itheima.dmp.report` | 放置报表任务
| `com.itheima.dmp.tags` | 放置标签有关的任务
| `com.itheima.dmp.utils` | 放置一些通用公用的类
|===

导入需要的依赖:
在 `DMP` 中, 暂时先不提供完整的 `Maven POM` 文件, 在一开始只导入必备的, 随着项目的进程, 用到什么再导入什么, 以下是必备的

* `Spark` 全家桶
* `Kudu` 一套
* `Scala` 依赖
* `SLF4J` 日志依赖
* `Junit` 单元测试
* `Java` 编译插件
* `Scala` 编译插件
* `Uber Jar` 编译插件 `Shade`
* `CDH Repo` 仓库, 需要一个 `CDH` 的 `Maven` 仓库配置是因为用到 `CDH` 版本的 `Kudu`

```xml
<properties>
    <scala.version>2.11.8</scala.version>
    <spark.version>2.2.0</spark.version>
    <hadoopo.version>2.6.1</hadoopo.version>
    <kudu.version>1.7.0-cdh5.16.0</kudu.version>
    <maven.version>3.5.1</maven.version>
    <junit.version>4.12</junit.version>
</properties>

<dependencies>
    <!-- Spark -->
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
        <version>${hadoopo.version}</version>
    </dependency>

    <!-- Kudu client -->
    <dependency>
        <groupId>org.apache.kudu</groupId>
        <artifactId>kudu-client</artifactId>
        <version>1.7.0-cdh5.16.1</version>
    </dependency>

    <!-- Kudu Spark -->
    <dependency>
        <groupId>org.apache.kudu</groupId>
        <artifactId>kudu-spark2_2.11</artifactId>
        <version>1.7.0-cdh5.16.1</version>
    </dependency>

    <!-- Logging -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>1.7.12</version>
    </dependency>

    <!-- Unit testing -->
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>${junit.version}</version>
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
            <version>${maven.version}</version>
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

        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>2.4</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>

<repositories>
    <repository>
        <id>cdh.repo</id>
        <name>Cloudera Repositories</name>
        <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
</repositories>
```

`Step 2`: 框架搭建::

编写 `Spark` 程序的时候, 往往不需要一个非常复杂的框架, 只是对一些基础内容的抽象和封装即可, 但是也要考虑如下问题

* 有哪些任务是要执行的

在做一个项目的时候, 尽量从全局的角度去看, 要考虑到周边的一些环境, 例如说回答自己如下几个问题

[cols="30,~"]
|===
| 问题 | 初步分析和解答

| 这个应用有几个入口 | 这个程序的入口数量是不确定的, 随着工作的进展而变化, 但是至少要有两个入口, 一个是生成报表数据, 一个是处理用户的标签数据
| 这个应用会放在什么地方执行 | 分为测试和生产, 测试可以直接使用 IDEA 执行, 生成需要打包并发送到集群执行
| 这个应用如何调度 | 这个应用包含了不止一个任务, 最终会由 `Oozie`, `Azkaban`, `AirFlow` 等工具去调度执行
|===

* 有哪些操作可能会导致重复代码过多

其实无论是 `Spring`, 还是 `Vue`, 还是 `Spark`, 这些框架和工具, 最终的目的都是帮助我们消除一些重复的和通用的代码

所以既然我们无需在 Spark 的应用中搭建复杂的项目框架, 但是对于重复的代码还是要消除的, 初步来看可能会有如下重复的代码点

* 各个数据库的访问
* 配置的读取

`Step 3`: 建立配置读取工具::

. 了解配置文件和读取框架

数据读取部分, 有一个比较好用的工具, 叫做 `lightbend/config`, 它可以读取一种叫做 `HOCON` 的配置文件

* `HOCON` 全称叫做 `Human-Optimized Config Object Notation`, 翻译过来叫做 为人类优化的配置对象表示法

`HOCON` 是一种类似于 `Properties` 的配置文件格式, 并包含 `JSON` 的语法格式, 比较易于使用, 其大致格式如下

```text
foo: {
  bar: 10,
  baz: 12
}

foo {
  bar = 10,
  baz = 12
}

foo.bar=10
foo.baz=10
```

以上三种写法是等价的, 其解析结果都是两个字段, 分别叫做 `foo.bar` 和 `foo.baz`

* 读取 `HOCON` 文件格式需要使用 `lightbend/config`, 它的使用非常的简单

当配置文件被命名为 `application.conf` 并且被放置于 `resources` 时, 可以使用如下方式直接加载

```scala
val config: Config = ConfigFactory.load()
val bar = config.getInt("foo.bar")
val baz = config.getInt("foo.baz")
```

创建配置文件

创建配置文件 `resource/spark.conf`, 并引入如下内容

```text
# Worker 心跳超时时间
spark.worker.timeout="500"

# RPC 请求等待结果的超时时间
spark.rpc.askTimeout="600s"

# 所有网络操作的等待时间, spark.rpc.askTimeout 默认值等同于这个参数
spark.network.timeoout="600s"

# 最大使用的 CPU 核心数
spark.cores.max="10"

# 任务最大允许失败次数
spark.task.maxFailures="5"

# 如果开启推测执行, 开启会尽可能的增快任务执行效率, 但是会占用额外的运算资源
spark.speculation="true"

# Driver 是否允许多个 Context
spark.driver.allowMutilpleContext="true"

# Spark 序列化的方式, 使用 Kryo 能提升序列化和反序列化的性能
spark.serializer="org.apache.spark.serializer.KryoSerializer"

# 每个页缓存, Page 指的是操作系统的内存分配策略中的 Page, 一个 Page 代表一组连续的内存空间
# Spark 在引入钨丝计划以后, 使用 Java 的 Unsafe API 直接申请内存, 其申请单位就是 Page
# 如果 Page 过大, 有可能因为操作系统的策略无法分配而拒绝这次内存申请, 从而报错
# 简单来说, 这个配置的作用是一次申请的内存大小
spark.buffer.pageSize="6m"
```

以上的配置列成表如下

|===
| 配置项目 | 描述

| `spark.worker.timeout` | 如果超过了这个配置项指定的时间, `Master` 认为 `Worker` 已经跪了
| `spark.network.timeout` | 因为 `Spark` 管理一整个集群, 任务可能运行在不同的节点上, 后通过网络进行通信, 一次网络通信有可能因为要访问的节点实效而一直等待, 这个配置项所配置的便是这个等待的超时时间
| `spark.cores.max` | `Spark` 整个应用最大能够申请的 `CPU` 核心数
| `spark.task.maxFailures` | `Spark` 本身是支持弹性容错的, 所以不能因为某一个 `Task` 失败了, 就认定整个 `Job` 失败, 一般会因为相当一部分 `Task` 失败了才会认定 `Job` 失败, 否则会进行重新调度, 这个参数的含义是, 当多少个 `Task` 失败了, 可以认定 `Job` 失败
| `spark.speculation` | 类似 `Hadoop`, `Spark` 也支持推测执行, 场景是有可能因为某台机器的负载过高, 或者其它原因, 导致这台机器运行能力很差, `Spark` 会根据一些策略检测较慢的任务, 去启动备用任务执行, 使用执行较快的任务的结果, 但是推测执行有个弊端, 就是有可能一个任务会执行多份, 浪费集群资源
| `spark.driver.allowMutilpleContext` | 很少有机会必须一定要在一个 `Spark Application` 中启动多个 `Context`, 所以这个配置项意义不大, 当必须要使用多个 `Context` 的时候, 开启此配置即可
| `spark.serializer` | `Spark` 将任务分发到集群中执行, 所以势必涉及序列化, 这个配置项配置的是使用什么序列化器, 默认是 `JDK` 的序列化器, 可以指定为 `Kyro` 从而提升性能, 但是如果使用 `Kyro` 的话需要序列化的类需要被先注册才能使用
| `spark.buffer.pageSize` | 每个页缓存, `Page` 指的是操作系统的内存分配策略中的 `Page`, 一个 `Page` 代表一组连续的内存空间, `Spark` 在引入钨丝计划以后, 使用 `Java` 的 `Unsafe API` 直接申请内存, 其申请单位就是 `Page`, 如果 `Page` 过大, 有可能因为操作系统的策略无法分配而拒绝这次内存申请, 从而报错, 简单来说, 这个配置的作用是一次申请的内存大小, 一般在报错的时候修改这个配置, 减少一次申请的内存
|===

. 导入配置读取的工具依赖

. 在 `pom.xml` 中的 `properties` 段增加如下内容

```text
<config.version>1.3.4</config.version>
```

. 在 `pom.xml` 中的 `dependencites` 段增加如下内容

```text
<!-- Config reader -->
<dependency>
    <groupId>com.typesafe</groupId>
    <artifactId>config</artifactId>
    <version>${config.version}</version>
</dependency>
```

. 配置工具的设计思路

在设计一个工具的时候, 第一步永远是明确需求, 我们现在为 `SparkSession` 的创建设置配置加载工具, 其需求如下:

* 配置在配置文件中编写
* 使用 `typesafe/config` 加载配置文件
* 在创建 `SparkSession` 的时候填写这些配置

前两点无需多说, 已经自表达, 其难点也就在于如何在 `SparkSession` 创建的时候填入配置, 大致思考的话, 有如下几种方式:

* 加载配置文件后, 逐个将配置的项设置给 `SparkSession`
`spark.config("spark.worker.timeout", config.get("spark.worker.timeout"))`
* 加载配置文件后, 通过隐式转换为 `SparkSession` 设置配置

`spark.loadConfig().getOrCreate()`

毫无疑问, 第二种方式更为方便

. 创建配置工具类

看代码之前, 先了解一下设计目标:

* 加载配置文件 `spark.conf`
* 无论配置文件中有多少配置都全部加载
* 为 `SparkSession` 提供隐式转换自动装载配置

下面是代码, 以及重点解读:

```text
class SparkConfigHelper(builder: SparkSession.Builder) {

  private val config: Config = ConfigFactory.load("spark")            // <1>

  def loadConfig(): SparkSession.Builder = {
    import scala.collection.JavaConverters._

    for (entry <- config.entrySet().asScala) {
      val value = entry.getValue
      val valueType = value.valueType()
      val valueFrom = value.origin().filename()                       // <2>
      if (valueType ## ConfigValueType.STRING && valueFrom != null) { // <3>
        val value = value.unwrapped().asInstanceOf[String]
        builder.config(entry.getKey, value)                           // <4>
      }
    }

    builder
  }
}

object SparkConfigHelper {                                            // <5>

  def apply(builder: SparkSession.Builder): SparkConfigHelper = {
    new SparkConfigHelper(builder)
  }

  implicit def setSparkSession(builder: SparkSession.Builder) = {     // <6>
    SparkConfigHelper(builder)
  }
}
```

<1> : 加载配置文件
<2> : 因为 `Config` 工具会自动的加载所有的系统变量, 需要通过 `Origin` 来源判断, 只接收来自于文件的配置
<3> : 判断: 1. 是 `String` 类型, 2. 来自于某个配置文件
<4> : 为 `SparkSession` 设置参数
<5> : 提供伴生对象的意义在于两点: 1. 更方便的创建配置类, 2. 提供隐式转换, 3. 以后可能需要获取某个配置项
<6> : 提供隐式转换, 将 `SparkSession` 转为 `ConfigHelper` 对象, 从而提供配置加载

## 3. 将数据集中的 IP 转为地域信息

[caption=""]
.导读
. `IP` 转换工具介绍
. 转换

`IP` 转换工具介绍::

进行 `IP` 转换这种操作, 一般有如下一些办法:
[cols="10,~,20,20"]
|===
| 方式 | 描述 | 优点 | 缺点

| 自己编写 a|
一般如果自己编写查找算法的话, 大致有如下几步

. 找到一个 `IP范围 : 省 : 市 : 区` 这样的数据集
. 读取数据集
. 将 `IP` 转为数字表示法, 本质上 `IP` 就是二进制的点位表示法, `192.168.0.1 -> 1100 0000 1010 1000 0000 0000 0000 0001 -> 3232235521`
. 使用 `3232235521` 这样的数字在 `IP` 数据集中通过二分法查找对应的省市区

| 没有第三方库的学习成本 a|

* 没有数据结构上的支持, 效率低
* 没有上层的封装, 使用麻烦

| 第三方库 a|
一般第三方库会有一些数据结构上的优化, 查找速度比二分法会更快一些, 例如 `BTree` 就特别适合做索引, 常见的方式有

* `GeoLite`
* 纯真数据库
* `ip2region`

a|

* 速度快
* 不麻烦
* 有上层封装, 用着爽
* 第三方一般会提供数据集, 数据集会定时更新, 更精准
* 有轮子就别自己搞了, 怪麻烦的

| 需要学习第三方工具, 有一定的学习成本, 而且不一定和以后工作中用同样一个工具

|===

选用 `ip2region` 这个工具来查找省市名称:

* `ip2region` 的优点

|===
| 工具 | 数据结构支持 | 中文支持

| `GeoLite` | 有 | 无
| 纯真 | 无 | 有
| `ip2region` | 有 | 有
|===

* 引入 `ip2region`

. 复制 `IP` 数据集 `ip2region.db` 到工程下的 `dataset` 目录
. 在 `Maven` 中增加如下代码

```xml
<dependency>
    <groupId>org.lionsoul</groupId>
    <artifactId>ip2region</artifactId>
    <version>1.7.2</version>
</dependency>
```

* `ip2region` 的使用

```scala
val searcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db")
val data = searcher.btreeSearch(ip)
println(data.getRegion)
```

选用 `GeoLite` 确定经纬度:

* 后面需要使用经纬度, 只有 `GeoLite` 可以查找经纬度

|===
| 工具 | 数据结构支持 | 中文支持 | 经纬度支持

| `GeoLite` | 有 | 无 | 有
| 纯真 | 无 | 有 | 无
| `ip2region` | 有 | 有 | 无
|===

* 引入 `GeoLite`

. 将 `GeoLite` 的数据集 `GeoLiteCity.dat` 拷贝到工程中 `dataset` 目录下
. 在 `pom.xml` 中添加如下依赖

```xml
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
```

* `GeoLite` 的使用方式

```scala
val lookupService = new LookupService("dataset/GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
val location = lookupService.getLocation("121.76.98.134")
println(location.latitude, location.longitude)
```

`IP` 转换思路梳理::

现在使用不同的视角, 理解一下在这个环节我们需要做的事情

工具视角:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190614162114.png)

数据视角:
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190614162634.png)

要做的事:

* 读取数据集 `pmt.json`
* 将 `IP` 转换为省市
* 设计 `Kudu` 表结构, 创建 `Kudu` 表
* 存入 `Kudu` 表

挑战和结构::

* 现在的任务本质上是把一个 `数据集 A` 转为 `数据集 B`, `数据集 A` 可能不够好, `数据集 B` 相对较好, 然后把 `数据集 B` 落地到 `Kudu`, 作为 `ODS` 层, 以供其它功能使用

* 但是如果 `数据集 A` 转为 `数据集 B` 的过程中需要多种转换呢?

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190614165344.png)

* 所以, 从面向对象的角度上来说, 需要一套机制, 能够组织不同的功能协同运行

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190614170054.png)

* 所以我们可以使用一个名为 `PmtETLRunner` 的类代表针对 `数据集 A` 到 `数据集 B` 的转换, 然后抽象出单位更小的负责具体某一个转换步骤的节点, 集成到 `PmtETLRunner` 中, 共同完成任务

参数配置::

. 为了让程序行为更可控制, 所以一般会在编写程序之前先大致计划以下程序中可能使用到的一些参数

* `Kudu` 的表名
* `Kudu` 表的复制因子
* `ODS` 层的表名

. 规划好以后, 着手创建配置文件 `resource/kudu.conf`

```text
# Server properties
kudu.common.master="192.168.169.101:7051,192.168.169.102:7051,192.168.169.103:7051"
kudu.common.factor=1

# Table name
kudu.name.pmt_ods="ODS_"
```

`Kudu` 的支持库::

为了方便 `Kudu` 的使用, 所以要创建一个 `Kudu` 的 `Helper`, 大致需求如下:
|===
| 需求 | 原始调用方式 | 理想调用方式

| 创建表 | `KuduContext.createTable()` | `SparkSession.createKuduTable()`
| 读取表 | `spark.read.option("kudu.master", KUDU_MASTERS).option("kudu.table", tableName).kudu`| `SparkSession.readKuduTable(tableName)`
| 通过 `DataFrame` 将数据保存到 `Kudu` 表 | `DataFrame.write.options(...).kudu` | `DataFrame.saveAsKuduTable`
|===

`KuduHelper` 的设计:

`KuduHelper` 的设计思路两句话可以总结

* 尽可能的不在处理类中读取配置文件
* 尽可能的提供易于调用的接口

```text
class KuduHelper {                                    // <1>
  private var spark: SparkSession = _
  private var dataFrame: DataFrame = _
  private val config = ConfigFactory.load("kudu")
  private val KUDU_MASTERS = config.getString("kudu.common.master")
  private var kuduContext: KuduContext = _            // <4>

  def this(spark: SparkSession) = {                   // <2>
    this()
    this.spark = spark
    this.kuduContext = new KuduContext(KUDU_MASTERS, spark.sparkContext)
  }

  def this(dataFrame: DataFrame) = {                  // <3>
    this(dataFrame.sparkSession)
    this.dataFrame = dataFrame
  }

  def createKuduTable(tableName: String, schema: Schema,
    partitionKey: Seq[String]): Unit = {              // <5>
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }

    import scala.collection.JavaConverters._
    val options = new CreateTableOptions()
      .setNumReplicas(config.getInt("kudu.common.factor"))
      .addHashPartitions(partitionKey.asJava, 6)

    kuduContext.createTable(tableName, schema, options)
  }

  def saveToKudu(tableName: String): Unit = {         // <6>
    if (dataFrame ## null) {
      throw new RuntimeException("请在 DataFrame 上调用 saveToKudu")
    }

    import org.apache.kudu.spark.kudu._

    dataFrame.write
      .option("kudu.table", tableName)
      .option("kudu.master", KUDU_MASTERS)
      .mode(SaveMode.Append)
      .kudu
  }

  def readKuduTable(tableName: String): Option[DataFrame] = { // <7>
    if (StringUtils.isBlank(tableName)) {
      throw new RuntimeException("请传入合法的表名")
    }

    import org.apache.kudu.spark.kudu._

    if (kuduContext.tableExists(tableName)) {
      val dataFrame = spark.read
        .option("kudu.master", KUDU_MASTERS)
        .option("kudu.table", tableName)
        .kudu

      Some(dataFrame)
    } else {
      None
    }
  }

}

object KuduHelper {

  implicit def sparkToKuduContext(spark: SparkSession): Unit = {     // <8>
    new KuduHelper(spark)
  }

  implicit def datasetToKuduContext(dataset: Dataset[Any]): Unit = { // <9>
    new KuduHelper(dataset)
  }
}
```

<1> : 主题设计思路就是将 `SparkSession` 或者 `DataFrame` 隐式转换为 `KuduHelper`, 在 `KuduHelper` 中提供帮助方法
<2> : 将 `SparkSession` 转为 `KuduHelper` 时调用
<3> : 将 `Dataset` 转为 `KuduHelper` 时调用
<4> : 在 `Helper` 内部读取配置文件, 创建 `KuduContext`
<5> : 此方法就是设计目标 `SparkSession.createKuduTable(tableName)` 中被调用的方法
<6> : 此方法就是设计目标 `DataFrame.saveToKudu(tableName)` 中被调用的方法
<7> : 此方法就是设计目标 `SparkSession.readKuduTable(tableName)` 中被调用的方法
<8> : 将 `SparkSession` 转为 `KuduHelper`
<9> : 将 `Dataset` 转为 `KuduHelper`

`ETL` 代码编写::

. 创建 `PmtETLRunner` 类

`PmtETLRunner` 类负责整个 `ETL` 过程, 但是不复杂中间过程中具体的数据处理, 具体数据如何转换, 要做什么事情由具体的某个 `Converter` 类负责

```text
object PmtETLRunner {

  def main(args: Array[String]): Unit = {
    import com.itheima.dmp.utils.SparkConfigHelper._
    import com.itheima.dmp.utils.KuduHelper._

    // 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("pmt_etl")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._

    // 读取数据
    val originDataset = spark.read.json("dataset/pmt.json")

  }
}
```

. 创建 `IPConverter` 类处理 `IP` 转换的问题

`IPConverter` 主要解决如下问题

* 原始数据集中有一个 `ip` 列, 要把 `ip` 这一列数据转为五列, 分别是 `ip`, `Longitude`, `latitude`, `region`, `city`, 从而扩充省市信息和经纬度信息
* 将新创建的四列数据添加到原数据集中

```text
object IPConverter {

  def process(dataset: Dataset[Row]): Dataset[Row] = {
    val dataConverted: RDD[Row] = dataset
      .rdd
      .mapPartitions(convertIPtoLocation)                                  // <1>

    val schema = dataset.schema
      .add("region", StringType)
      .add("city", StringType)
      .add("longitude", DoubleType)
      .add("latitude", DoubleType)

    val completeDataFrame = dataset
      .sparkSession
      .createDataFrame(dataConverted, schema)                              // <6>

    completeDataFrame
  }

  def convertIPtoLocation(iterator: Iterator[Row]): Iterator[Row] = {      // <2>
    val searcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db")

    val lookupService = new LookupService(
      "dataset/GeoLiteCity.dat",
      LookupService.GEOIP_MEMORY_CACHE)

    iterator.map(row => {
      val ip = row.getAs[String]("ip")

      val regionData = searcher.btreeSearch(ip).getRegion.split("\\|")     // <3>
      val region = regionData(2)
      val city = regionData(3)

      val location = lookupService.getLocation(ip)                         // <4>
      val longitude = location.longitude.toDouble
      val latitude = location.latitude.toDouble

      val rowSeq = row.toSeq :+ region :+ city :+ longitude :+ latitude
      Row.fromSeq(rowSeq)                                                  // <5>
    })
  }
}
```

<1> : 通过 `mapPartitions` 算子, 对每一个分区数据调用 `convertIPtoLocation` 进行转换, 需要注意的是, 这个地方已经被转为 `RDD` 了, 而不是 `DataFrame`, 因为 `DataFrame` 在转换中不能更改 `Schema`
<2> : `convertIPtoLocation` 主要做的事情是扩充原来的 `row`, 增加四个新的列
<3> : 获取省市中文名
<4> : 获取经纬度
<5> : 根据原始 `row` 生成新的 `row`, 新的 `row` 中包含了省市和经纬度信息
<6> : 新的 `row` 对象的 `RDD` 结合 被扩充过的 `Schema`, 合并生成新的 `DataFrame` 返回给 `Processor`

. 在 `PmtETLRunner` 中调用 `IPConverter`

```scala
object PmtETLRunner {

  def main(args: Array[String]): Unit = {
    ...

    val originDataset = spark.read.json("dataset/pmt.json")

    // 调用 IPConverter, 传入 originDataset, 生成包含经纬度和省市的 DataFrame
    val ipConvertedResult = IPConverter.process(originDataset)

    // 要 Select 的列们, 用于组织要包含的结果集中的数据
    // 因为太多, 不再此处展示, 若要查看, 请移步代码工程
    val selectRows: Seq[Column] = Seq(...)

    // 选中相应的列
    ipConvertedResult.select(selectRows:_*).show()
  }
```

. 将已经处理好的数据存入 `Kudu` 表中

. 思路

. 创建 `Kudu` 表 (需要表名)
. 向 `Kudu` 表中插入数据 (需要表名和数据)
在 `KuduHelper` 伴生对象中增加日期获取方法

```scala
object KuduHelper {

  ...

  def formattedDate(): String = {
    FastDateFormat.getInstance("yyyyMMdd").format(new Date)
  }
}
```

. 在 `PmtETLRunner` 中获取结果并存储到 `Kudu` 中

```scala
object PmtETLRunner {
  val ODS_TABLE_NAME: String = "ODS_" + KuduHelper.formattedDate()

  def main(args: Array[String]): Unit = {

    ...

    val selectRows: Seq[Column] = Seq(
      ...
    )

    // 创建 Kudu 表
    spark.createKuduTable(ODS_TABLE_NAME, schema, Seq("uuid"))
    // 将 DataFrame 的数据落地到 Kudu
    ipConvertedResult.select(selectRows:_*).saveToKudu(ODS_TABLE_NAME)
  }

  import scala.collection.JavaConverters._

  lazy val schema = new Schema(List(
    ...
  ))

}
```

## 4. 报表生成

目标:
一般的系统需要使用报表来展示公司的运营情况, 数据情况, 这个章节对数据进行一些常见报表的开发

步骤:
. 生成数据集的地域分布报表数据
. 生成广告投放的地域分布报表数据
. 生成广告投放的 `APP` 分布报表数据
. 生成广告投放的设备分布报表数据
. 生成广告投放的网络类型分布报表数据
. 生成广告投放的网络运营商分布报表数据
. 生成广告投放的渠道分布报表数据

### 4.1. 按照地域统计数据集的分布情况

目标和步骤::

目标:
按照地域统计数据集的分布情况, 看到不同的地区有多少数据, 从而能够根据地区优化公司运营策略

步骤:
. 需求分析, 从结果看过程
. 代码编写

需求分析::

报表生成其实就通过现有的数据, 先进行统计, 后生成报表, 然后落地到 `Kudu` 中

. 结果表

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190705180037.png)

* 三个字段
** 省
** 市
** 数据量
* 含义就是想看到所有的省市, 分别有多少数据

. 处理过程

. 读取 `ODS` 数据
. 按照省市分组
. 统计数量
. 落地 `Kudu`

代码编写::

. 创建文件 `com.itheima.dmp.report.RegionReportProcessor`
. 代码编写

```scala
object RegionReportProcessor {

  def main(args: Array[String]): Unit = {
    import com.itheima.dmp.utils.SparkConfigHelper._
    import com.itheima.dmp.utils.KuduHelper._
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder()                      // <1>
      .master("local[6]")
      .appName("pmt_etl")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._

    val origin = spark.readKuduTable(ORIGIN_TABLE_NAME)     // <2>

    if (origin.isEmpty) return

    val result = origin.get.groupBy($"region", $"city")     // <3>
      .agg(count($"*") as "count")
      .select($"region", $"city", $"count")

    result.createKuduTable(TARGET_TABLE_NAME, schema, keys) // <4>
    result.saveToKudu(TARGET_TABLE_NAME)                    // <5>
  }

  private val ORIGIN_TABLE_NAME = PmtETLRunner.ODS_TABLE_NAME
  private val TARGET_TABLE_NAME = "ANALYSIS_REGION_" + KuduHelper.formattedDate()

  import scala.collection.JavaConverters._
  private val schema = new Schema(List(
      new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build,
      new ColumnSchemaBuilder("city", Type.STRING).nullable(true).key(true).build(),
      new ColumnSchemaBuilder("count", Type.STRING).nullable(true).key(false).build()
    ).asJava
  )

  private val keys = Seq("region", "city")
}
```

<1> 创建 `SparkSession`
<2> 读取数据
<3> 处理数据
<4> 创建落地表
<5> 落地到 `Kudu` 表中

. 运行测试

. 在 `IDEA` 本地运行 `main` 方法
. 在 `WebUI` 查看 `Kudu` 中的表

`Kudu` 的 `WebUI` 地址: `cdh01:8051`

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190705161943.png)
使用 `Impala` 查看数据

. 在 `Impala` 中创建表

```sql
CREATE EXTERNAL TABLE `ANALYSIS_REGION_20190705` STORED AS KUDU
TBLPROPERTIES(
    'kudu.table_name' = 'ANALYSIS_REGION_20190705',
    'kudu.master_addresses' = 'cdh01:7051,cdh02:7051,cdh03:7051');
```

. 使用如下 `SQL` 查看内容

```sql
select * from ANALYSIS_REGION_20190705;
```

总结::

. 整体分析: 整体上的编写步骤

. 创建 `SparkSession`
. 从 `ODS` 层经过 `IP` 转换的表中读取数据
. 从读出的数据中按照省和市的信息分组
. 求出每组数据的总数, 从而得到报表数据
. 将报表数据放入对应的 `Kudu` 表中

. 细节分析: 数据处理流程

```scala
val result = origin.groupBy($"region", $"city")
  .agg(count($"*") as "count")
  .select($"region", $"city", $"count")
```

. 按照 `regison` 和 `city` 分组
. 对分组过的数据进行聚合统计, 得出 `count`
. 筛选出需要的列

. 细节分析: 读取和落地

* `SparkSession` 对象被隐式转换后, 可以使用 `KuduHelper` 中的方法, 隐式转换便是由以下部分代码完成

```scala
object KuduHelper {

  implicit def sparkToKuduContext(spark: SparkSession): KuduHelper = {
    new KuduHelper(spark)
  }

  ...
}
```

* `Dataset` 对象也可以被隐式转换为 `KuduHelper`

```scala
object KuduHelper {

  ...

  implicit def datasetToKuduContext(dataFrame: DataFrame): KuduHelper = {
    new KuduHelper(dataFrame)
  }

  ...
}
```

* 数据的读取使用 `KuduHelper` 中的 `readKuduTable` 方法

```scala
spark.readKuduTable(ORIGIN_TABLE_NAME)
```

* 表的创建使用 `KuduHelper` 中的 `createKuduTable` 方法

```scala
spark.createKuduTable(TARGET_TABLE_NAME, schema, keys)
```

* 数据的落地使用 `Dataset` 转换而成的 `KuduHelper` 中的 `saveToKudu` 方法, 注意这个方法必须要从 `Dataset` 调用

```scala
result.saveToKudu(TARGET_TABLE_NAME)
```

### 4.2. 抽取公共类

目标和步骤::

目标:

* 所有的报表统计基本上都是从一个表中读取数据, 经过处理, 落地到另外一张表中, 所以可以抽取出框架
* 面向对象是一个非常重要的技能
* 通过这个小节的学习, 抽取一个简单的框架, 对于以后查看源码和重构优化代码有非常大的帮助

步骤:
. 分析现有代码逻辑
. 框架设计
. 修改现有代码

分析现有代码逻辑::

[NOTE]
公共代码抽取和设计的步骤

. 详细的观察代码, 从中找到流程, 找到重复的东西
. 从流程中, 找到可能会有变化的点
. 抽取这些变化点
. 抽取流程

```scala
object RegionReportProcessor {

  def main(args: Array[String]): Unit = {
    import com.itheima.dmp.utils.SparkConfigHelper._
    import com.itheima.dmp.utils.KuduHelper._
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("pmt_etl")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._

    val origin = spark.readKuduTable(ORIGIN_TABLE_NAME)

    if (origin.isEmpty) return

    val result = origin.get.groupBy($"region", $"city")
      .agg(count($"*") as "count")
      .select($"region", $"city", $"count")

    spark.createKuduTable(TARGET_TABLE_NAME, schema, keys)
    result.saveToKudu(TARGET_TABLE_NAME)
  }

  private val ORIGIN_TABLE_NAME = PmtETLRunner.ODS_TABLE_NAME
  private val TARGET_TABLE_NAME = "ANALYSIS_REGION_" + KuduHelper.formattedDate()

  import scala.collection.JavaConverters._
  private val schema = new Schema(List(
      new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build,
      new ColumnSchemaBuilder("city", Type.STRING).nullable(true).key(true).build(),
      new ColumnSchemaBuilder("count", Type.STRING).nullable(true).key(false).build()
    ).asJava
  )

  private val keys = Seq("region", "city")
}
```

可变点:
. 要读取的原始表

```scala
spark.readKuduTable(ORIGIN_TABLE_NAME)
```

. 数据处理流程, 和要落地的数据

```scala
val result = origin.groupBy($"region", $"city")
  .agg(count($"*") as "count")
  .select($"region", $"city", $"count")
```

. 要落地的表名

```scala
spark.createKuduTable(TARGET_TABLE_NAME, schema, keys)
result.saveToKudu(TARGET_TABLE_NAME)
```

. 要落地的表结构

```scala
private val schema = new Schema(List(
    new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build,
    new ColumnSchemaBuilder("city", Type.STRING).nullable(true).key(true).build(),
    new ColumnSchemaBuilder("count", Type.STRING).nullable(true).key(false).build()
  ).asJava
)
```

. 要落地的表分区 `Key`

```scala
private val keys = Seq("region", "city")
```

公共流程:
. 创建 `SparkSession` 对象

```scala
val spark = SparkSession.builder()
  .master("local[6]")
  .appName("pmt_etl")
  .loadConfig()
  .getOrCreate()
```

. 读取表, 虽然名字不同, 但是读取的动作所有的报表统计中都要有

```scala
spark.readKuduTable(ORIGIN_TABLE_NAME)
```

. 处理数据, 虽然数据处理后生成的表数据不同, 但是数据都要经过处理

```scala
val result = origin.groupBy($"region", $"city")
  .agg(count($"*") as "count")
  .select($"region", $"city", $"count")
```

. 创建落地表, 虽然落地表的结构, 表名, 分区 `Key` 不同, 但是都要创建这样一张表

```scala
spark.createKuduTable(TARGET_TABLE_NAME, schema, keys)
```

. 落地表数据, 虽然要落地的表名不同, 但是都要落地

```scala
result.saveToKudu(TARGET_TABLE_NAME)
```

框架设计::

可变点 `Processor`:
需要有一个对象, 表示不同的报表如何统计, 有如下要求

* 这个对象能够提供数据处理的过程
* 这个对象能够提供源表的名称
* 这个对象能够提供目标表的名称
* 这个对象能够提供目标表的结构信息
* 这个对象能够提供目标表的分区 `key`

```scala
trait ReportProcessor {

  def process(origin: DataFrame): DataFrame

  def sourceTableName(): String

  def targetTableName(): String

  def targetTableSchema(): Schema

  def targetTableKeys(): List[String]
}
```

公共流程 `DailyReportRunner`:
每一个报表都有自己的独特信息, 但是所有的报表统计流程却是比较相似, 都有如下流程

. 创建 `SparkSession`
. 读取源表
. 处理数据
. 创建目标表
. 保存数据到目标表中

另外, 还需要一个集合保存所有的报表特质

```scala
object DailyReportRunner {

  def main(args: Array[String]): Unit = {
    import com.itheima.dmp.utils.SparkConfigHelper._
    import com.itheima.dmp.utils.KuduHelper._

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("pmt_etl")
      .loadConfig()
      .getOrCreate()

    val processors: List[ReportProcessor] = List(
      NewRegionReportProcessor
    )

    for (processor <- processors) {
      val origin = spark.readKuduTable(processor.sourceTableName())

      if (origin.isDefined) {
        val result = processor.process(origin.get)
        result.createKuduTable(processor.targetTableName(), processor.targetTableSchema(), processor.targetTableKeys())
        result.saveToKudu(processor.targetTableName())
      }
    }
  }
}
```

修改现有代码::

可以根据现有的地域统计报表处理类, 改造为一个新的 `Processor`, 符合整体框架流程

```scala
object NewRegionReportProcessor extends ReportProcessor {

  override def process(origin: DataFrame): DataFrame = {
    import origin.sparkSession.implicits._

    origin.groupBy($"region", $"city")
      .agg(count($"*") as "count")
      .select($"region", $"city", $"count")
  }

  override def sourceTableName(): String = {
    PmtETLRunner.ODS_TABLE_NAME
  }

  override def targetTableName(): String = {
    "ANALYSIS_REGION_" + KuduHelper.formattedDate()
  }

  override def targetTableSchema(): Schema = {
    import scala.collection.JavaConverters._
    new Schema(List(
      new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build,
      new ColumnSchemaBuilder("city", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("count", Type.INT64).nullable(false).key(false).build()
    ).asJava
    )
  }

  override def targetTableKeys(): List[String] = {
    List("region", "city")
  }
}
```

总结::

. 框架抽取的好处

* 减少重复的代码
* 在编写新功能的时候, 有一个指引, 不需要所有的事情都由开发者操心

其实这也和 `Spark`, `Hadoop` 这样的计算框架很像, 这些计算框架不需要我们考虑分布式的细节, 而是自己把脏活累活都干了, 这其实也是一般框架在设计的时候的目的所在

. 框架抽取流程, 学会举一反三

* 不一定所有的框架都这么抽取, 但是对于流程性的东西, 这样做确实比较科学
* 抽取其实就是要找到两个东西: 第一, 什么是变的, 特有的东西. 第二, 什么是不变的, 共有的东西
* 根据每个不同的功能, 编写特质 `Trait`
* 根据特质, 编写流程

. 改善现有代码
+
改善后, 现有代码减少了一半

* 原有代码中, 有 `25` 行左右的有效代码
* 改善后, 有 `12` 行左右的有效代码
* 并且经过改善后整体流程更加清晰

### 4.3. 按照地域统计广告投放的分布情况

目标和步骤::

目标:
我们所使用的数据集归根结底是关于广告的数据, 所以通过这个小节, 我们可以看到广告在不同省市的分布情况

步骤:
. 需求说明
. 代码编写

需求说明::

[cols="30,~",caption=""]
.需要用到的字段
|===
| 字段 | 解释

| `requestmode`          a|
数据请求方式, 一般请求可能有三种, 如下

* `1` : 请求
* `2` : 展示
* `3` : 点击

| `processnode`          a|
流程节点, 所做的事情

* `1` : 请求量 `KPI`, 只计算请求量
* `2` : 有效请求, 是一个有效的请求, 有效的请求可能也有一部分系统管理的请求
* `3` : 广告请求, 是一个和广告有关的请求

| `adplatformproviderid` a|
广告平台商 `ID`

* `>= 100000` : `AdExchange`
* `< 100000` : `AdNetwork`

| `iseffective`          a|
是否可以正常计费

| `isbilling`            a|
是否收费

| `isbid`                a|
是否是 `RTB`

| `adorderid`            a|
广告 `ID`

| `adcreativeid`         a|
广告创意 `ID`

* `>= 200000` : `DSP`
* `<  200000` : `OSS`

| `winprice`             a| `RTB` 竞价成功价格

| `adpayment`            a| 转换后的广告消费 (保留小数点后 `6` 位)
|===

[cols="30,~",caption=""]
.需要统计的值
|===
| 统计值 | 解释

| 所有请求数量 a|

* 请求 `requestmode = 1`
* 所有类型 `processnode >= 1`

| 有效的请求和广告请求数量 a|

* 请求 `requestmode = 1`
* 有效和广告 `processnode >= 2`

| 广告请求数量 a|

* 请求 `requestmode = 1`
* 广告 `processnode = 3`

| `AdExchange` 的有效记录数量 a|

* `AdExchange` `adplatformproviderid >= 100000`
* 可以正常计价 `iseffective = 1`
* 收费 `isbilling = 1`
* `RTB` `isbid = 1`
* 是一个正常的广告 `adorderid != 0`

| 来自 `AdExchange` 的广告的竞价成功记录数量 a|

* `AdExchange` `adplatformproviderid >= 100000`
* 可以正常计价 `iseffective = 1`
* 收费 `isbilling = 1`
* 竞价成功 `iswin = 1`

| 可以正常计费的广告展示记录数量 a|

* 广告展示 `requestmode = 2`
* 可以正常计价 `iseffective = 1`

| 可以正常计费的广告点击记录数量 a|

* 广告点击 `requestmode = 3`
* 可以正常计价 `iseffective = 1`

| 来自 AdExchange 的并且由 DSP 处理的竞价成功价格 a|

* `AdExchange` `adplatformproviderid >= 100000`
* 可以正常计价 `iseffective = 1`
* 收费 `isbilling = 1`
* `RTB` `isbid = 1`
* 是一个正常的广告 `adorderid > 20000`
* DSP `adcreativeid > 200000`
* 竞价成功价格 `winprice`

| 来自 AdExchange 的并且由 DSP 处理的广告消费价格 a|

* `AdExchange` `adplatformproviderid >= 100000`
* 可以正常计价 `iseffective = 1`
* 收费 `isbilling = 1`
* `RTB` `isbid = 1`
* 是一个正常的广告 `adorderid > 20000`
* DSP `adcreativeid > 200000`
* 广告消费 `adpayment`

| 广告请求的占比 a|

* 广告请求的数量 / 有效的请求和广告请求数量

| 付费的广告占比 a|

* 付费广告点击数量 / 广告点击数量
|===

.结果集

```text
+``````+`````````+````````````-+`````````---+```````````````+`````````---+
| region | city       | orginal_req_cnt | valid_req_cnt | success_rtx_rate   | ad_click_rate |
+``````+`````````+````````````-+`````````---+```````````````+`````````---+
| 云南省 | 昆明市     | 3               | 2             | 1.333333333333333  | 1             |
| 内蒙古 | 呼和浩特市 | 4               | 1             | 1.25               | 1             |
| 北京   | 北京市     | 39              | 28            | 1.113636363636364  | 1             |
| 吉林省 | 吉林市     | 5               | 5             | 0.8333333333333334 | 1             |
| 吉林省 | 四平市     | 1               | 0             | 0.5                | 1             |
| 吉林省 | 延边       | 8               | 4             | 1.153846153846154  | 1             |
| 吉林省 | 白山市     | 0               | 0             | 2                  | 1             |
| 吉林省 | 长春市     | 17              | 12            | 1.260869565217391  | 1             |
| 四川省 | 凉山       | 2               | 1             | 1                  | 1             |
| 四川省 | 南充市     | 6               | 4             | 1                  | 1             |
| 四川省 | 广元市     | 1               | 1             | 1                  | 1             |
| 四川省 | 德阳市     | 6               | 2             | 0.5714285714285714 | 1             |
| 四川省 | 成都市     | 11              | 9             | 1.1                | 1             |
| 四川省 | 泸州市     | 7               | 6             | 2.666666666666667  | 1             |
| 四川省 | 绵阳市     | 3               | 1             | 0.5                | 1             |
+``````+`````````+````````````-+`````````---+```````````````+`````````---+
```

代码编写::

代码只需要实现 `ReportProcessor` 即可

[source,scala]
.`AdsRegionProcessor`

```scala
object AdsRegionProcessor extends ReportProcessor {
  private val ORIGIN_TEMP_TABLE: String = "adsregion"
  private val MID_TEMP_TABLE: String = "midtable"

  override def process(origin: DataFrame): DataFrame = {
    lazy val sql =                                                // <1>
      s"""
         |SELECT t.region,
         |       t.city,
         |       sum(CASE
         |               WHEN (t.requestmode = 1
         |                     AND t.processnode >= 1) THEN 1
         |               ELSE 0
         |           END) AS orginal_req_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 1
         |                     AND t.processnode >= 2) THEN 1
         |               ELSE 0
         |           END) AS valid_req_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 1
         |                     AND t.processnode = 3) THEN 1
         |               ELSE 0
         |           END) AS ad_req_cnt,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling =1
         |                     AND t.isbid = 1
         |                     AND t.adorderid != 0) THEN 1
         |               ELSE 0
         |           END) AS join_rtx_cnt,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling =1
         |                     AND t.iswin = 1) THEN 1
         |               ELSE 0
         |           END) AS success_rtx_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 2
         |                     AND t.iseffective = 1) THEN 1
         |               ELSE 0
         |           END) AS ad_show_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 3
         |                     AND t.iseffective = 1) THEN 1
         |               ELSE 0
         |           END) AS ad_click_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 2
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1) THEN 1
         |               ELSE 0
         |           END) AS media_show_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 3
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1) THEN 1
         |               ELSE 0
         |           END) AS media_click_cnt,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1
         |                     AND t.iswin = 1
         |                     AND t.adorderid > 20000
         |                     AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
         |               ELSE 0
         |           END) AS dsp_pay_money,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1
         |                     AND t.iswin = 1
         |                     AND t.adorderid > 20000
         |                     AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
         |               ELSE 0
         |           END) AS dsp_cost_money
         |FROM $ORIGIN_TEMP_TABLE t
         |GROUP BY t.region,
         |         t.city
    """.stripMargin

    lazy val sqlWithRate =                                        // <2>
      s"""
         |SELECT t.*,
         |       t.success_rtx_cnt / t.join_rtx_cnt AS success_rtx_rate,
         |       t.media_click_cnt / t.ad_click_cnt AS ad_click_rate
         |FROM $MID_TEMP_TABLE t
         |WHERE t.success_rtx_cnt != 0
         |  AND t.join_rtx_cnt != 0
         |  AND t.media_click_cnt != 0
         |  AND t.ad_click_cnt != 0
    """.stripMargin

    origin.createOrReplaceTempView(ORIGIN_TEMP_TABLE)
    val midTemp = origin.sparkSession.sql(sql)                    // <3>
    midTemp.createOrReplaceTempView(MID_TEMP_TABLE)
    origin.sparkSession.sql(sqlWithRate)                          // <4>
  }

  override def sourceTableName(): String = {
    PmtETLRunner.ODS_TABLE_NAME
  }

  override def targetTableName(): String = {
    "ANALYSIS_ADS_REGION_" + KuduHelper.formattedDate()
  }

  override def targetTableSchema(): Schema = {
    import scala.collection.JavaConverters._

    new Schema(
      List(
        new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("city", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("orginal_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("valid_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("join_rtx_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("success_rtx_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_show_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_click_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("media_show_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("media_click_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("dsp_pay_money", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("dsp_cost_money", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("success_rtx_rate", Type.DOUBLE).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_click_rate", Type.DOUBLE).nullable(true).key(false).build()
      ).asJava
    )
  }

  override def targetTableKeys(): List[String] = {
    List("region", "city")
  }
}
```

<1> 查询基础统计数据的 `SQL` 语句
<2> 查询比例的 `SQL` 语句
<3> 执行基础查询
<4> 在基础查询的基础上查询比例

总结::

* 业务不需要全部记住
* 本例使用 `SQL` 的方式查询
* 框架设置好后, 即可直接通过框架的定义进行查询

### 4.4. 按照 App 统计广告投放的分布情况

与报表2一致，分组字段为appid、appname

### 4.5. 按照设备统计广告投放的分布情况

与报表2一致，分组字段为client、device

### 4.6. 按照网络类型统计广告投放的分布情况

与报表2一致，分组字段为networkmannerid、networkmannername

### 4.7. 按照网络运营商统计广告投放的分布情况

与报表2一致，分组字段为ispid, ispname

### 4.8. 按照渠道统计广告投放的分布情况

与报表2一致，分组字段为channelid

## 5. 商圈库

目标:

* 例如说一个有实体店的电商公司在投放广告的时候, 应该会优先选择附近的用户进行投放
* 再例如说, 如果要对一个市场营销活动投放广告的话, 应该也会优先选择活动场地附近的人
* 所以对于广告业务来说, 商圈所代表的地理位置信息是非常重要的, 这个小节我们就对整个数据集获取一下商圈库的信息

步骤:
. 高德 `API` 介绍
. 通过高德获取地理位置信息
. 解析高德所返回的 `JSON`
. 生成商圈库

### 5.1. 高德 API

目标和步骤::

目标:
通过这个小节了解如何使用高德逆地理位置 `API` 来通过位置信息获取其位置名称

步骤:
. 申请高德 `API`
. `API` 介绍

申请高德 `API`::

. 注册高德开发者账号
+
. 进入 `https://lbs.amap.com/`
. 右上角点击注册, 按照流程进行注册即可

. 创建新的应用
+
. 创建应用
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706111445.png)

. 填写应用信息
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706112005.png)

. 创建新的 `Key`
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706112431.png)

. 填写 `Key` 的信息
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706112633.png)

. 获取 Key
+
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706112754.png)

使用高德 API::

. 高德提供了哪些功能

* 通过 `https://lbs.amap.com/api/` 可知, 高德提供如下的功能
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706113009.png)

* 其含义分别为

[cols="20,~"]
|===
| 功能 | 作用

| 地理/逆地理编码 a|

地理编码/逆地理编码 `API` 是通过 `HTTP/HTTPS` 协议访问远程服务的接口, 提供结构化地址与经纬度之间的相互转化的能力

结构化地址是一串字符, 内含国家、省份、城市、区县、城镇、乡村、街道、门牌号码、屋邨、大厦等建筑物名称

| 路径规划 a|

路径规划API是一套以 `HTTP` 形式提供的步行、公交、驾车查询及行驶距离计算接口

| 行政区域查询 a|

行政区域查询是一类简单的 `HTTP` 接口，根据用户输入的搜索条件可以帮助用户快速的查找特定的行政区域信息

| 搜索 `POI` a|

`POI` 全称叫做 `Point of interest`, 是地图上一些有意义的位置, 如商铺, 政府单位等

搜索服务 `API` 是一类简单的 `HTTP` 接口, 提供多种查询 `POI` 信息的能力

| `IP` 定位 a|

`IP` 定位是一个简单的 `HTTP` 接口, 根据用户输入的 `IP` 地址, 能够快速的帮用户定位 `IP` 的所在位置

|===

. 逆地理位置 `API` 的使用

* 选择 `API`

* 商圈库的含义是显式商圈的名称, 例如说国贸, TBD, 金燕龙等等
* 逆地理编码的作用是把地理位置信息转为结构化地址, 恰好符合我们的需求

* 逆地理位置的 `API` 调用方式

* 了解一个工具的用法最好的去处就是官方文档, `https://lbs.amap.com/api/webservice/guide/api/georegeo#regeo`

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706122610.png)

* 这个 `API` 的含义是使用 `Http` 调用一个 `Web` 接口

[cols="20,~"]
|===
| `URL` | `https://restapi.amap.com/v3/geocode/regeo?parameters`
| 请求方式 | `GET`
|===

[cols="20,20,~",caption=""]
.请求参数
|===
| `key` | 高德 `Key` | 用户在高德地图官网申请 `Web` 服务 `API` 类型 `Key`
| `location` | 经纬度坐标 | 经度在前, 纬度在后, 经纬度间以 `,` 分割, 经纬度小数点后不要超过 `6` 位
|===

[source,text]
.返回样例

```text
{
  "status": "1",
  "regeocode": {
    "addressComponent": {
      "city": ),
      "province": "北京市",
      "adcode": "110108",
      "district": "海淀区",
      "towncode": "110108015000",
      "streetNumber": {
        "number": "5号",
        "location": "116.310454,39.9927339",
        "direction": "东北",
        "distance": "94.5489",
        "street": "颐和园路"
      },
      "country": "中国",
      "township": "燕园街道",
      "businessAreas": [
        {
          "location": "116.303364,39.97641",
          "name": "万泉河",
          "id": "110108"
        },
        {
          "location": "116.314222,39.98249",
          "name": "中关村",
          "id": "110108"
        },
        {
          "location": "116.294214,39.99685",
          "name": "西苑",
          "id": "110108"
        }
      ],
      "building": {
        "name": "北京大学",
        "type": "科教文化服务;学校;高等院校"
      },
      "neighborhood": {
        "name": "北京大学",
        "type": "科教文化服务;学校;高等院校"
      },
      "citycode": "010"
    },
    "formatted_address": "北京市海淀区燕园街道北京大学"
  },
  "info": "OK",
  "infocode": "10000"
}
```

总结::

* 当前的功能是需要根据数据集中的经纬度信息, 找到对应的商圈信息, 高德提供了大量的地图相关的 `API`, 我们可以免费使用, 其中这个功能应该使用高德逆地理位置 `API`
* 逆地理位置的 `API` 其实就是 `Http` 的一个接口, 使用 `GET` 请求, 发送对应的参数即可调用
* 后续需要做的事情

. 通过 `Http` 客户端在程序里调用高德 `API`
. 获取数据后, 使用 JSON 解析工具将 JSON 解析为对象

### 5.2. Http 客户端

目标和步骤::

目标:
即使是在大数据系统中, `Http` 协议也是非常重要的一个协议, 通过本章, 详细的介绍 `Http` 协议, 并且介绍常见的 `Http` 常见的访问方式

步骤:
. `Http` 协议
. 常见的 `Http` 客户端

`Http` 协议::

* `TCP`

* 问题的提出, 这两个微信如何通信

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706145833.png)

* 解决方案一: 使用网线或者 `WIFI`

首要考虑的问题就是 `zhangsan` 如何知道 `lisi` 在网络中的为止

* 解决方案二: 使用 `IP` 标注主机在网络中的位置

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706151205.png)

解决的问题: 现在 `zhangsan` 的手机和 `lisi` 的手机在网络中已经有位置了, 这两个手机之间已经可以通信了
遗留的问题: 但是 `zhangsan` 的收集和 `lisi` 的手机中运行了很多程序, `zhangsan` 手机中的微信该如何找到 `lisi` 手机中的微信?

* 解决方案三: 使用 `Port` 标注程序在 电脑或手机 中的位置

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706151256.png)

解决的问题: 现在 `zhangsan` 手机上的微信, 已经可以和 `lisi` 手机上的微信交互了, 通过端口可以解决网络中的主机中的软件识别问题
遗留的问题: 网络通信中有很多问题, 需要通信双方进行协调, 比如说协议版本, 容错方式, 数据包长度, 路由信息等, 如果没有一个好的协议, 双方可以进行通信吗? 就好像, 一个只会说普通话的中国人, 和一个只会说印度语的印度人, 他们之间能交流吗?

* 解决方案四: 使用 `TCP` 协议, 规范通信双方都需要遵循的报文格式

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706151930.png)

解决的问题: 现在双方终于有了完备的, 可以相互通信的环境了, `TCP` 规定了通信双方报文的格式, 包括各种标志位, 源 `IP` 和目标 `IP`, 通信版本等
遗留的问题: `TCP` 适合所有场景吗?

跟着这个路径来解决的问题就是双方如何通信的问题, 解决方案叫做 `TCP / IP`, 同样的, 也有另外一个基础协议, 是不安全的无连接的, 叫做 `UDP / IP`

* `Http`

* 客户端服务器通信常见下的特殊需求

* 客户端访问服务器的主要目的是操作数据, 数据的操作有四种, 增删改查, `TCP` 可以表达吗?
* 服务器返回的数据一般是结构化的, 结构化的数据格式有很多种, 例如 `JSON`, `XML`, 具体使用哪种数据组织形式, `TCP` 可以描述吗?
* 服务器一般要在客户端记录一些状态信息, 叫做 `Cookie`, `TCP` 能描述如何创建和修改 `Cookie` 吗?

不能, 因为 `TCP` 只是一种基础的协议, `TCP` 只负责双方能够知道对方是谁, 以及如何通信

在使用 `TCP` 的基础之上, 对于某些特定的细分领域, 还需要在 `TCP` 之上再加上一层数组组织的协议, 对 `TCP` 协议做一层补充

`Http` 就是基于 `TCP` 协议的一种上层协议, 适合 `Web` 服务和客户端通信的场景

* `Http` 协议过程

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706154111.png)

* 一般是客户端发起一个向服务器的请求, 告知服务器要做什么, 这在 `Http` 中叫 `Reqeust`
* 服务器操作数据
* 服务器返回响应, 告知客户端操作结果, 这在 `Http` 中叫 `Response`

* `Http` 消息结构

请求和响应的基本机构都如下所示

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706173434.png)

* 常见的 `Http Method`

* `Get` 请求, 一般用于获取数据
* `Post` 请求, 一般用于添加数据
* `Put` 请求, 一般用于修改数据
* `Delete` 请求, 一般用于删除数据

* `Get` 和 `Post` 的参数传递区别

* 几乎所有的请求都可以在 `Url` 上拼接参数, 大致形式如下

```text
http[s]:// host:port /path1/path2 ? param1=x & param2=x
```

* 除了 `Get` 和 `Delete` 不建议外, 其它的请求也可以将参数放入 `Body`

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706173434.png)

* `Get`

`Get` 和 `Post` 是传统 `Web` 中常提到的两个词, `Get` 场景常被理解为访问某个网页就是 `Get`, 但是 `Get` 其实本质是获取数据, 数据可以是网页形式的, 也可以是 `Json` 形式的, 也可以是 `XML` 形式的, 一切都由请求头定义

* 在访问某个网页的时候

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706170650.png)

* 在请求高德 API 的时候

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706170904.png)

总结::

* `Http` 协议基于 `TCP` 协议, `Http` 协议的主要目的是规范客户端和服务器的通信
* `Http` 并不是只用作于访问某个网站, 获取某个页面, 提交某个表单, 事实上, `Http` 是规范了客户端和服务器端通信, 只要是客户端和服务器通信, 都适用于 `Http` 的场景, 不只是网页
* 访问高德的 `API` 就是使用 `Http` 中的 `Get` 请求方法, 去请求某个 `API`, 然后得到对应的返回数据

### 5.3. OkHttp 的使用

目标和步骤::

目标:
通过这个小节, 大家能够理解 `Http` 的本质, 并且掌握 `OkHttp` 的基础使用

步骤:
. 常见的 `Http` 客户端
. `OkHttp` 的使用
. 封装 `OkHttp` 的工具类

常见的 `Http` 客户端::

* 浏览器

* 在浏览器地址栏输入 `Url` 进行访问便是 `Get` 请求

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706174208.png)

* 但是使用浏览器想要请求 `Post` 就比较麻烦了, 需要编写 `Html`

. Post.html

```text
<form action="http://baidu.com" method="post">
Name: <br>
<input type="text" name="name" value="Please input your first name">
<br>
Age: <br>
<input type="text" name="age" value="Please input your age">
<br>
<input type="submit" value="Submit">
</form>
```

. 通过提交按钮提交

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706180141.png)

. 查看开发者工具, 发现 `Post` 请求已经发出

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706180043.png)

* `Http` 图形工具

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190706180739.png)

* `HttpClient`

* `HttpClient` 是早期 `Apache` 开源的 `Http` 客户端
* `HttpClient` 是一个 `Java` 客户端库
* `HttpClient` 调用方式比较繁琐, 性能也相对较差, 所以现阶段使用的比较少了

```scala
val REGEO_API_URL = s"https://restapi.amap.com/v3/geocode/regeo"

val client = new HttpClient()
val get = new GetMethod(REGEO_API_URL)

client.executeMethod(get)
if(get.getStatusCode ## 200) {
  val json = get.getResponseBodyAsString
  json
}
else {
  ""
}
```

* `OkHttp`

* `OkHttp` 是一个相对较新(其实也很久了)的一个 `Http` 客户端 `Java` 库
* `OkHttp` 支持 `HTTP2.0/SPDY`
* `OkHttp` 调用方式相对灵活简便, 同时 `OkHttp` 有一个上层的封装 `Retrofit`
* `OkHttp` 代码精简, 没有太多历史遗留问题, 性能较好, 现阶段使用的比较多

```scala
val url = s"https://restapi.amap.com/v3/geocode/regeo"

val httpClient = new HttpClient
val getMethod = new GetMethod(url)

val code = httpClient.executeMethod(getMethod)
```

`OkHttp` 的使用::

. 导入 Maven 依赖

```xml
<dependency>
    <groupId>com.squareup.okhttp3</groupId>
    <artifactId>okhttp</artifactId>
    <version>3.14.2</version>
</dependency>
```

. 编写 OkHttp 代码

```scala
private val client = new OkHttpClient()

val request = new Request.Builder()
  .url(url)
  .get()
  .build()

val response = client.newCall(request).execute()
response.body().string()
```

* 整体思路是 `OkHttpClient -> Request -> execute`, 非常简单易懂

封装 `OkHttp` 的工具类::

. 创建配置文件 `common.conf` 放置高德的 `Key` 和基础 `Url`

```text
amap.key="ee47d9583e049cc2dd9db6c134a6dae7"
amap.baseUrl="https://restapi.amap.com/"
```

. 创建工具类 `HttpUtils`, 读取配置文件

```scala
object HttpUtils {
  private val commonConfig = ConfigFactory.load("common")
}
```

. 创建方法访问高德 API

```scala
object HttpUtils {
  private val commonConfig = ConfigFactory.load("common")
  private val client = new OkHttpClient()

  def getAddressInfo(longitude: Double, latitude: Double): Option[String] = {
    val url = s"${commonConfig.getString("amap.baseUrl")}" +
      s"/geocode/regeo" +
      s"?key=${commonConfig.getString("amap.key")}" +
      s"&location=$longitude,$latitude"

    val request = new Request.Builder()
      .url(url)
      .get()
      .build()

    try {
      val response = client.newCall(request).execute()
      if (response.isSuccessful) {
        Some(response.body().string())
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
}
```

总结::

. 不仅是浏览器可以访问 `Http` 服务器, 有很多 `Http` 客户端工具, 例如一些图形化的工具, 以及一些 `Java` 库
. `OkHttp` 比 `HttpClient` 要新很多, 源码也更简洁有力, 效率更高, 所以采用 `OkHttp` 访问高德的 `API`
. 一般情况下, 项目中的 `Url` 和 `Key` 之类的东西, 要放在配置文件中

### 5.4. Json 解析

目标和步骤::

目标:
通过小节, 大家可以了解什么是 `JSON` 以及如何解析 `JSON`, 很多大数据项目中, 都需要涉及一些 `JSON` 解析的功能, 这个技能点一定要点

步骤:
. 什么是 `JSON`
. 什么是 `JSON` 解析
. 使用 `JSON4S`

什么是 `JSON`::

* 简介

`JSON (JavaScript Object Notation)` 是一种轻量级的数据交换格式, 易于人阅读和编写, 同时也易于机器解析和生成

```text
{
  "info": "OK",
  "infocode": "10000",
  "status": "1",
  "regeocode": {
      "country": "中国",
      "township": "燕园街道",
      "businessAreas": [
        {
          "location": "116.303364,39.97641",
          "name": "万泉河",
          "id": "110108"
        },
        {
          "location": "116.314222,39.98249",
          "name": "中关村",
          "id": "110108"
        },
        {
          "location": "116.294214,39.99685",
          "name": "西苑",
          "id": "110108"
        }
      ]
  }
}
```

* 结构和类型

. 整体上的结构有三种

* `Json` 对象

```text
{
  "name": "zhangsan",
  "age": 10
}
```

* `Json` 数组

```text
[
  {"name": "zhangsan", "age": 10},
  {"name": "lisi", "age": 15},
  {"name": "wangwu", "age": 20},
  {"name": "zhaoliu", "age": 25}
]
```

* `Json` 行文件

```text
{"name": "zhangsan", "age": 10}
{"name": "lisi", "age": 15}
{"name": "wangwu", "age": 20}
{"name": "zhaoliu", "age": 25}
```

有如下几种类型支持

* 数字型

```text
"age": 3
```

* 字符串

```text
"name": "张三"
```

* 布尔型

```text
"isSchool": true
```

* 数组

```text
"friends": ["张三", "李四", "王五"]
```

* 对象

```text
"person": {"name": "张三", "age": 10}
```

* 空

```text
"school": null
```

什么是 `JSON` 解析::

* `JSON` 本质上是字符串, 是带有 `JSON` 语法结构的字符串, 一般情况下是不会需要用到整个 `JSON` 字符串的, 需要的是 `JSON` 中某些字段和数据
* 在程序中一般使用对象来表示数据
* 将 `JSON` 转化为对象的过程就是 `JSON` 解析, 同理, 对象也可以转化为 `JSON` 字符串

[source,text,caption=""]
.`JSON`

```JSON
{                                          --> case class Gaode (
  "info": "OK",                            -->     info: String,
  "infocode": "10000",                     -->     infoCode: String,
  "status": "1",                           -->     status: String,
  "regeocode": {                           -->     regeocode: ReGeoCode)
      "country": "中国",
      "township": "燕园街道",
      "businessAreas": [
        {
          "location": "116.303364,39.97641",
          "name": "万泉河",
          "id": "110108"
        },
        {
          "location": "116.314222,39.98249",
          "name": "中关村",
          "id": "110108"
        },
        {
          "location": "116.294214,39.99685",
          "name": "西苑",
          "id": "110108"
        }
      ]
  }
}
```

[source,scala,caption=""]
.`Scala` 对象

```scala

case class Gaode(status: String, info: String, infocode: String, regeocode: ReGeoCode)

case class ReGeoCode(businessAreas: Option[List[BusinessArea]])

case class BusinessArea(location: String, name: String)
```

使用 `JSON4S`::

```scala
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

val json =
  """
    |{
    |  "info": "OK",
    |  "infocode": "10000",
    |  "status": "1",
    |  "regeocode": {
    |      "country": "中国",
    |      "township": "燕园街道",
    |      "businessAreas": [
    |        {
    |          "location": "116.303364,39.97641",
    |          "name": "万泉河",
    |          "id": "110108"
    |        },
    |        {
    |          "location": "116.314222,39.98249",
    |          "name": "中关村",
    |          "id": "110108"
    |        },
    |        {
    |          "location": "116.294214,39.99685",
    |          "name": "西苑",
    |          "id": "110108"
    |        }
    |      ]
    |  }
    |}
  """.stripMargin

implicit val formats = Serialization.formats(NoTypeHints) // <1>
val jsonObj = read[Gaode](json)                           // <2>

println(jsonObj)

case class Gaode(status: String, info: String, infocode: String, regeocode: ReGeoCode)

case class ReGeoCode(businessAreas: Option[List[BusinessArea]])

case class BusinessArea(location: String, name: String)
```

<1> 关键代码, 指定类型格式化方式, 例如说时间如何格式化等, 以隐式转换的方式注入进去
<2> 关键代码, 把 `JSON` 字符串读取为对象

* 注意点: 目标对象类型必须要匹配 `JSON` 字符串类型
* 注意点: 如果对象中某个属性可能没有, 应该使用 `Optional` 对象表示, 有的时候为实例 `Some`, 没有的时候为实例 `None`

融合进 `Http` 请求流程中::

```scala
object HttpUtils {

  ...

  def parseAddressInfo(json: String): AMapResponse = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.read

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    read[AMapResponse](json)
  }
}

case class AMapResponse(status: String, info: String, infocode: String, regeocode: ReGeoCode)

case class ReGeoCode(addressComponent: AddressComponent)

case class AddressComponent(businessAreas: Option[List[BusinessArea]])

case class BusinessArea(location: String, name: String)
```

* 刚才测试环节中的 `JSON` 数据和真实获取到的不一样, 所以

总结::

* `JSON` 是一种数据格式, 其实说白了, 就是有结构的字符串
* `JSON` 的解析本质上就是读取字符串, 按照 `JSON` 的格式, 把数据字段赋值给对象, 使用 JSON4S 的话如下

```scala
def parseAddressInfo(json: String): AMapResponse = {
  import org.json4s._
  import org.json4s.jackson.Serialization
  import org.json4s.jackson.Serialization.read

  implicit val formats: Formats = Serialization.formats(NoTypeHints)
  read[AMapResponse](json)
}
```

### 5.6. 商圈库

目标和步骤::

目标:

步骤:

总结::

nihao

hello
