---
title: 01_Zeppelin
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - DMP
---

Zeppelin

.导读
`Zeppelin` 入门和安装
. `Zeppelin` 的基本使用
. 使用 `Zeppelin` 编写代码并可视化

## 入门

.导读
`Zeppelin` 是什么, 有什么使用场景
. 安装 `Zeppelin`
. 界面介绍

### `Zeppelin` 是什么

.导读
传统方式的痛点
. `Zeppelin` 提供的功能
. `Zeppelin` 的特点

痛点::

如果想对一个工具或者框架想要有深入的了解, 就要先了解这个工具或者框架所解决的问题, 面临的挑战, `Zeppelin` 所要解决的其实也就是如下几个问题

数据分析是一个探索性的过程:
没有人能一蹴而就的完成分析和挖掘任务

往往数据挖掘和数据分析是需要多次尝试和调试, 不断的迭代和探索数据的过程
数据探索需要一些工具支持

因为要不断的探索数据, 所以要求工具必须要支持多种组件, 多种语言, 要支持可调试
`Spark shell` 是一个比较简单的交互式环境

虽然 `Spark shell` 能够及时的看到结果, 但是其功能还是太过简单, 只能在 `Shell` 中使用, 并且也没太好的方式记录整个调试过程
`Zeppelin` 的一个重点, 是让交互式探索变得容易

所以 `Zeppelin` 要解决的一个比较重要的问题就是让交互式探索变得更容易

如何表达对数据的理解:
如何表达数据分析的结论?

在一次数据分析结束后, 总是要有一些结论的, 常见的形态如下:

. 数据分析, 得出报表数据, 存入 HBase 或者 KUDU 之类的数据库中
. 前端读取 Kudu 或者 HBase 中的数据, 展示结果

这种形态的表达比较重, 适合对外提供, 如果希望在内部进行结论的共享, 该如何做呢?
`Spark shell` 只能作为调试工具

`Spark shell` 是一个交互式工具, 但是如果想把整个调试过程, 包括对数据的解读都记录下来, 使用 `Spark shell` 还是力有不逮
`Zeppelin` 是一款笔记工具

`Zeppelin` 是一款笔记工具, 意味着 Zeppelin 的基本操作单位是笔记, 在一个笔记中可以同时做到如下三件事

* 使用 `Markdown` 编写文字性内容
* 编写 Python, R, Scala 等程序, 操作 Spark, Flink 等工具
* 展示执行结果, 提供丰富的可视化支持

这三板斧配合起来, 足以轻松并且愉快的表达数据结论

擅长 SQL 和擅长 Scala 的同事如何配合?:
有人做算法, 有人写代码, 有人做研究

在一个数据项目中, 是分为很多岗位的, 我们主要通过代码来完成需求和分析数据, 但是有些岗位是数学家, 或者算法工程师, 他们和我们所掌握的技能是不同的

有人只写 `SQL`, 有人只写 `Scala`, 有人只写公式, 这些人如何配合起来呢? 仅通过 `Spark shell` 来配合显然不够
`Zeppelin` 是一个 `Web` 程序

`Zeppelin` 是一个 `Web` 程序, 以 `Java` 编写, 使用 `Shiro` 做权限控制, 所以 `Zeppelin` 支持如下功能

* 多人同时访问
* 针对笔记进行权限限制

所以 `Zeppelin` 可以成为一个团队的配合工具

`Zeppelin` 能做什么::

总结一下, `Zeppelin` 是什么?

*`Zeppelin` 是一个多语言混合的 `REPL` 交互式笔记工具*

[cols="20,~"]
|===

^.^| 多用途笔记工具 a| ![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605120455.png)
^.^| 多语言后端 a| ![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605120815.png)
^.^| 重点支持 `Spark` a| ![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605120916.png)
^.^| 数据可视化 a|
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605153340.png)
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605153427.png)
^.^| 支持表单设计 a| ![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605153533.png)
^.^| 可以分享和协作 a| ![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605153623.png)

### 安装

.导读
下载
. 安装
. 配置

下载::

. `Zeppelin` 的官方网站是: `http://zeppelin.apache.org/`
. 下载安装包

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605154151.png)
![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605154308.png)
. 上传安装包

在 `DMP` 的课程目录中, 已经提供了相应的下载包, 直接上传即可

配置::

配置在哪?:
配置分为两个部分, 一个是 `XML` 的配置文件, 一个是环境变量配置, 还有一个用于配置权限的 `Shiro` 配置文件

* `zeppelin/conf/zeppelin-site.xml`

这个配置文件中配置的是 `Zeppelin` 的参数, `zeppelin-env` 中的配置大部分这里也有, 但是这个文件中没有关于 `Zeppelin server` 和 `Zeppelin Interpreter` 的内存和运行参数

* `zeppelin/conf/zeppelin-env.sh`

这个配置文件中配置和 `zeppelin-site` 中的内容类似, 大部分也都支持, 但是多了一部分关于内存的配置, 少了一些关于权限的配置

* `zeppelin/conf/shiro.ini`

`Zeppelin` 使用 `Shiro` 作为权限系统, 这个配置是 `Shiro` 的配置文件

这两个配置文件使用之前, 都必须要先复制一份不带 `template` 结尾的文件

```text
cd /export/servers/zeppelin/conf
cp zeppelin-env.sh.template zeppelin-env.sh
cp zeppelin-site.xml.template zeppelin-site.xml
```

要修改的具体配置:

* 端口

`Zeppelin` 因为使用 `Java` 开发, 所以默认占用 `8080` 作为其 `Http` 端口, 这样会影响其它的 `Java web` 程序, 所以一般会修改

端口可以在 `zeppelin-site.xml` 中配置

```xml
<property>
  <name>zeppelin.server.port</name>
  <value>8090</value>
  <description>Server port.</description>
</property>
```

* `Spark Home`

如果不配置 `Spark` 的 `Home` 目录, 则会自动使用 `Zeppelin` 的内置 `Spark`, 所以需要指定一下 `Spark` 的 `Home`

在 `zeppelin-env.sh` 中添加

```text
export SPARK_HOME=/export/servers/spark/
```

* 内存

因为我们在本地测试, 可能虚拟机中没有足够的内存, 所以可以主动减少 `Zeppelin server` 服务和 `Zeppelin Interpreter` 的内存占用

内存只能在 `zeppelin-env.sh` 中配置

```text
export ZEPPELIN_MEM="-Xms512m -Xmx512m -XX:MaxPermSize=256m"
```

* 权限配置

. 在 `zeppelin-site.xml` 中禁用匿名用户登录

```xml
<property>
  <name>zeppelin.anonymous.allowed</name>
  <value>false</value>
  <description>Anonymous user allowed by default</description>
</property>
```

. 在 `shiro.ini` 中设置账户密码

```text
admin = admin, admin
```

## 基本使用

.导读
登录
. 界面介绍
. `Note` 使用

访问和登录::

通过 `node01:8090` 即可访问 `Zeppelin`, 进入后会看到如下界面要求我们登录

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605175212.png)

使用 `admin:admin` 登录即可

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605175327.png)

主界面::

登录后会看到主界面如下

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605175625.png)

使用如下界面可以创建新的笔记或者查看示例笔记

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605175949.png)

在主页面中也有相关的笔记操作, 而且在这个位置可以导入外部的笔记

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605180232.png)

配置 `Spark` 解释器::

`Zeppelin` 只是一个笔记工具, 使用 `Zeppelin` 来编写 `Spark` 代码的时候, 依然使用的 `Spark` 的运行环境, `Spark` 的运行环境会在一个叫做 `Spark 解释器` 的进程中运行, 也可以连接 `Yarn` 来运行 `Spark` 程序

首先在主界面右上角选中解释器配置

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605182930.png)

找到 `Spark` 相关的配置, 进行配置

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605183437.png)

将上述标注的参数改为如下样子

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605190129.png)

`Note` 界面::

创建一个新的 `Note`

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605205026.png)

进入 Note 界面后如下

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605205923.png)

笔记是由一个一个的段组成的, 每一个段可以不同类型的, 有的段被当作代码运行, 有的段是 `MarkDown` 文本

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605210113.png)

一个段由三部分组成

* 代码段
* 结果段
* 命令

在代码部分编写代码, 可以是 `Spark`, `Python`, `SQL`, `MarkDown` 等, 支持什么语言是根据有什么 `Interpreter` 解释器来决定的, 如果安装了 `Spark` 解释器, 才可以编写 `Spark` 的代码

代码段编写过代码以后, 可以通过 `Shift + Enter` 来运行这段代码, 结果会显式在结果段中, `Spark` 的代码运行会显式其运行结果, `MarkDown` 的代码运行会显式其被转为 `HTML` 的样式

可以通过在第一行以 `%` 号开头, 指定此段代码的类型

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605231529.png)

[NOTE]
在 Zeppelin 0.8 中, 简版默认没有安装 MarkDown 解释器, 而这又是一个常见的解释器, 所以需要安装一下, 使用如下命令即可安装

```text
./bin/install-interpreter.sh --name "md" --artifact org.apache.zeppelin:zeppelin-markdown:0.8.0
```

安装以后重启 Zeppelin, 添加 MarkDown 解释器, 即可在代码段使用

![image](https://doc-1256053707.cos.ap-beijing.myqcloud.com/20190605230511.png)
