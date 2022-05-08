---
title: 04_scala.md
date: 2019/8/28 08:16:25
updated: 2019/8/28 21:52:30
comments: true
tags:
     scala
categories: 
     - 项目
     - scala
---

Summary

课程目标

- 理解高阶函数的概念（作为值的函数、匿名函数、闭包、柯里化）
- 掌握隐式转换和隐式参数
- 掌握Akka并发编程框架

## 高阶函数

scala 混合了面向对象和函数式的特性，在函数式编程语言中，函数是“头等公民”，它和Int、String、Class等其他类型处于同等的地位，可以像其他类型的变量一样被传递和操作。
高阶函数包含
作为值的函数
匿名函数
闭包
柯里化等等

### 作为值的函数

在scala中，函数就像和数字、字符串一样，可以将函数传递给一个方法。我们可以对算法进行封装，然后将具体的动作传递给方法，这种特性很有用。
我们之前学习过List的map方法，它就可以接收一个函数，完成List的转换。

### 示例

示例说明
将一个整数列表中的每个元素转换为对应个数的小星星

```html
List(1, 2, 3...) => *, **, ***
```

步骤

1. 创建一个函数，用于将数字装换为指定个数的小星星
2. 创建一个列表，调用map方法
3. 打印转换为的列表

参考代码

```scala
package com.xhchen.highlevel

object _01FuncDemo {
  def main(args: Array[String]): Unit = {
    // 1. 创建函数，将数字转换为小星星
    val func: Int => String = (num:Int) => "*" * num

    // 2. 创建列表，执行转换
    val starList = (1 to 10).map(func)

    // 3. 打印测试
    println(starList)
  }
}
```

运行结果：

```text
Vector(*, **, ***, ****, *****, ******, *******, ********, *********, **********)
```

### 匿名函数

### 定义

上面的代码，给`(num:Int) => "*" * num`函数赋值给了一个变量，但是这种写法有一些啰嗦。在scala中，可以不需要给函数赋值给变量，没有赋值给变量的函数就是匿名函数

```scala
package com.xhchen.highlevel

object _02FuncDemo {
  def main(args: Array[String]): Unit = {
    // 使用匿名函数简化代码编写
    // 字符串*方法，表示生成指定数量的字符串
    val startList = (1 to 10).map(x => "*" * x)
    println(startList)

    // 使用下划线来简化代码编写
    val starList2 = (1 to 10).map("*" * _)
    println(starList2)
  }
}
```

示例
使用匿名函数优化上述代码
参考代码

```scala
   val startList = (1 to 10).map(x => "*" * x)
// 因为此处x变量只使用了一次，而且只是进行简单的计算，所以可以省略参数列表，使用_替代参数
println((1 to 10).map("*" * _))
```

### 柯里化

在scala和spark的源代码中，大量使用到了柯里化。为了后续方便阅读源代码，我们需要来了解下柯里化。
定义
柯里化（Currying）是指将原先接受多个参数的方法转换为多个只有一个参数的参数列表的过程。
![1552811606951](/04_scala/1552811606951.png)
柯里化过程解析
![1552811639044](04_scala/1552811639044.png)
示例
示例说明
编写一个方法，用来完成两个Int类型数字的计算
具体如何计算封装到函数中
使用柯里化来实现上述操作

参考代码

```scala
package com.xhchen.highlevel

object _03FuncDemo {
  // 1. 定义一个方法（柯里化），计算两个Int类型的值
  def calculate(a:Int, b:Int)(calc:(Int, Int)=>Int) = {
    calc(a, b)
  }

  // 2. 调用柯里化方法
  def main(args: Array[String]): Unit = {
    println(calculate(10, 10){
        (x,y) => x + y
    })
    println(calculate(1, 2)(_ + _))
    println(calculate(1, 2)(_ * _))
    println(calculate(1, 2)(_ - _))
  }
}
```

运行结果

```text
20
3
2
-1
```

### 闭包

闭包其实就是一个函数，只不过这个函数的返回值依赖于声明在函数外部的变量。
可以简单认为，就是可以访问不在当前作用域范围的一个函数。

### 示例一

定义一个闭包

```scala
package com.xhchen.highlevel

object _04FuncDemo {
  def main(args: Array[String]): Unit = {
    val y = 10

    // 定义一个函数，访问函数作用域外部的变量
    val add: Int => Int = (x:Int) => x + y

    println(add(1))
  }
}

```

add函数就是一个闭包

运行结果

```text
11
```

### 示例二

柯里化就是一个闭包

```scala
  def add(x:Int)(y:Int) = {
    x + y
  }
```

上述代码相当于

```scala
  def add(x:Int) = {
    (y:Int) => x + y
  }
```

### 隐式转换和隐式参数

隐式转换和隐式参数是scala非常有特色的功能，也是Java等其他编程语言没有的功能。我们可以很方便地利用隐式转换来丰富现有类的功能。后面在编写Akka并发编程、Spark SQL、Flink都会看到隐式转换和隐式参数的身影。
定义
所谓隐式转换，是指以implicit关键字声明的带有单个参数的方法。它是自动被调用的，自动将某种类型转换为另外一种类型。
使用步骤

1. 在object中定义隐式转换方法（使用implicit）
2. 在需要用到隐式转换的地方，引入隐式转换（使用import）
3. 自动调用隐式转化后的方法

示例
示例说明
使用隐式转换，让File具备有read功能——实现将文本中的内容以字符串形式读取出来
步骤

1. 创建RichFile类，提供一个read方法，用于将文件内容读取为字符串
2. 定义一个隐式转换方法，将File隐式转换为RichFile对象
3. 创建一个File，导入隐式转换，调用File的read方法

参考代码

```scala
package com.xhchen.highlevel

import java.io.File

import scala.io.Source

object _05ImplicitDemo {
  // 1. 创建扩展类，实现对File的扩展，读取文件内容
  class RichFile(val file:File) {
    def read() = {
      // 将文件内容读取为字符串
      Source.fromFile(file).mkString
    }
  }

  // 2. 创建隐式转换
  object ImplicitDemo {
    // 将File对象转换为RichFile对象
    implicit def fileToRichFile(file:File) = new RichFile(file)
  }

  // 3. 导入隐式转换，测试读取文件内容
  def main(args: Array[String]): Unit = {
       //加载文件
    val file = new File("./data/1.txt")
    //导入隐式转换
    import ImplicitDemo.fileToRichFile

    // 调用隐式转换的方法
    println(file.read())
  }
}
```

### 隐式转换的时机

当对象调用类中不存在的方法或者成员时，编译器会自动将对象进行隐式转换
当方法中的参数的类型与目标类型不一致时

### 自动导入隐式转换方法

前面，我们手动使用了import来导入隐式转换。是否可以不手动import呢？
在scala中，如果在当前作用域中有隐式转换方法，会自动导入隐式转换。
示例：将隐式转换方法定义在main所在的object中

```scala
package com.xhchen.highlevel

import java.io.File

import scala.io.Source

object _06ImplicitDemo {
  // 1. 创建扩展类，实现对File的扩展，读取文件内容
  class RichFile(val file:File) {
    def read() = {
      // 将文件内容读取为字符串
      Source.fromFile(file).mkString
    }
  }


  // 3. 导入隐式转换，测试读取文件内容
  def main(args: Array[String]): Unit = {
    // 2. 创建隐式转换
    // 将File对象转换为RichFile对象
    implicit def fileToRichFile(file:File) = new RichFile(file)

    val file = new File("./data/1.txt")

    // 调用隐式转换的方法
      // 调用的其实是RichFile的read方法
    println(file.read())
  }
}

```

### 隐式参数

方法可以带有一个标记为implicit的参数列表。这种情况，编译器会查找缺省值，提供给该方法。
定义

1. 在方法后面添加一个参数列表，参数使用implicit修饰
2. 在object中定义implicit修饰的隐式值
3. 调用方法，可以不传入implicit修饰的参数列表，编译器会自动查找缺省值

1.和隐式转换一样，可以使用import手动导入隐式参数
2.如果在当前作用域定义了隐式值，会自动进行导入
示例
示例说明
定义一个方法，可将传入的值，使用一个分隔符前缀、后缀包括起来
使用隐式参数定义分隔符
调用该方法，并打印测试

参考代码

```scala
// 使用implicit定义一个参数
def quote(what:String)(implicit delimiter:(String, String)) = {
    delimiter._1 + what + delimiter._2
}

// 隐式参数
object ImplicitParam {
    implicit val DEFAULT_DELIMITERS = ("<<<", ">>>")
}

def main(args: Array[String]): Unit = {
  // 导入隐式参数
    import ImplicitParam.DEFAULT_DELIMITERS

    println(quote("李雷和韩梅梅"))
}
```

## Akka并发编程框架简介

### Akka介绍

Akka是一个用于构建高并发、分布式和可扩展的基于事件驱动的应用的工具包。Akka是使用scala开发的库，同时可以使用scala和Java语言来开发基于Akka的应用程序。

### Akka特性

- 提供基于异步非阻塞、高性能的事件驱动编程模型
- 内置容错机制，允许Actor在出错时进行恢复或者重置操作
- 超级轻量级的事件处理（每GB堆内存几百万Actor）
- 使用Akka可以在单机上构建高并发程序，也可以在网络中构建分布式程序。

### Akka通信过程

以下图片说明了Akka Actor的并发编程模型的基本流程：

1. 学生创建一个ActorSystem
2. 通过ActorSystem来创建一个ActorRef（老师的引用），并将消息发送给ActorRef
3. ActorRef将消息发送给Message Dispatcher（消息分发器）
4. Message Dispatcher将消息按照顺序保存到目标Actor的MailBox中
5. Message Dispatcher将MailBox放到一个线程中
6. MailBox按照顺序取出消息，最终将它递给TeacherActor接受的方法中

![1552871108166](04_scala/1552871108166.png)

### Akka-入门案例

案例说明
基于Akka创建两个Actor，Actor之间可以互相发送消息。
![1552879431645](04_scala/1552879431645.png)

### 实现步骤

1. 创建Maven模块
2. 创建并加载Actor
3. 发送/接收消息

### 1. 创建Maven模块

使用Akka需要导入Akka库，我们这里使用Maven来管理项目

1. 创建Maven模块
2. 打开pom.xml文件，导入akka Maven依赖和插件

### 2. 创建并加载Actor

创建两个Actor

- SenderActor：用来发送消息
- ReceiveActor：用来接收，回复消息

创建Actor

1. 创建ActorSystem
2. 创建自定义Actor
3. ActorSystem加载Actor

### 3. 发送/接收消息

- 使用样例类封装消息
- SubmitTaskMessage——提交任务消息
- SuccessSubmitTaskMessage——任务提交成功消息
- 使用类似于之前学习的Actor方式，使用`!`发送异步消息

参考代码

```scala
case class SubmitTaskMessage(msg:String)
case class SuccessSubmitTaskMessage(msg:String)

// 注意：要导入的是Akka下的Actor
object SenderActor extends Actor {

  override def preStart(): Unit = println("执行SenderActor的preStart()方法")

  override def receive: Receive = {
    case "start" =>
      val receiveActor = this.context.actorSelection("/user/receiverActor")
      receiveActor ! SubmitTaskMessage("请完成#001任务!")
    case SuccessSubmitTaskMessage(msg) =>
      println(s"接收到来自${sender.path}的消息: $msg")
  }
}

object ReceiverActor extends Actor {

  override def preStart(): Unit = println("执行ReceiverActor()方法")

  override def receive: Receive = {
    case SubmitTaskMessage(msg) =>
      println(s"接收到来自${sender.path}的消息: $msg")
      sender ! SuccessSubmitTaskMessage("完成提交")
    case _ => println("未匹配的消息类型")
  }
}

object SimpleAkkaDemo {
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("SimpleAkkaDemo", ConfigFactory.load())

    val senderActor: ActorRef = actorSystem.actorOf(Props(SenderActor), "senderActor")
    val receiverActor: ActorRef = actorSystem.actorOf(Props(ReceiverActor), "receiverActor")

    senderActor ! "start"

  }
}
```

程序输出：

```text
接收到来自akka://SimpleAkkaDemo/user/senderActor的消息: 请完成#001任务!
接收到来自akka://SimpleAkkaDemo/user/receiverActor的消息: 完成提交
```

### Akka定时任务

如果我们想要使用Akka框架定时的执行一些任务，该如何处理呢？

### 使用方式

Akka中，提供一个scheduler对象来实现定时调度功能。使用ActorSystem.scheduler.schedule方法，可以启动一个定时任务。

schedule方法针对scala提供两种使用形式：

第一种：发送消息

```scala
def schedule(
    initialDelay: FiniteDuration,    // 延迟多久后启动定时任务
    interval: FiniteDuration,      // 每隔多久执行一次
    receiver: ActorRef,          // 给哪个Actor发送消息
    message: Any)            // 要发送的消息
(implicit executor: ExecutionContext)  // 隐式参数：需要手动导入
```

第二种：自定义实现

```scala
def schedule(
    initialDelay: FiniteDuration,      // 延迟多久后启动定时任务
    interval: FiniteDuration        // 每隔多久执行一次
)(f: ⇒ Unit)                // 定期要执行的函数，可以将逻辑写在这里
(implicit executor: ExecutionContext)    // 隐式参数：需要手动导入
```

示例一
示例说明
定义一个Actor，每1秒发送一个消息给Actor，Actor收到后打印消息
使用发送消息方式实现
参考代码

```scala
 // 1. 创建一个Actor，用来接收消息，打印消息
  object ReceiveActor extends Actor {
    override def receive: Receive = {
      case x => println(x)
    }
  }

  // 2. 构建ActorSystem，加载Actor
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("actorSystem", ConfigFactory.load())
    val receiveActor = actorSystem.actorOf(Props(ReceiveActor))

    // 3. 启动scheduler，定期发送消息给Actor
    // 导入一个隐式转换
    import scala.concurrent.duration._
    // 导入隐式参数
    import actorSystem.dispatcher

    actorSystem.scheduler.schedule(0 seconds,
      1 seconds,
      receiveActor, "hello")
  }
```

示例二
示例说明
定义一个Actor，每1秒发送一个消息给Actor，Actor收到后打印消息
使用自定义方式实现

参考代码

```scala
object SechdulerActor extends Actor {
  override def receive: Receive = {
    case "timer" => println("收到消息...")
  }
}

object AkkaSchedulerDemo {
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("SimpleAkkaDemo", ConfigFactory.load())

    val senderActor: ActorRef = actorSystem.actorOf(Props(SechdulerActor), "sechdulerActor")

    import actorSystem.dispatcher
    import scala.concurrent.duration._

    actorSystem.scheduler.schedule(0 seconds, 1 seconds) {
      senderActor ! "timer"
    }
  }
}
```

1. 需要导入隐式转换`import scala.concurrent.duration._`才能调用0 seconds方法
2. 需要导入隐式参数`import actorSystem.dispatcher`才能启动定时任务

## Akka实现两个进程之间的通信

## 案例介绍

基于Akka实现在两个进程间发送、接收消息。Worker启动后去连接Master，并发送消息，Master接收到消息后，再回复Worker消息。
![1552886264753](04_scala/1552886264753.png)

### 1. Worker实现

步骤

1. 创建一个Maven模块，导入依赖和配置文件
2. 创建启动WorkerActor
3. 发送"setup"消息给WorkerActor，WorkerActor接收打印消息
4. 启动测试

参考代码

Worker.scala

```scala
val workerActorSystem = ActorSystem("actorSystem", ConfigFactory.load())
val workerActor: ActorRef = workerActorSystem.actorOf(Props(WorkerActor), "WorkerActor")

// 发送消息给WorkerActor
workerActor ! "setup"
```

WorkerActor.scala

```scala
object WorkerActor extends Actor{
  override def receive: Receive = {
    case "setup" =>
      println("WorkerActor:启动Worker")
  }
}
```

### 2. Master实现

步骤

1. 创建Maven模块，导入依赖和配置文件
2. 创建启动MasterActor
3. WorkerActor发送"connect"消息给MasterActor
4. MasterActor回复"success"消息给WorkerActor
5. WorkerActor接收并打印接收到的消息
6. 启动Master、Worker测试

参考代码

Master.scala

```scala
val masterActorSystem = ActorSystem("MasterActorSystem", ConfigFactory.load())
val masterActor: ActorRef = masterActorSystem.actorOf(Props(MasterActor), "MasterActor")
```

MasterActor.scala

```scala
object MasterActor extends Actor{
  override def receive: Receive = {
    case "connect" =>
      println("2. Worker连接到Master")
      sender ! "success"
  }
}
```

WorkerActor.scala

```scala
object WorkerActor extends Actor{
  override def receive: Receive = {
    case "setup" =>
      println("1. 启动Worker...")
      val masterActor = context.actorSelection("akka.tcp://MasterActorSystem@127.0.0.1:9999/user/MasterActor")

      // 发送connect
      masterActor ! "connect"
    case "success" =>
      println("3. 连接Master成功...")
  }
}
```

### 简易版spark通信框架案例

案例介绍
模拟Spark的Master与Worker通信
一个Master
  管理Worker
若干个Worker（Worker可以按需添加）
  注册
  发送心跳
![1552890302701](04_scala/1552890302701.png)

### 实现思路

1. 构建Master、Worker阶段
  构建Master ActorSystem、Actor
  构建Worker ActorSystem、Actor

2. Worker注册阶段
  Worker进程向Master注册（将自己的ID、CPU核数、内存大小(M)发送给Master）

3. Worker定时发送心跳阶段

  Worker定期向Master发送心跳消息
4. Master定时心跳检测阶段
  Master定期检查Worker心跳，将一些超时的Worker移除，并对Worker按照内存进行倒序排序
5. 多个Worker测试阶段
  启动多个Worker，查看是否能够注册成功，并停止某个Worker查看是否能够正确移除

### 1. 工程搭建

项目使用Maven搭建工程
步骤

1. 分别搭建几下几个项目

| 工程名            | 说明                   |
| ----------------- | ---------------------- |
| spark-demo-common | 存放公共的消息、实体类 |
| spark-demo-master | Akka Master节点        |
| spark-demo-worker | Akka Worker节点        |

2.导入依赖(资料包中的pom.xml)
  master/worker添加common依赖
3.导入配置文件(资料包中的application.conf)
  修改Master的端口为7000
  修改Worker的端口为7100

### 2. 构建Master和Worker

分别构建Master和Worker，并启动测试
步骤

1. 创建并加载Master Actor
2. 创建并加载Worker Actor
3. 测试是否能够启动成功

参考代码

Master.scala

```scala
val sparkMasterActorSystem = ActorSystem("sparkMaster", ConfigFactory.load())
val masterActor = sparkMasterActorSystem.actorOf(Props(MasterActor), "masterActor")
```

MasterActor.scala

```scala
object MasterActor extends Actor{
  override def receive: Receive = {
    case x => println(x)
  }
}
```

Worker.scala

```scala
val sparkWorkerActorSystem = ActorSystem("sparkWorker", ConfigFactory.load())
sparkWorkerActorSystem.actorOf(Props(WorkerActor), "workerActor")
```

WorkerActor.scala

```scala
object WorkerActor extends Actor{
  override def receive: Receive = {
    case x => println(x)
  }
}
```

### 3. Worker注册阶段实现

在Worker启动时，发送注册消息给Master
步骤

1. Worker向Master发送注册消息（workerid、cpu核数、内存大小）
  随机生成CPU核（1、2、3、4、6、8）
  随机生成内存大小（512、1024、2048、4096）（单位M）
2. Master保存Worker信息，并给Worker回复注册成功消息
3. 启动测试

参考代码

MasterActor.scala

```scala
object MasterActor extends Actor{

  private val regWorkerMap = collection.mutable.Map[String, WorkerInfo]()

  override def receive: Receive = {
    case WorkerRegisterMessage(workerId, cpu, mem) => {
      println(s"1. 注册新的Worker - ${workerId}/${cpu}核/${mem/1024.0}G")
      regWorkerMap += workerId -> WorkerInfo(workerId, cpu, mem, new Date().getTime)
      sender ! RegisterSuccessMessage
    }
  }
}
```

WorkerInfo.scala

```scala
/**
  * 工作节点信息
  * @param workerId workerid
  * @param cpu CPU核数
  * @param mem 内存多少
  * @param lastHeartBeatTime 最后心跳更新时间
  */
case class WorkerInfo(workerId:String, cpu:Int, mem:Int, lastHeartBeatTime:Long)
```

MessagePackage.scala

```scala
/**
  * 注册消息
  * @param workerId
  * @param cpu CPU核数
  * @param mem 内存大小
  */
case class WorkerRegisterMessage(workerId:String, cpu:Int, mem:Int)

/**
  * 注册成功消息
  */
case object RegisterSuccessMessage
```

WorkerActor.scala

```scala
object WorkerActor extends Actor{

  private var masterActor:ActorSelection = _
  private val CPU_LIST = List(1, 2, 4, 6, 8)
  private val MEM_LIST = List(512, 1024, 2048, 4096)

  override def preStart(): Unit = {
    masterActor = context.system.actorSelection("akka.tcp://sparkMaster@127.0.0.1:7000/user/masterActor")

    val random = new Random()
    val workerId = UUID.randomUUID().toString.hashCode.toString
    val cpu = CPU_LIST(random.nextInt(CPU_LIST.length))
    val mem = MEM_LIST(random.nextInt(MEM_LIST.length))

    masterActor ! WorkerRegisterMessage(workerId, cpu, mem)
  }

  ...
}
```

### 4. Worker定时发送心跳阶段

Worker接收到Master返回注册成功后，发送心跳消息。而Master收到Worker发送的心跳消息后，需要更新对应Worker的最后心跳时间。

步骤

1. 编写工具类读取心跳发送时间间隔
2. 创建心跳消息
3. Worker接收到注册成功后，定时发送心跳消息
4. Master收到心跳消息，更新Worker最后心跳时间
5. 启动测试

参考代码

ConfigUtil.scala

```scala
object ConfigUtil {
  private val config: Config = ConfigFactory.load()

  val `worker.heartbeat.interval` = config.getInt("worker.heartbeat.interval")
}

```

MessagePackage.scala

```scala
package com.xhchen.spark.common

...

/**
  * Worker心跳消息
  * @param workerId
  * @param cpu CPU核数
  * @param mem 内存大小
  */
case class WorkerHeartBeatMessage(workerId:String, cpu:Int, mem:Int)
```

WorkerActor.scala

```scala
object WorkerActor extends Actor{
  ...

  override def receive: Receive = {
    case RegisterSuccessMessage => {
      println("2. 成功注册到Master")

      import scala.concurrent.duration._
      import context.dispatcher

      context.system.scheduler.schedule(0 seconds,
        ConfigUtil.`worker.heartbeat.interval` seconds){
        // 发送心跳消息
        masterActor ! WorkerHeartBeatMessage(workerId, cpu, mem)
      }
    }
  }
}
```

MasterActor.scala

```scala
object MasterActor extends Actor{
  ...

  override def receive: Receive = {
  ...
    case WorkerHeartBeatMessage(workerId, cpu, mem) => {
      println("3. 接收到心跳消息, 更新最后心跳时间")
      regWorkerMap += workerId -> WorkerInfo(workerId, cpu, mem, new Date().getTime)
    }
  }
}
```

### 5. Master定时心跳检测阶段

如果某个worker超过一段时间没有发送心跳，Master需要将该worker从当前的Worker集合中移除。可以通过Akka的定时任务，来实现心跳超时检查。
步骤

1. 编写工具类，读取检查心跳间隔时间间隔、超时时间
2. 定时检查心跳，过滤出来大于超时时间的Worker
3. 移除超时的Worker
4. 对现有Worker按照内存进行降序排序，打印可用Worker

参考代码

ConfigUtil.scala

```scala
object ConfigUtil {
  private val config: Config = ConfigFactory.load()

  // 心跳检查时间间隔
  val `master.heartbeat.check.interval` = config.getInt("master.heartbeat.check.interval")
  // 心跳超时时间
  val `master.heartbeat.check.timeout` = config.getInt("master.heartbeat.check.timeout")
}
```

MasterActor.scala

```scala
  override def preStart(): Unit = {
    import scala.concurrent.duration._
    import context.dispatcher

    context.system.scheduler.schedule(0 seconds,
      ConfigUtil.`master.heartbeat.check.interval` seconds) {
      // 过滤出来超时的worker
      val timeoutWorkerList = regWorkerMap.filter {
        kv =>
          if (new Date().getTime - kv._2.lastHeartBeatTime > ConfigUtil.`master.heartbeat.check.timeout` * 1000) {
            true
          }
          else {
            false
          }
      }

      if (!timeoutWorkerList.isEmpty) {
        regWorkerMap --= timeoutWorkerList.map(_._1)
        println("移除超时的worker:")
        timeoutWorkerList.map(_._2).foreach {
          println(_)
        }
      }

      if (!regWorkerMap.isEmpty) {
        val sortedWorkerList = regWorkerMap.map(_._2).toList.sortBy(_.mem).reverse
        println("可用的Worker列表:")
        sortedWorkerList.foreach {
          var rank = 1
          workerInfo =>
            println(s"<${rank}> ${workerInfo.workerId}/${workerInfo.mem}/${workerInfo.cpu}")
            rank = rank + 1
        }
      }
    }
  }
  ...
}
```

### 6. 多个Worker测试阶段

修改配置文件，启动多个worker进行测试。
步骤

1. 测试启动新的Worker是否能够注册成功
2. 停止Worker，测试是否能够从现有列表删除

## 总示例

### 示例-

#### highlevel-高阶函数

##### _01FuncDemo.scala

src/com/xhchen/highlevel/_01FuncDemo.scala

```scala
package com.xhchen.highlevel

object _01FuncDemo {
  def main(args: Array[String]): Unit = {
    // 1. 创建函数，将数字转换为小星星
    val func: Int => String = (num:Int) => "*" * num

    // 2. 创建列表，执行转换
    val starList = (1 to 10).map(func)

    // 3. 打印测试
    println(starList)
  }
}
```

##### _02FuncDemo.scala

src/com/xhchen/highlevel/_02FuncDemo.scala

```scala
package com.xhchen.highlevel

object _02FuncDemo {
  def main(args: Array[String]): Unit = {
    // 使用匿名函数简化代码编写
    val startList = (1 to 10).map(x => "*" * x)
    println(startList)

    // 使用下划线来简化代码编写
    val starList2 = (1 to 10).map("*" * _)
    println(starList2)
  }
}
```

##### _03FuncDemo.scala

src/com/xhchen/highlevel/_03FuncDemo.scala

```scala
package com.xhchen.highlevel

object _03FuncDemo {
  // 1. 定义一个方法（柯里化），计算两个Int类型的值
  def calculate(a:Int, b:Int)(calc:(Int, Int)=>Int) = {
    calc(a, b)
  }

  // 2. 调用柯里化方法
  def main(args: Array[String]): Unit = {
    println(calculate(1, 2)(_ + _))
    println(calculate(1, 2)(_ * _))
    println(calculate(1, 2)(_ - _))
  }
}
```

##### _04FuncDemo.scala

src/com/xhchen/highlevel/_04FuncDemo.scala

```scala
package com.xhchen.highlevel

object _04FuncDemo {
  def main(args: Array[String]): Unit = {
    val y = 10

    // 定义一个函数，访问函数作用域外部的变量
    val add: Int => Int = (x:Int) => x + y

    println(add(1))
  }
}
```

##### _05ImplicitDemo.scala-隐式转换

src/com/xhchen/highlevel/_05ImplicitDemo.scala

```scala
package com.xhchen.highlevel

import java.io.File

import scala.io.Source

object _05ImplicitDemo {
  // 1. 创建扩展类，实现对File的扩展，读取文件内容
  class RichFile(val file:File) {
    def read() = {
      // 将文件内容读取为字符串
      Source.fromFile(file).mkString
    }
  }

  // 2. 创建隐式转换
  object ImplicitDemo {
    // 将File对象转换为RichFile对象
    implicit def fileToRichFile(file:File) = new RichFile(file)
  }

  // 3. 导入隐式转换，测试读取文件内容
  def main(args: Array[String]): Unit = {
    val file = new File("./data/1.txt")

    import ImplicitDemo.fileToRichFile

    // 调用隐式转换的方法
    println(file.read())
  }
}
```

##### _06ImplicitDemo.scala-自动导入隐式转换

src/com/xhchen/highlevel/_06ImplicitDemo.scala

```scala
package com.xhchen.highlevel

import java.io.File

import scala.io.Source

object _06ImplicitDemo {
  // 1. 创建扩展类，实现对File的扩展，读取文件内容
  class RichFile(val file:File) {
    def read() = {
      // 将文件内容读取为字符串
      Source.fromFile(file).mkString
    }
  }


  // 3. 导入隐式转换，测试读取文件内容
  def main(args: Array[String]): Unit = {
    // 2. 创建隐式转换
    // 将File对象转换为RichFile对象
    implicit def fileToRichFile(file:File) = new RichFile(file)

    val file = new File("./data/1.txt")

    // 调用隐式转换的方法
    println(file.read())
  }
}
```

##### _07ImplicitDemo.scala-隐式参数

src/com/xhchen/highlevel/_07ImplicitDemo.scala

```scala
package com.xhchen.highlevel

object _07ImplicitDemo {
  // 1. 定义一个方法，这个方法有一个隐式参数
  def quote(what:String)(implicit delimeters:(String, String)) = {
    delimeters._1 + what + delimeters._2
  }

  // 2. 定义一个隐式参数
  object ImplicitDemo {
    implicit val delimeterParam = ("<<", ">>")
  }

  // 3. 调用方法执行测试
  def main(args: Array[String]): Unit = {
    import ImplicitDemo._

    println(quote("你好"))
  }
}
```

#### akka-demo-入门案例

##### pom.xml

akka-demo/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.xhchen</groupId>
    <artifactId>akka-demo</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <scala.compat.version>2.11</scala.compat.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-actor_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-remote_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
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
                <version>2.4.3</version>
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
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
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

##### Entrance.scala

akka-demo/src/main/scala/com/xhchen/akka/demo/Entrance.scala

```scala
package com.xhchen.akka.demo

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Entrance {
  def main(args: Array[String]): Unit = {
    // 1. 实现一个Actor trait
    // 2. 创建ActorSystem
    val actorSystem = ActorSystem("actorSystem", ConfigFactory.load())

    // 3. 加载Actor
    val senderActor = actorSystem.actorOf(Props(SenderActor), "senderActor")
    val receiverActor = actorSystem.actorOf(Props(ReceiverActor), "receiverActor")

    // 在main方法中，发送一个start字符串消息给SenderActor
    senderActor ! "start"
  }
}
```

##### MessageDefinition.scala

akka-demo/src/main/scala/com/xhchen/akka/demo/MessageDefinition.scala

```scala
package com.xhchen.akka.demo

// 提交任务消息
case class SubmitTaskMessage(message:String)

// 提交任务成功消息
case class SuccessSubmitTaskMessage(message:String)
```

##### ReceiverActor.scala

akka-demo/src/main/scala/com/xhchen/akka/demo/ReceiverActor.scala

```scala
package com.xhchen.akka.demo

import akka.actor.Actor

object ReceiverActor extends Actor{
  override def receive: Receive = {
    case SubmitTaskMessage(message) => {
      println(s"ReceiverActor:接收到任务提交消息 ${message}")
      // 回复一个任务提交成功消息给SenderActor
      sender ! SuccessSubmitTaskMessage("成功提交任务")
    }
  }
}
```

##### SenderActor.scala

akka-demo/src/main/scala/com/xhchen/akka/demo/SenderActor.scala

```scala
package com.xhchen.akka.demo

import akka.actor.Actor

// 实现AkkaActor
object SenderActor extends Actor{
  // 在Actor并发编程模型，需要实现act，想要持续接收消息
  // loop + react
  // 但在akka编程模型中，直接在receive方法中编写偏函数直接处理消息就可以了
  override def receive: Receive = {
    case "start" => {
      println("SenderActor：接收到start消息")

      // 发送一个SubmitTaskMessage消息给ReceiverActor
      // akka://actorSystem的名字/user/actor的名字
      val receiverActor = context.actorSelection("akka://actorSystem/user/receiverActor")
      // 发送消息
      receiverActor ! SubmitTaskMessage("提交任务")
    }
    case SuccessSubmitTaskMessage(message) =>
      println(s"SenderActor：接收到任务提交成功消息 ${message}")
  }
}
```

#### scheduler-定时任务

##### _01SchedulerDemo.scala

akka-demo/src/main/scala/com/xhchen/akka/scheduler/_01SchedulerDemo.scala

```scala
package com.xhchen.akka.scheduler

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object _01SchedulerDemo {
  // 1. 创建一个Actor，接收打印消息
  object ReceiveActor extends Actor {
    override def receive: Receive = {
      case x => println(x)
    }
  }

  // 2. 构建ActorSystem，加载Actor
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("actorSystem", ConfigFactory.load())
    val receiveActor = actorSystem.actorOf(Props(ReceiveActor), "receiveActor")

    // 导入隐式转换
    import scala.concurrent.duration._
    // 导入隐式参数
    import actorSystem.dispatcher

    // 3. 定时发送消息给Actor
    // 3.1 延迟多久启动定时任务
    // 3.2 定时任务的周期
    // 3.3 指定发送消息给哪个Actor
    // 3.4 发送的消息是什么
    actorSystem.scheduler.schedule(0 seconds,
      1 seconds,
      receiveActor,
      "hello"
    )
  }

}
```

##### _02SchedulerDemo.scala

akka-demo/src/main/scala/com/xhchen/akka/scheduler/_02SchedulerDemo.scala

```scala
package com.xhchen.akka.scheduler

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object _02SchedulerDemo {
  // 1. 创建Actor，接收打印消息
  object ReceiveActor extends Actor {
    override def receive: Receive = {
      case x => println(x)
    }
  }

  // 2. 构建ActorSystem，加载Actor
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem("actorSystem", ConfigFactory.load())
    val receiveActor = actorSystem.actorOf(Props(ReceiveActor), "receiveActor")

    // 导入隐式转换
    import scala.concurrent.duration._
    // 导入隐式参数
    import actorSystem.dispatcher

    // 3. 定时发送消息（自定义方式）
    actorSystem.scheduler.schedule(0 seconds, 1 seconds) {
      // 业务逻辑
      receiveActor ! "hello"
    }
  }

}
```

## 两个进程通信示例

### akka-master

#### resources

##### application.conf

akka-master/src/main/resources/application.conf

```conf
akka.actor.provider = "akka.remote.RemoteActorRefProvider"
akka.remote.netty.tcp.hostname = "127.0.0.1"
akka.remote.netty.tcp.port = "8888"
```

#### akka

##### Entrance.scala

akka-master/src/main/scala/com/xhchen/akka/Entrance.scala

```scala
package com.xhchen.akka

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Entrance {
  def main(args: Array[String]): Unit = {
    // 1. 构建ActorSystem
    val actorSystem = ActorSystem("actorSystem", ConfigFactory.load())

    // 2. 加载Actor
    val masterActor = actorSystem.actorOf(Props(MasterActor), "masterActor")
  }
}
```

##### MasterActor.scala

akka-master/src/main/scala/com/xhchen/akka/MasterActor.scala

```scala
package com.xhchen.akka

import akka.actor.Actor

object MasterActor extends Actor{
  override def receive: Receive = {
    case "connect" => {
      println("MasterActor：接收到connect消息")
      // 获取发送者Actor的引用
      sender ! "success"
    }
  }
}
```

##### pom.xml

akka-master/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.xhchen</groupId>
    <artifactId>akka-master</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <scala.compat.version>2.11</scala.compat.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-actor_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-remote_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
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
                <version>2.4.3</version>
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
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
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

### akka-worker

#### resources

##### application.conf

akka-worker/src/main/resources/application.conf

```conf
akka.actor.provider = "akka.remote.RemoteActorRefProvider"
akka.remote.netty.tcp.hostname = "127.0.0.1"
akka.remote.netty.tcp.port = "9999"
```

#### akka

##### Entrance.scala

akka-worker/src/main/scala/com/xhchen/akka/Entrance.scala

```scala
package com.xhchen.akka

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Entrance {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个ActorSystem
    val actorSystem = ActorSystem("actorSystem", ConfigFactory.load())

    // 2. 加载Actor
    val workerActor = actorSystem.actorOf(Props(WorkerActor), "workerActor")

    // 3. 发送消息给Actor
    workerActor ! "setup"
  }
}
```

##### WorkerActor.scala

akka-worker/src/main/scala/com/xhchen/akka/WorkerActor.scala

```scala
package com.xhchen.akka

import akka.actor.Actor

object WorkerActor extends Actor{
  override def receive: Receive = {
    case "setup" => {
      println("WorkerActor：接收到消息setup")
      // 发送消息给Master
      // 1. 获取到MasterActor的引用
      // Master的引用路径：akka.tcp://actorSystem@127.0.0.1:8888/user/masterActor
      val masterActor = context.actorSelection("akka.tcp://actorSystem@127.0.0.1:8888/user/masterActor")

      // 2. 再发送消息给MasterActor
      masterActor ! "connect"
    }
    case "success" => {
      println("WorkerActor：收到success消息")
    }
  }
}
```

##### pom.xml

akka-worker/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.xhchen</groupId>
    <artifactId>akka-worker</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <scala.compat.version>2.11</scala.compat.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-actor_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-remote_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
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
                <version>2.4.3</version>
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
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
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

#### data

##### 1.txt

data/1.txt

```txt
hadoop spark storm flink
```

## Spark 简易通信框架

### spark-demo-common

##### Entities.scala

spark-demo-common/src/main/scala/com/xhchen/spark/common/Entities.scala

```scala
package com.xhchen.spark.common

// Worker基本信息
case class WorkerInfo(workerid:String,
                      cpu:Int,
                      mem:Int,
                      lastHeartBeatTime:Long)
```

##### MessagePackage.scala

spark-demo-common/src/main/scala/com/xhchen/spark/common/MessagePackage.scala

```scala
package com.xhchen.spark.common

// 封装Worker注册消息
// 1. wokerid
// 2. cpu核数
// 3. mem内存大小(M)
case class WorkerRegisterMessage(workerid:String,
                                 cpu:Int,
                                 mem:Int)

// 注册成功消息
case object RegisterSuccessMessage

// 心跳消息
case class WorkerHeartBeatMessage(workerid:String,
                                  cpu:Int,
                                  mem:Int)
```

##### pom.xml

spark-demo-common/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.xhchen</groupId>
    <artifactId>spark-demo-common</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <scala.compat.version>2.11</scala.compat.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
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
                <version>2.4.3</version>
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
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
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

#### spark-demo-master

##### resources

###### application.conf

spark-demo-master/src/main/resources/application.conf

```conf
akka.actor.provider = "akka.remote.RemoteActorRefProvider"
akka.remote.netty.tcp.hostname = "127.0.0.1"
akka.remote.netty.tcp.port = "7000"

# 配置检查Worker心跳的时间周期（秒）
master.check.heartbeat.interval = 6
# 配置Worker心跳超时的时间（秒）
master.check.heartbeat.timeout = 15
```

##### scala/spark/master

###### ConfigUtil.scala

spark-demo-master/src/main/scala/com/xhchen/spark/master/ConfigUtil.scala

```scala
package com.xhchen.spark.master

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtil {
  private val config: Config = ConfigFactory.load()

  // 配置检查Worker心跳的时间周期（秒）
  val `master.check.heartbeat.interval` = config.getInt("master.check.heartbeat.interval")
  // 配置Worker心跳超时的时间（秒）
  val `master.check.heartbeat.timeout` = config.getInt("master.check.heartbeat.timeout")
}
```

###### Master.scala

spark-demo-master/src/main/scala/com/xhchen/spark/master/Master.scala

```scala
package com.xhchen.spark.master

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Master {
  def main(args: Array[String]): Unit = {
    // 1. 构建ActorSystem
    val masterActorSystem = ActorSystem("masterActorSystem", ConfigFactory.load())

    // 2. 加载Actor
    val masterActor = masterActorSystem.actorOf(Props(MasterActor), "masterActor")

    // 3. 启动测试
  }
}
```

###### MasterActor.scala

spark-demo-master/src/main/scala/com/xhchen/spark/master/MasterActor.scala

```scala
package com.xhchen.spark.master

import java.util.Date

import akka.actor.Actor
import com.xhchen.spark.common.{RegisterSuccessMessage, WorkerHeartBeatMessage, WorkerInfo, WorkerRegisterMessage}

object MasterActor extends Actor{

  private val regWorkerMap = collection.mutable.Map[String,WorkerInfo]()

  override def preStart(): Unit = {
    // 导入时间单位隐式转换
    import scala.concurrent.duration._
    // 导入隐式参数
    import context.dispatcher

    // 1. 启动定时任务
    context.system.scheduler.schedule(0 seconds,
      ConfigUtil.`master.check.heartbeat.interval` seconds){
      // 2. 过滤大于超时时间的Worker
      val timeoutWorkerMap = regWorkerMap.filter {
        keyval =>
          // 获取最后一次心跳更新时间
          val lastHeartBeatTime = keyval._2.lastHeartBeatTime
          // 当前系统时间 - 最后一次心跳更新时间 > 超时时间（配置文件） * 1000，返回true，否则返回false
          if (new Date().getTime - lastHeartBeatTime > ConfigUtil.`master.check.heartbeat.timeout` * 1000) {
            true
          }
          else {
            false
          }
      }

      // 3. 移除超时Worker
      if(!timeoutWorkerMap.isEmpty) {
        regWorkerMap --= timeoutWorkerMap.map(_._1)

        // 4. 对Worker按照内存进行降序排序，打印Worker
        val workerList = regWorkerMap.map(_._2).toList
        val sortedWorkerList = workerList.sortBy(_.mem).reverse
        println("按照内存降序排序后的Worker列表：")
        println(sortedWorkerList)
      }
    }
  }

  override def receive: Receive = {
    case WorkerRegisterMessage(workerid, cpu, mem) => {
      println(s"MasterActor：接收到Worker注册消息${workerid}/${cpu}/${mem}")

      // 1. 保存worker信息（WorkerInfo）
      regWorkerMap += workerid -> WorkerInfo(workerid, cpu, mem, new Date().getTime)

      // 2. 回复一个注册成功消息
      sender ! RegisterSuccessMessage
    }
    case WorkerHeartBeatMessage(workerid, cpu, mem) => {
      println(s"MasterActor：接收到${workerid}心跳消息")

      regWorkerMap += workerid -> WorkerInfo(workerid, cpu, mem, new Date().getTime)
      println(regWorkerMap)
    }
  }
}
```

##### pom.xml

spark-demo-master/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.xhchen</groupId>
    <artifactId>spark-demo-master</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <scala.compat.version>2.11</scala.compat.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-actor_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-remote_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

        <dependency>
            <groupId>com.xhchen</groupId>
            <artifactId>spark-demo-common</artifactId>
            <version>1.0-SNAPSHOT</version>
        </dependency>

    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
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
                <version>2.4.3</version>
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
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
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

#### spark-demo-worker

##### resources

###### application.conf

spark-demo-worker/src/main/resources/application.conf

```conf
akka.actor.provider = "akka.remote.RemoteActorRefProvider"
akka.remote.netty.tcp.hostname = "127.0.0.1"
akka.remote.netty.tcp.port = "7103"

# 配置worker发送心跳的周期(s)
worker.heartbeat.interval = 5
```



##### spark/worker

###### ConfigUtil.scala

spark-demo-worker/src/main/scala/com/xhchen/spark/worker/ConfigUtil.scala

```scala
package com.xhchen.spark.worker

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtil {
  private val config: Config = ConfigFactory.load()

  val `worker.heartbeat.interval` = config.getInt("worker.heartbeat.interval")
}
```

###### Worker.scala

spark-demo-worker/src/main/scala/com/xhchen/spark/worker/Worker.scala

```scala
package com.xhchen.spark.worker

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Worker {
  def main(args: Array[String]): Unit = {
    val workerActorSystem = ActorSystem("workerActorSystem", ConfigFactory.load())
    val workerActor = workerActorSystem.actorOf(Props(WorkerActor), "workerActor")
  }
}
```

###### WorkerActor.scala

spark-demo-worker/src/main/scala/com/xhchen/spark/worker/WorkerActor.scala

```scala
package com.xhchen.spark.worker

import java.util.UUID

import akka.actor.{Actor, ActorSelection}
import com.xhchen.spark.common.{RegisterSuccessMessage, WorkerHeartBeatMessage, WorkerRegisterMessage}

import scala.util.Random

object WorkerActor extends Actor{

  private var masterActorRef:ActorSelection = _
  private var workerid:String = _
  private var cpu:Int = _
  private var mem:Int = _
  private val CPU_LIST = List(1,2,3,4,6,8)
  private val MEM_LIST = List(512,1024,2048,4096)

  // 在Actor启动之前就会执行的一些代码
  // 放在preStart中
  override def preStart(): Unit = {
    // 1. 获取到MasterActor的引用
    val masterActorPath = "akka.tcp://masterActorSystem@127.0.0.1:7000/user/masterActor"
    masterActorRef = context.actorSelection(masterActorPath)

    // 2. 构建注册消息
    workerid = UUID.randomUUID().toString
    val r = new Random()
    cpu = CPU_LIST(r.nextInt(CPU_LIST.length))
    mem = MEM_LIST(r.nextInt(MEM_LIST.length))
    val registerMessage = WorkerRegisterMessage(workerid, cpu, mem)

    // 3. 发送消息给MasterActor
    masterActorRef ! registerMessage
  }

  override def receive: Receive = {
    case RegisterSuccessMessage => {
      println("WorkerActor：接收到注册成功消息")

      // 导入时间单位隐式转换
      import scala.concurrent.duration._
      // 导入隐式参数
      import context.dispatcher

      // 定时发送心跳消息给Master
      context.system.scheduler.schedule(0 seconds,
        ConfigUtil.`worker.heartbeat.interval` seconds) {
        masterActorRef ! WorkerHeartBeatMessage(workerid, cpu, mem)
      }
    }
  }
}
```



##### pom.xml

spark-demo-worker/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.xhchen</groupId>
    <artifactId>spark-demo-worker</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.11.8</scala.version>
        <scala.compat.version>2.11</scala.compat.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-actor_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

        <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-remote_2.11</artifactId>
            <version>2.3.14</version>
        </dependency>

        <dependency>
            <groupId>com.xhchen</groupId>
            <artifactId>spark-demo-common</artifactId>
            <version>1.0-SNAPSHOT</version>
        </dependency>

    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <testSourceDirectory>src/test/scala</testSourceDirectory>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
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
                <version>2.4.3</version>
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
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
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

