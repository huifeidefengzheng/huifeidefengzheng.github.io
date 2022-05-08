---
title: 为什么spark处理速度比mapreduce快很多
date: 2019/9/15 08:16:25
updated: 2019/9/15 21:52:30
comments: true
tags:
     Spark
categories: 
     - 项目
     - DMP-6
---

### 1、为什么spark处理速度比mapreduce快很多？

* 1、基于内存

```text
mapreduce的任务后期在运行的时候任务的结果数据都会落地磁盘，后面有其他的job需要依赖于前面job的输出结果数据，这个时候只能够进行大量的磁盘io操作获取得到。这里性能就比较低。

举例：hivesql： select name,age from (select * from user where age > 30 and age <40)
              ---------- job2------   ----------------------  job1 -----------------

              先需要进行任务的划分，然后进行资源的申请-------- 这一块有可能浪费大量的时间

              map 0%  reduce 0%
                 map 20%  reduce 0%
                    map 100%  reduce 0%
                       map 100%  reduce 10%
                          map 100%  reduce 100%
      在一些大数据计算框架中可能都有这种通病：
任务的调度时间远远大于任务实际的计算的时间。
      select count (*) from user;

spark任务后期再运行的时候任务的结果数据可以保存在内存，后面有其他的job需要依赖于前面job的输出结果数据，这里就可以直接从内存中获取得到，大大减少磁盘io操作，性能比较高。

spark比较合适于迭代计算。
job1------> job2------>job3------>job4------>job5------>job6------>....
```

* 2、进程与线程

```text
mapreduce任务以进程的方式运行在yarn集群中，比如说程序有100个MapTask要运行,后期每一个task在运行的时候都需要一个进程，这里一共就需要100个进程。需要开启100个进程。

spark任务以线程的方式运行在worker节点的executor进程中，比如说程序有100个MapTask要运行，后期每一个task在运行的时候都需要一个线程，这里可以这样极端一点: 可以只开启1个进程，在这1个进程中运行100个线程就可以了。开启一个进程与开启一个线程的代价是不一样。 开启一个进程比开启一个线程需要的时间和调度资源大大增加。
```

### 2、spark中的Driver端是什么

* 1、涉及到spark任务划分
* 2、spark任务的资源调度流程
