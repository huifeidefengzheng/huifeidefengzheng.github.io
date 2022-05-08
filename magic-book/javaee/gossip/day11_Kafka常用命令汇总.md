Kafka常用API命令汇总:

0、启动kafka集群

```shell
kafka-server-start.sh -d config/server.properties
```

1、列出集群里的所有主题。

$ kafka-topics.sh --zookeeper node01:2181,node02:2181,node03:2181 --list

2、 创建一个叫作my-topic的主题,主题包含8分区,每个分区拥有两个副本。
kafka-topics.sh --zookeeper node01:2181,node02:2181,node03:2181 --create --topic source_topic --replication-factor 3 --partitions 2

3、列出集群里所有主题的详细信息。
kafka-topics.sh --zookeeper node01:2181,node02:2181,node03:2181 --describe

4、列出集群里特定主题的详细信息。
kafka-topics.sh --zookeeper node01:2181,node02:2181,node03:2181 --describe  --topic my-topic

5、删除一个叫作my-topic的主题。
kafka-topics.sh --zookeeper node01:2181,node02:2181,node03:2181 --delete  --topic my-topic

需要设置 delete.topic.enable 为true

6、列出旧版本的所有消费者群组。
kafka-consumer-groups.sh --zookeeper node01:2181,node02:2181,node03:2181 --list

7、列出新版本的所有消费者群组。
kafka-consumer-groups.sh --new-consumer --bootstrap-server node01:9092,node02:9092,node03:9092 --list

8、获取旧版本消费者群组testgroup的详细信息。
kafka-consumer-groups.sh --zookeeper node01:2181 --describe --group testgroup

9、获取新版本消费者群组console-consumer-87024的详细信息。
kafka-consumer-groups.sh --new-consumer --bootstrap-server node01:9092,node02:9092,node03:9092 --describe --group console-consumer-87024

10、查看某一个topic对应的消息数量。
kafka-run-class.sh  kafka.tools.GetOffsetShell --broker-list node01:9092 --topic B2CDATA_COLLECTION3 --time -1

11、查看log日志片段的内容,显示查看日志的内容。
kafka-run-class.sh kafka.tools.DumpLogSegments --files 00000000000000000000.log --print-data-log

12、控制台生产者:向主题 my-topic 生成两个消息。
kafka-console-producer.sh --broker-list node01:9092 --topic my-topic

13、控制台消费者:从主题 my-topic 获取消息。
kafka-console-consumer.sh --zookeeper node01:2181,node02:2181,node03:2181 --topic my-topic --from-beginning



**server.properties 配置项**

| 名称                                  | 说明                                                         | 默认值                  | 有效值                              | 重要性   |
| :------------------------------------ | :----------------------------------------------------------- | :---------------------- | :---------------------------------- | :------- |
| bootstrap.servers                     | kafka集群的broker-list，如：hadoop01:9092,hadoop02:9092      | 无                      |                                     | 必选     |
| acks                                  | 确保生产者可靠性设置，有三个选项：acks=0:不等待成功返回acks=1:等Leader写成功返回acks=all:等Leader和所有ISR中的Follower写成功返回,all也可以用-1代替 | -1                      | 0,1,-1,all                          |          |
| key.serializer                        | key的序列化器                                                |                         | ByteArraySerializerStringSerializer | 必选     |
| value.serializer                      | value的序列化器                                              |                         | ByteArraySerializerStringSerializer | 必选     |
| buffer.memory                         | Producer总体内存大小                                         | 33554432                | 不要超过物理内存，根据实际情况调整  | 建议必选 |
| compression.type                      | 压缩类型压缩最好用于批量处理，批量处理消息越多，压缩性能越好 | 无                      | none、gzip、snappy                  |          |
| retries                               | 发送失败尝试重发次数                                         | 0                       |                                     |          |
| batch.size                            | 每个partition的未发送消息大小                                | 16384                   | 根据实际情况调整                    | 建议必选 |
| client.id                             | 附着在每个请求的后面，用于标识请求是从什么地方发送过来的     |                         |                                     |          |
| connections.max.idle.ms               | 连接空闲时间超过过久自动关闭（单位毫秒）                     | 540000                  |                                     |          |
| linger.ms                             | 数据在缓冲区中保留的时长,0表示立即发送为了减少网络耗时，需要设置这个值太大可能容易导致缓冲区满，阻塞消费者太小容易频繁请求服务端 | 0                       |                                     |          |
| max.block.ms                          | 最大阻塞时长                                                 | 60000                   |                                     |          |
| max.request.size                      | 请求的最大字节数，该值要比batch.size大不建议去更改这个值，如果设置不好会导致程序不报错，但消息又没有发送成功 | 1048576                 |                                     |          |
| partitioner.class                     | 分区类，可以自定义分区类，实现partitioner接口                | 默认是哈希值%partitions |                                     |          |
| receive.buffer.bytes                  | socket的接收缓存空间大小,当阅读数据时使用                    | 32768                   |                                     |          |
| request.timeout.ms                    | 等待请求响应的最大时间,超时则重发请求,超过重试次数将抛异常   | 3000                    |                                     |          |
| send.buffer.bytes                     | 发送数据时的缓存空间大小                                     | 131072                  |                                     |          |
| timeout.ms                            | 控制server等待来自followers的确认的最大时间                  | 30000                   |                                     |          |
| max.in.flight.requests.per.connection | kafka可以在一个connection中发送多个请求，叫作一个flight,这样可以减少开销，但是如果产生错误，可能会造成数据的发送顺序改变。 | 5                       |                                     |          |
| metadata.fetch.timeout.ms             | 从ZK中获取元数据超时时间比如topic\host\partitions            | 60000                   |                                     |          |
| metadata.max.age.ms                   | 即使没有任何partition leader 改变，强制更新metadata的时间间隔 | 300000                  |                                     |          |
| metric.reporters                      | 类的列表，用于衡量指标。实现MetricReporter接口，将允许增加一些类，这些类在新的衡量指标产生时就会改变。JmxReporter总会包含用于注册JMX统计 | none                    |                                     |          |
| metrics.num.samples                   | 用于维护metrics的样本数                                      | 2                       |                                     |          |
| metrics.sample.window.ms              | metrics系统维护可配置的样本数量，在一个可修正的window size。这项配置配置了窗口大小，例如。我们可能在30s的期间维护两个样本。当一个窗口推出后，我们会擦除并重写最老的窗口 | 30000                   |                                     |          |
| reconnect.backoff.ms                  | 连接失败时，当我们重新连接时的等待时间。这避免了客户端反复重连 | 10                      |                                     |          |
| retry.backoff.ms                      | 在试图重试失败的produce请求之前的等待时间。避免陷入发送-失败的死循环中 | 100                     |                                     |          |
|                                       |                                                              |                         |                                     |          |

