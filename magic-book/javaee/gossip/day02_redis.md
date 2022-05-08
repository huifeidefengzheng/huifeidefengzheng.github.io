# 娱乐头条 -- day02 redis



## 1. Redis的概念

![1562653058666](./day02_redis/1562653058666.png)

​	Redis是一款由C语言编写, 基于内存的持久化的数据库, 其中数据是以KEY-VALUE的形式存储的, Redis提供了丰富的数据类型

## 2. Redis的特点

* 1) Redis将数据存储到**内存**当中, 所以Redis的读写效率非常高:  读 11w/s  写 8w/s
* 2) redis提供了丰富的数据类型:  string , hash , list ,set , sortedSet
  * 注意: redis中数据类型, 主要是描述的key-value中value的数据类型, 而key只有string
* 3) Redis数据的移植非常快的

* 4) redis中所有的操作都是原子性的, 保证数据的完整性的

## 3. redis数据类型的使用场景和特点

* string:   可以使用json转换对象, 存储
  * 特点:  和 java中 string是类似的, 表示的就是字符串
  * 使用场景: 做缓存

* hash:  存储对象是比较方便的
  * 特点: 和 java中 hashMap是类似的
  * 使用场景:  做缓存 (hash使用较少)
* list:
  * 特点:  和 java中 linkedList是类似, 可以看做是队列(FIFO)先进先出
  * 使用场景:  任务队列

* set :
  * 特点:  和 java中set集合是类似的  去重 无序
  * 使用场景: 去重业务
* sortedSet
  * 特点:  和 java中 sortedSet(TreeSet)类型    有序   去重
  * 使用场景:  排序操作(排行榜)

## 4. redis安装

### 4.1 安装目录的准备:

```shell
安装目录: /export/servers
软件存放的目录:  /export/software
日志文件的目录:  /export/logs
数据存放的目录: /export/data

创建以上目录:  
mkdir -p /export/servers
mkdir -p /export/software
mkdir -p /export/logs
mkdir -p /export/data
```

### 4.2 下载redis安装包  (142)

```
cd /export/software/
wget http://download.redis.io/releases/redis-4.0.2.tar.gz
tar -zxvf redis-4.0.2.tar.gz  -C ../servers
cd /export/servers/
mv redis-4.0.2 redis-src
```

### 4.3 安装编译环境

- 由于下载下来的只是redis的源码包, 需要对其进行编译执行, 故需要安装C语言环境

```shell
yum -y install gcc gcc-c++ libstdc++-devel tcl -y

# 安装报错请参考：https://www.yht7.com/news/114295
```

### 4.4 编译并进行安装redis

```shell
cd /export/servers/redis-src/
make MALLOC=libc
make PREFIX=/export/servers/redis install
```

### 4.5 准备redis的启动的相关配置文件

- 在指定的位置创建一个redis的配置文件

```shell
mkdir -p /export/servers/redis/conf
cd /export/servers/redis/conf
vi redis_6379.conf
```

- 配置文件的内容如下
  - 注意: 
    - 1) 内容中的bind 后面的ip地址需要配置成自己虚拟机的ip地址, 将以下的内容复制好以后, :wq退出即可
    - 2) 在进行复制之前先输入 i  进行编辑模式

```properties
bind 127.0.01
protected-mode yes
port 6379
tcp-backlog 511
timeout 0
tcp-keepalive 300
daemonize yes
supervised no
pidfile /export/data/redis/6379/redis_6379.pid
loglevel notice
logfile "/export/data/redis/6379/log.log"
databases 16
always-show-logo yes
save 900 1
save 300 10
save 60 10000
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
dir /export/data/redis/6379/
slave-serve-stale-data yes
slave-read-only yes
repl-diskless-sync no
repl-diskless-sync-delay 5
repl-disable-tcp-nodelay no
slave-priority 100
lazyfree-lazy-eviction no
lazyfree-lazy-expire no
lazyfree-lazy-server-del no
slave-lazy-flush no
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
aof-load-truncated yes
aof-use-rdb-preamble no
lua-time-limit 5000
slowlog-log-slower-than 10000
slowlog-max-len 128
latency-monitor-threshold 0
notify-keyspace-events ""
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-size -2
list-compress-depth 0
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
hll-sparse-max-bytes 3000
activerehashing yes
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit slave 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60
hz 10
aof-rewrite-incremental-fsync yes
```

### 4.6 启动redis的服务

```shell
mkdir -p /export/data/redis/6379/
cd /export/servers/redis/bin/
./redis-server ../conf/redis_6379.conf             # 启动命令
```

- 查看是否正常启动redis的服务

```
查看是否正确启动的命令
ps -ef|grep redis
```

- 启动redis的命令(此操作已在上面执行):
  - 注意: 执行此命令的时候, 必须先进入redis的bin目录下: /export/servers/redis/bin/

```shell
./redis-server ../conf/redis_6379.conf 
```

redis和mysql一样也是分为服务端和客户端, 刚刚我们已经启动了redis的服务端, 接下来就可以使用客户端连接redis了

- redis的客户端的连接
  - 使用: -h 跟着redis的服务器的ip地址

```shell
cd /export/servers/redis/bin/
./redis-cli -h 192.168.72.142
以下命令为检测命令:
输入:  ping   返回 pong  表示成功

客户端退出:
	第一种:强制退出 Ctrl+c
	第二种: quit
```

## 

## 5. redis的客户端工具: jedis

​	jedis是一款java连接redis客户端工具包, jedis提供了一套非常省力的API, jedis中最大的特点就是其API和redis中命令都是一样的, 大大的降低了学习成本, 只需要学习其中的一个就可以了

如果想要使用jedis, 首先先进行导包操作

```xml
		<dependency>
            <groupId>redis.clients</groupId>
            <artifactId>jedis</artifactId>
            <version>2.9.0</version>
        </dependency>
```

* jedis的入门程序

```java
@Test
    public void jedisTest01(){
        //1. 创建 jedis对象: 指定redis的ip地址 和  端口号
        // Jedis 可以将看做是 connection连接对象
        Jedis jedis = new Jedis("192.168.72.142",6379);

        //2. 执行相关的操作
        String pong = jedis.ping();
        System.out.println(pong);

        //3. 释放资源
        jedis.close();

    }
```

![1547697439234](./day02_redis/1547697439234.png)

```
出现的原因主要有以下几个:
	1) 防火墙没有关闭, 或者没有开放6379端口号
	2) redis没有开启
	3) 代码中ip地址或者端口号写错了
```

### 5.1 jedis操作redis -- string

```java
//2. 使用jedis操作Redis --->  string

    @Test
    public void jedisTest02() throws InterruptedException {
        //1. 创建 jedis对象
        Jedis jedis = new Jedis("192.168.72.142",6379);

        //2. 执行相关操作
        //2.1 进行赋值操作: name :  隔壁老王
        jedis.set("name","隔壁老王");

        //2.2 取值操作
        String name = jedis.get("name");
        System.out.println(name);

        //2.3 想让 某一个key的值 ++  或者 --操作
        jedis.set("age","18");

        Long incr = jedis.incr("age");
        System.out.println(incr); //19

        Long decr = jedis.decr("age");
        System.out.println(decr);  //18

        //2.4 设置一个nickName的key, 要求: 这个key只能存活5秒, 5秒后消失
        jedis.setex("nickName",5,"给别人戴帽子");

        while(jedis.exists("nickName")){

            // 获取一下还剩下多少时间
            // 如果返回的是正数, 表示还剩下多少时间
            // 如果返回的-2 表示的当前这个key已经不存在了
            // 如果返回的是-1  表示的当前这个key是一个永久有效的key
            Long time = jedis.ttl("nickName");
            System.out.println(time);
            Thread.sleep(1000);
        }
        //2.5 给已经存在的key设置有效时间
        jedis.expire("age",5);

        while(jedis.exists("age")){

            // 获取一下还剩下多少时间
            // 如果返回的是正数, 表示还剩下多少时间
            // 如果返回的-2 表示的当前这个key已经不存在了
            // 如果返回的是-1  表示的当前这个key是一个永久有效的key
            Long time = jedis.ttl("age");
            System.out.println(time);
            Thread.sleep(1000);
        }

        //2.6 如何拼接字符串:  name  隔壁老王+隔壁老张
        jedis.append("name","隔壁老张");

       name = jedis.get("name");
        System.out.println(name);

        //2.7 通用的删除某一个key
        jedis.del("name");

        //3. 释放资源

        jedis.close();

    }
```

### 5.2 jedis操作redis -- list

```java
@Test
    public void jedisTest03(){
        //1. 创建jedis对象
        // 在使用list想相关的API的时候, 建议从左侧添加, 从右侧弹出, 或者 从右侧添加从左侧 弹出
        Jedis jedis = new Jedis("192.168.72.142",6379);
        //2. 执行相关的操作
        //0 清空数据
        jedis.del("list1");
        //2.1 从左侧添加数据, 从右侧弹出数据
        jedis.lpush("list1","a","b","c","d");

        String rpop = jedis.rpop("list1");
        System.out.println(rpop); // a
        //2.2  查看列表
        List<String> list = jedis.lrange("list1", 0, -1);
        System.out.println(list); // [b c d]  [d c b ]

        //2.3 获取集合的个数
        Long size = jedis.llen("list1");
        System.out.println(size);

        //2.4 想在 c这个元素的前面添加一个 数字 0
        //  参数1:  在那个key中操作
        // 参数2:  添加到哪里去  before after
        // 参数3:  在谁的前面或者后面
        // 参数4:  添加的元素内容
        jedis.linsert("list1", BinaryClient.LIST_POSITION.BEFORE,"c","0");

        list = jedis.lrange("list1", 0, -1);
        System.out.println(list);

        //2.5 从右侧弹出一个元素, 将弹出的这个元素在添加到这个集合的头部
        String key1 = jedis.rpoplpush("list1", "list1");
        System.out.println(key1);


        list = jedis.lrange("list1", 0, -1);
        System.out.println(list);

        //3. 释放资源
        jedis.close();


    }
```

### 5.3 jedis操作redis -- set

```java
 	//4. 使用jedis 操作 redis --- set
    // set特点:  无序  去重
    @Test
    public void jedisTest04(){
        //1. 创建 jedis对象
        Jedis jedis = new Jedis("192.168.72.142",6379);

        //2. 执行相关的操作
        //2.1  添加元素:
        jedis.sadd("set1","q","w","e","r","w","q");
        //2.2 遍历出来
        Set<String> set = jedis.smembers("set1");
        System.out.println(set);
        //2.3  判断某一个元素在set集合是否存在
        // 如果值存在的, 返回true , 如果不存在, false
        Boolean flag = jedis.sismember("set1", "d");

        System.out.println(flag);

        //2.4 获取set集合中数量
        Long size = jedis.scard("set1");
        System.out.println(size);

        //3. 释放资源
        jedis.close();

    }
```

### 5.4 jedis操作redis -- sortedSet

```java
@Test
    public void jedisTest05(){
        //1. 创建 jedis对象
        Jedis jedis =  new Jedis("192.168.72.142",6379);

        //2. 执行相关的操作
        //2.1 添加元素:
        jedis.zadd("bookList",95,"斗破苍穹");
        jedis.zadd("bookList",85,"斗罗大陆");
        jedis.zadd("bookList",500,"西游记");
        jedis.zadd("bookList",436,"红楼梦");
        jedis.zadd("bookList",147,"水浒传");
        jedis.zadd("bookList",800,"三国演义");

        //2.2  从大到小进行遍历
       // Set<String> set = jedis.zrevrange("bookList", 0, -1);
        Set<Tuple> bookList = jedis.zrevrangeWithScores("bookList", 0, -1);
        for (Tuple tuple : bookList) {
            String book = tuple.getElement();
            double score = tuple.getScore();
            System.out.println(book +"  "+ score);
        }
        //2.3  想要看一下红楼梦这本书从大到小排名第几
        Long rank = jedis.zrevrank("bookList", "红楼梦");
        System.out.println(rank);  // 2  因为 从0开始


        //2.4  返回某个分数范围元素一共有多少个

        Long size = jedis.zcount("bookList", 450, 1000);
        System.out.println(size);
        //3. 释放资源
        jedis.close();

    }
```

## 6. jedis连接池

​	jedis看做是一个连接对象, 频繁的创建一个连接, 比较耗时耗资源的 通常情况, 采用连接池的技术, 提前的创建好一部分的连接对象, 放置到容器中, 反复的使用即可

* jedis的连接池基本使用

```java
//6. jedis连接池技术
    @Test
    public void jedisTest06(){
        //1. 创建一个连接池对象
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);  //最大连接数
        config.setMaxIdle(50);   // 最大闲时的数量
        config.setMinIdle(25);   // 最小闲时的数量

        JedisPool jedisPool = new JedisPool(config,"192.168.72.142",6379);

        //2. 从连接池中获取连接对象: jedis对象
        Jedis jedis = jedisPool.getResource();

        //3. 执行相关的操作
        String pong = jedis.ping();
        System.out.println(pong);

        //4. 释放资源(归还)

        jedis.close();

    }
```

* 提取成工具类

```java
package com.itheima.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtils {
    private static JedisPool jedisPool;
    // 什么加载: 随着类的加载而加载, 一般只会加载一次
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);  //最大连接数
        config.setMaxIdle(50);   // 最大闲时的数量
        config.setMinIdle(25);   // 最小闲时的数量
        // 注意:  千万别在等号左侧  写上 "JedisPool jedisPool " 而应该写成 "jedisPool"
        jedisPool = new JedisPool(config,"192.168.72.142",6379);
    }

    // 获取连接的方法

    public static Jedis getJedis(){


        return jedisPool.getResource() ;
    }
}

```

## 7. redis的持久化

持久化:  从内存到硬盘的过程

序列化: 从内存到硬盘的过程

反序列化 : 从硬盘到内存过程

> 监听器:   setAttribute(String name  Object value)

钝化 : 从内存到硬盘的过程

活化:  从硬盘内存过程

我们知道, redis是将数据存储在了内存当中, 那么当关闭服务器, 内存的资源也就会消失了, 这时存储在redis中的数据也会消失, 那么我们应该如何做呢?

- 在redis中已经提供了两种持久化的方案
  - RDB: redis提供的一种**基于快照机制**实现的持久化方案, 而快照就类似于照相机, 会将一个服务器某个时刻的一个状态整体保存下来, 快照文件一般都非常的小,只有几kb左右
    - 优点: 由于持久化的文件非常小, 适合于做灾难恢复
    - 缺点: 由于redis中持久化的时机问题, 会存在数据丢失的问题
  - AOF: redis提供的一种**基于日志机制**实现的持久化方案, 会将用户操作(增 删 改)的所有的命令整体的记录下来保存到日志文件中,一般文件都比较庞大
    - 优点: AOF机制可以让将用户所有的命令都记录下来, 顾其数据保存的比较完整, 不容易丢失
    - 缺点: 持久化的文件比较庞大, 不利于灾难恢复 
- RDB保存机制: redis默认是开启RDB机制

```text
save 900 1    :  在900秒之内,如果有一个数据进行修改,就会执行一下保存
save 300 10   : 在300秒之内, 如果有10个以上的数据被修改, 就会执行一下保存
save 60 10000 : 在60秒之内, 如果有10000个以上的数据被修改. 就会执行一下保存
```

- 当服务器宕机, 最大丢失数据量为在不到5分钟的时间里丢掉9999个数据
  - 一般情况下redis的不会出现宕机的现象, 除非redis中的数据将内存撑爆了, 但这个现象在大公司是不会出现的

------

- AOF的保存机制:redis默认不开启

如何开启AOF:

- 打开redis的配置文件,修改下列两个参数即可

```properties
appendonly yes    //默认此项是no,没有启动AOF机制
appendfsync everysec  //appendfsync的取值: [always everysec no]
```

- always: 总是, 只要有新的命令执行, 就将其保存到文件中
  - 优点: 数据保存最完整, 几乎不会丢掉数据
  - 缺点: 大大降低了redis的性能
- everysec: 每秒钟执行一次保存
  - 此种保存不会大幅度的降低redis的性能,但是会丢失最大1s的数据
- no: 不执行保存, 由操作系统自动调用保存(linux一般30分钟刷新一次内存)
  - 一般不使用

开发中:

​	一般小公司会采用AOF + RDB  大公司只会使用RDB





作业:

​	1) 昨天linux的环境统一

​	2) nginx的 静态网站的发布:  二种    ./nginx -s reload

​		一种: 放置在 html目录下

​		一种: 放置和 html目录同级

​	3) nginx的反向代理 和 负载均衡

```
upstream  别名 {
    server tomcat地址1;
    server tomcat地址2;
}

server {
    listen  80;
    servername  pinyougou.com;
    
    location / {
        proxy_pass http://别名;
    	index index.html  index.htm
    }	
    
    
}
```

​	4) redis : API (参考命令文档)

​	5) redis 提取 连接池工具类

# redis操作命令锦集

本章节给大家规整一下redis中常用的操作命令

## redis中五种数据类型

- 1) 字符串 String    ----**重点**
  - 特点: 存储所有的字符和字符串
  - 应用场景:  做缓存使用
- 2) 哈希  hash
  - 特点: 相当于java中hashMap集合
  - 应用场景: 可以存储javaBean对象, 此种使用场景不多,可被String替代
- 3) list集合
  - 特点: 相当于java中linkList, 是一个链表的结构
  - 应用场景: 做任务队列,
    - 在java中客户端提供了线程安全获取集合数据的方式
- 4) set 集合
  - 特点: 唯一, 无序
  - 应用场景: 集合运算
    - 例如去重复的操作
- 5) 有序set集合: sorted set
  - 特点:唯一, 有序
  - 应用场景: 一般用来做排行榜





## redis中String类型相关命令

- 赋值:  set key value
  - 设定key持有指定的字符串value，如果该key存在则进行覆盖操作。总是返回”OK”
- 取值: get key
  - 获取key的value。如果与该key关联的value不是String类型，redis将返回错误信息，因为get命令只能用于获取String value；如果该key不存在，返回(nil)
- 删除: del key
  - 删除指定的key
- 数值增减:
  - 增减值: incr key
    - 将指定的key的value原子性的递增1.如果该key不存在，其初始值为0，在incr之后其值为1。如果value的值不能转成整型，如hello，该操作将执行失败并返回相应的错误信息。
  - 减少值: decr key
    - 将指定的key的value原子性的递减1.如果该key不存在，其初始值为0，在incr之后其值为-1。如果value的值不能转成整型，如hello，该操作将执行失败并返回相应的错误信息
  - 增加固定的值: incrby key increment
    - 将指定的key的value原子性增加increment，如果该key不存在，器初始值为0，在incrby之后，该值为increment。如果该值不能转成整型，如hello则失败并返回错误信息
  - 减少固定的值: decrby key decrement
    - 将指定的key的value原子性减少decrement，如果该key不存在，器初始值为0，在decrby之后，该值为decrement。如果该值不能转成整型，如hello则失败并返回错误信息
- 拼接value值:  append key  value
  - 拼凑字符串。如果该key存在，则在原有的value后追加该值；如果该key不存在，则重新创建一个key|value
- 为key中内容设置有效时长:
  - 为新创建的key设置时长
    - setex key seconds value
  - 为已有的key设置有效时长
    - expire key seconds 
- 判断key是否存在: exists key
  - 返回1 表示存在, 返回0 表示不存在
- 获取key还剩余有效时长:  ttl  key
  - 特殊: 返回-1 表示永久有效 返回-2 表示不存在 
- keys * :  用来查看所有的key

## redis中hash类型的相关命令

- 存值: hset key field value
  - key为一个字符串, value类似于map,同样有一个field和value
- 取值:
  - 获取指定key的field的值:  hget key field
  - 获取指定key的多个field值: hmget key fields
  - 获取指定key中的所有的field与value的值:  hgetall key
  - 获取指定key中map的所有的field: hkeys key
  - 获取指定key中map的所有的value: hvals key
- 删除:
  - hdel key field [field … ] ：可以删除一个或多个字段，返回值是被删除的字段个数
  - del key ：删除整个内容
- 增加数字:
  - hincrby key field number：为某个key的某个属性增加值
- 判断某个key中的filed是否存在: hexists key field
  - 返回 0表示没有,  返回1 表示有
- 获取key中所包含的field的数量: hlen key

## redis中list集合类型的相关命令

redis的中的list集合类似于java中的linkedlist集合,此集合也是队列的一种, 支持向两端操作

- 添加:
  - 从左侧添加: lpush key values[value1 value2…]
    - 在指定的key所关联的list的头部插入所有的values，如果该key不存在，该命令在插入的之前创建一个与该key关联的空链表，之后再向该链表的头部插入数据。插入成功，返回元素的个数。
  - 从右侧添加: rpush key values[value1、value2…]
    - 在该list的尾部添加元素
- 查看列表 : lrange key start end
  - 获取链表中从start到end的元素的值，start、end从0开始计数；end可为负数，若为-1则表示链表尾部的元素，-2则表示倒数第二个，依次类推…
- 删除(弹出):
  - 从左侧弹出:lpop key
    - 返回并弹出指定的key关联的链表中的第一个元素，即头部元素。如果该key不存在，返回nil；若key存在，则返回链表的头部元素
  - 从右侧弹出: rpop key 
    - 从尾部弹出元素
- 获取列表中元素的个数: llen key
  - 返回指定的key关联的链表中的元素的数量
- 向指定的key插入数据, 仅在key存在时插入, 不存在不插入
  - 从左侧:lpushx key value
  - 从右侧: rpushx key value
- lrem key count value:
  - 删除count个值为value的元素，如果count大于0，从头向尾遍历并删除count个值为value的元素，如果count小于0，则从尾向头遍历并删除。如果count等于0，则删除链表中所有等于value的元素
- lset key index value:
  - 设置链表中的index的脚标的元素值，0代表链表的头元素，-1代表链表的尾元素。操作链表的脚标不存在则抛异常。
- linsert key before|after pivot value 
  - 在pivot元素前或者后插入value这个元素。
- rpoplpush resource destination
  - 将链表中的尾部元素弹出并添加到头部。[循环操作]

## redis中的set集合的相关命令操作

- 添加: sadd key values[value1、value2…]
  - 向set中添加数据，如果该key的值已有则不会重复添加
- 删除: srem key members[member1、member2…]
  - 删除set中指定的成员
- 获取所有的元素: smembers key
  - 获取set中所有的成员
- 判断元素是否存在: sismember key member
  - 判断参数中指定的成员是否在该set中，1表示存在，0表示不存在或者该key本身就不存在。（无论集合中有多少元素都可以极速的返回结果）
- 集合的差集运算: sdiff key1 key2…
  - 返回key1与key2中相差的成员，而且与key的顺序有关。那个在前, 返回那个key对应的差集
- 集合的交集运算:sinter key1 key2 key3…
  - 返回交集, 两个key都有的
- 集合的并集运算:sunion key1 key2 key3…
  - 返回并集
- 获取set中成员的数量: 
  - scard key
- 随机返回set中的一个成员: 
  - srandmember key
- 将key1、key2相差的成员存储在destination上: 
  - sdiffstore destination key1 key2…
- 将返回的交集存储在destination上: 
  - sinterstore destination key[key…]
- 将返回的并集存储在destination上:
  - sunionstore destination key[key…]

## redis中的sortedset集合的相关操作:

- 添加数据: zadd key score member
  - 将所有成员以及该成员的分数存放到sorted-set中。如果该元素已经存在则会用新的分数替换原有的分数。返回值是新加入到集合中的元素个数，不包含之前已经存在的元素
- 获得元素: 
  - zscore key member: 返回指定元素的值
  - zcard key: 获取集合中的成员数量
- 删除元素:zrem key member[member…]
  - 移除集合中指定的成员，可以指定多个成员。
- zrank key member:
  - 返回成员在集合中的排名。（从小到大）
- zrevrank key member
  - 返回成员在集合中的排名。（从大到小）
- zincrby key increment member:
  - 设置指定成员的增加的分数。返回值是更改后的分数 ...
- 范围查询: 
  - zrange key start end [withscores]: 获取集合中脚标为start-end的成员，[withscores]参数表明返回的成员包含其分数
  - zrevrange key start stop [withscores]: 按照元素分数从大到小的顺序返回索引从start到stop之间的所有元素（包含两端的元素）
- zremrangebyrank key start stop: 按照排名范围删除元素
- zremrangebyscore key min max: 按照分数范围删除元素
- zrangebyscore key min max \[withscores][limit offset count]:
  - 返回分数在[min,max]的成员并按照分数从低到高排序。[withscores]：显示分数；[limit offset count]：offset，表明从脚标为offset的元素开始并返回count个成员a
- zcount key min max:
  - 获取分数在[min,max]之间的成员

