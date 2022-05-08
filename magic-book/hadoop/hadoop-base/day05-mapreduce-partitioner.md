---
title: day05-Hadoop-Mapreduce
date: 2019-08-11 11:20:24
tags: Hadoop
categories: day05-Hadoop-Mapreduce
---

# Hadoop-Mapreduce

## 1. MapReduce 介绍

MapReduce思想在生活中处处可见。或多或少都曾接触过这种思想。MapReduce的思想核心是“分而治之”，适用于大量复杂的任务处理场景（大规模数据处理场景）。

* Map负责“分”，即把复杂的任务分解为若干个“简单的任务”来并行处理。可以进行拆分的前提是这些小任务可以并行计算，彼此间几乎没有依赖关系。
* Reduce负责“合”，即对map阶段的结果进行全局汇总。
* MapReduce运行在yarn集群
  1. ResourceManager
  2. NodeManager

这两个阶段合起来正是MapReduce思想的体现。

![1565173934380](day05-mapreduce-partitioner/1565173934380.png)

还有一个比较形象的语言解释MapReduce:

我们要数图书馆中的所有书。你数1号书架，我数2号书架。这就是“Map”。我们人越多，数书就更快。

现在我们到一起，把所有人的统计数加在一起。这就是“Reduce”。




### 1.1. MapReduce 设计构思

MapReduce是一个分布式运算程序的编程框架，核心功能是将用户编写的业务逻辑代码和自带默认组件整合成一个完整的分布式运算程序，并发运行在Hadoop集群上。

MapReduce设计并提供了统一的计算框架，为程序员隐藏了绝大多数系统层面的处理细节。为程序员提供一个抽象和高层的编程接口和框架。程序员仅需要关心其应用层的具体计算问题，仅需编写少量的处理应用本身计算问题的程序代码。如何具体完成这个并行计算任务所相关的诸多系统层细节被隐藏起来,交给计算框架去处理：

Map和Reduce为程序员提供了一个清晰的操作接口抽象描述。MapReduce中定义了如下的Map和Reduce两个抽象的编程接口，由用户去编程实现.Map和Reduce,MapReduce处理的数据类型是<key,value>键值对。

* Map: `(k1; v1) → [(k2; v2)]`

* Reduce: `(k2; [v2]) → [(k3; v3)]`


一个完整的mapreduce程序在分布式运行时有三类实例进程：
1. `MRAppMaster` 负责整个程序的过程调度及状态协调
2. `MapTask` 负责map阶段的整个数据处理流程
3. `ReduceTask` 负责reduce阶段的整个数据处理流程

![1565173988392](day05-mapreduce-partitioner/1565173988392.png)



![1565174018996](day05-mapreduce-partitioner/1565174018996.png)

![1565174048246](day05-mapreduce-partitioner/1565174048246.png)

## 2. MapReduce 编程规范

> MapReduce 的开发一共有八个步骤, 其中 Map 阶段分为 2 个步骤，Shuffle 阶段 4 个步骤，Reduce 阶段分为 2 个步骤

#####  Map 阶段 2 个步骤

1. 设置 InputFormat 抽象类,这里使用资子类TextInputFormat, 将数据切分为 Key-Value**(K1和V1)** 对, 输入到第二步
2. 自定义 MyMapper 继承Mapper实现map()方法逻辑, 将第一步的结果转换成另外的 Key-Value（**K2和V2**） 对, 输出结果

##### Shuffle 阶段 4 个步骤

3. 对输出的 Key-Value 对进行**分区(Parttion)**
4. 对不同分区的数据按照相同的 Key **排序(Sort)**
5. (可选) 对分组过的数据初步**规约(COmbiner)**, 降低数据的网络拷贝
6. 对数据进行**分组(Group BY)**, 相同 Key 的 Value 放入一个集合中

##### Reduce 阶段 2 个步骤

7. 自定义MyReducer继承Reducer,实现reducer()方法,相同分区的数据会给同一个reducer,对多个 Map 任务的结果进行排序以及合并, 编写 Reduce 函数实现自己的逻辑, 对输入的 Key-Value 进行处理, 转为新的 Key-Value（**K3和V3**）输出
8. 设置 OutputFormat是抽象类使用实现子类TextOutputFormat, 处理并保存 Reduce 输出的 Key-Value 数据

![1565312167170](day05-mapreduce-partitioner/1565312167170.png)

## 3. WordCount

> 需求: 在一堆给定的文本文件中统计输出每一个单词出现的总次数

##### Step 1. 数据格式准备

1. 创建一个新的文件
  ```shell
  cd /export/servers
  vim wordcount.txt
  ```
2. 向其中放入以下内容并保存
  ```text
  hello,world,hadoop
  hive,sqoop,flume,hello
  kitty,tom,jerry,world
  hadoop
  ```
3. 上传到 HDFS
  ```shell
  hdfs dfs -mkdir /wordcount/
  hdfs dfs -put wordcount.txt /wordcount/
  ```

##### Step 2. Mapper

- 序列化(serializer)的应用场景
  - 将对象转换为字节流
  - 将对象保存在磁盘
  - 将对象通过网络发送出去
- hdfs与java的数据类型

```
LongWritable	-->	对应java中的Long
Text			-->String
FloatWritable	-->Float
DoubleWritable	-->Double
```

```java
// Mapper<LongWritable,Text,Text,LongWritable>
//LongWritable	:K1的类型
//Text			:V1的类型
//Text			:K2的类型
//LongWritable	:V2的类型
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    //map方法就是将K1和V1转换为K2和V2
    /*
    参数:
    	LongWritable	:K1行偏移量
    	Text			:V1每一行文本的数据
    	Context			:context表示上下文对象
    */
    
    /*
    如何将K1和V1转换为K2和V2
    K1			V1
    0			hello,world,hadoop
    15			hdfs,hive,hello
    ------------------------------
    
    K2			V2
    hello		1
    world		1
    hadoop		1
    hdfs		1
    hive		1
    hello		1
    */
    
    //map方法执行的次数是由文件中的K1和V1的对数来决定
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将一行文本数据进行拆分
        String line = value.toString();
        String[] split = line.split(",");
        //2.遍历数组:组织K2和V2
        for (String word : split) {
            //3.将K2和V2写入上下文
            //text.set(word);
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
```

##### Step 3. Reducer

```java
/**
     * 自定义我们的reduce逻辑
     * 所有的key都是我们的单词，所有的values都是我们单词出现的次数
     * @param key		:K2类型	
     * @param values	:V2类型
     * @param key		:K3类型
     * @param values	:v3类型
     */
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    /**
     * 自定义我们的reduce逻辑
     * 所有的key都是我们的单词，所有的values都是我们单词出现的次数
     * @param key		:新K2类型	
     * @param values	:集合新V2类型
     * @param context	:表示上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    //----------------------
    /*
    如何将新的k2和v2转换为k3和v3
    新	k2			v2
    	hello		<1,1,1>
    	world		<1,1>
    	hadoop		<1>
    ----------------------------
    	k3			v3
    	hello		3
    	world		2
    	hadoop		1
    */
//reduce方法的作用,将新的k2和v2转换为k3和v3,将k3和v3写入上下文中
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        //1.遍历集合,将集合中数字相加,得到v3
        for (LongWritable value : values) {
            count += value.get();
        }
        //2.将k3和v3写入上下文中
        context.write(key,new LongWritable(count));
    }
}
```

##### Step 4. 定义主类, 描述 Job 并提交 Job

```java
//Tool导包hadoop.util.Tool
//Configured导包hadoop.conf.Configured
public class JobMain extends Configured implements Tool {
    //该方法用于指定一个job任务
    @Override
    public int run(String[] args) throws Exception {
        //1.创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
        //2.配置job任务对象(八个步骤)
        //打包到集群上面运行时候，必须要添加以下配置，指定程序的main函数
        job.setJarByClass(JobMain.class);
        //第一步：指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcount"));

        //第二步：设置我们的mapper类的处理方式和数据类型
        job.setMapperClass(WordCountMapper.class);
        //设置我们map阶段完成之后的输出k2类型
        job.setMapOutputKeyClass(Text.class);
        //设置我们map阶段完成之后的输出v2类型
        job.setMapOutputValueClass(LongWritable.class);
        
        //第三步，第四步，第五步，第六步，采用默认方式
        
        //第七步：设置我们的reduce类
        job.setReducerClass(WordCountReducer.class);
        //设置我们reduce阶段完成之后的输出类型
        //设置k3的类型
        job.setOutputKeyClass(Text.class);
        //设置v3的类型
        job.setOutputValueClass(LongWritable.class);
        
        //第八步：设置输出类以及输出路径
        //设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径
        Path path = new Path("hdfs://node01:8020/wordcount_out");
        TextOutputFormat.setOutputPath(job,path);

        TextOutputFormat.setOutputPath(job,path);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        boolean b1 = fileSystem.exists(path);
        if (b1){
            //说明已经存储该文件夹
            fileSystem.delete(path,true);
        }
        
        //等待任务结束
        boolean b = job.waitForCompletion(true);//设置为true:表示控制台会输出进度信息设置为false控制台不会输出进度信息
        return b?0:1;
    }

    /**
     * 程序main函数的入口类
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Tool tool  =  new JobMain();
        //启动job任务
        int run = ToolRunner.run(configuration, tool, args);
        System.exit(run);
    }
}
```

##### 常见错误

如果遇到如下错误

```text
Caused by: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException): Permission denied: user=admin, access=WRITE, inode="/":root:supergroup:drwxr-xr-x
```

直接将hdfs-site.xml当中的权限关闭即可

```xml
<property>
  <name>dfs.permissions</name>
  <value>false</value>
</property>
```

最后重启一下 HDFS 集群

##### 小细节

本地运行完成之后，就可以打成jar包放到服务器上面去运行了，实际工作当中，都是将代码打成jar包，开发main方法作为程序的入口，然后放到集群上面去运行

## 4. MapReduce 运行模式

##### 本地运行模式

1. MapReduce 程序是被提交给 LocalJobRunner 在本地以单进程的形式运行
2. 处理的数据及输出结果可以在本地文件系统, 也可以在hdfs上
3. 怎样实现本地运行? 写一个程序, 不要带集群的配置文件, 本质是程序的 `conf` 中是否有 `mapreduce.framework.name=local` 以及 `yarn.resourcemanager.hostname=local` 参数
4. 本地模式非常便于进行业务逻辑的 `Debug`, 只要在 `Eclipse` 中打断点即可

```java
configuration.set("mapreduce.framework.name","local");
configuration.set(" yarn.resourcemanager.hostname","local");
TextInputFormat.addInputPath(job,new Path("file:///F:\\传智播客大数据离线阶段课程资料\\3、大数据离线第三天\\wordcount\\input"));
TextOutputFormat.setOutputPath(job,new Path("file:///F:\\传智播客大数据离线阶段课程资料\\3、大数据离线第三天\\wordcount\\output"));
```

![1565320283912](day05-mapreduce-partitioner/1565320283912.png)

![1565320006668](day05-mapreduce-partitioner/1565320006668.png)

- 如果打包出错则需要配置一下代码
- ![Z1565320994760](day05-mapreduce-partitioner/1565320994760.png)

- 删除已存在文件夹

![1565321091126](day05-mapreduce-partitioner/1565321091126.png)

- 删除本地已存在文件

![1565321336251](day05-mapreduce-partitioner/1565321336251.png)

##### 集群运行模式

1. 将 MapReduce 程序提交给 Yarn 集群, 分发到很多的节点上并发执行
2. 处理的数据和输出结果应该位于 HDFS 文件系统
3. 提交集群的实现步骤: 将程序打成JAR包，然后在集群的任意一个节点上用hadoop命令启动

```shell
hadoop jar hadoop_hdfs_operate-1.0-SNAPSHOT.jar cn.itcast.hdfs.demo1.JobMain
```

## 5. MapReduce 分区

在 MapReduce 中, 通过我们指定分区, 会将同一个分区的数据发送到同一个 Reduce 当中进行处理

例如: 为了数据的统计, 可以把一批类似的数据发送到同一个 Reduce 当中, 在同一个 Reduce 当中统计相同类型的数据, 就可以实现类似的数据分区和统计等

其实就是相同类型的数据, 有共性的数据, 送到一起去处理

Reduce 当中默认的分区只有一个

![1565332686941](day05-mapreduce-partitioner/1565332686941.png)

需求：将以下数据进行分开处理

详细数据参见partition.csv  这个文本文件，其中第五个字段表示开奖结果数值，现在需求将15以上的结果以及15以下的结果进行分开成两个文件进行保存

![1565347220987](day05-mapreduce-partitioner/1565347220987.png)

![1565334522763](day05-mapreduce-partitioner/1565334522763.png)

##### Step 1. 定义 Mapper

这个 Mapper 程序不做任何逻辑, 也不对 Key-Value 做任何改变, 只是接收数据, 然后往下发送

```java
/*
	k1:是行偏移量LongWritble]
	v1:行文本数据Text
	
	k2:行文本数据Text
	V2:占位符NullWritable
*/
public class MyMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    //map方法将k1和v1转成k2和v2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }
}
```

分区步骤:

##### Step 2. 自定义 Partitioner

主要的逻辑就在这里, 这也是这个案例的意义, 通过 Partitioner 将数据分发给不同的 Reducer

```
public class PartitonerOwn extends Partitioner<Text,LongWritable> {
	/*
	此getPartition()方法有两个功能
		1.定义分区规则
		2.返回对应的分区编号
	*/
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        //1.拆分文本数据k2,获取中奖字段的值
        String[] split = text.toString().split("\t");
        String numStr = split[5];
        //2.判断中奖指定的值和15之间的关系,然后返回对应的分区编号
        if(Integer.parentInt(numStr) > 15 ){
            return  0;
        }else{
            return 1;
        }
    }
}
```

##### Step 3. 定义 Reducer 逻辑

这个 Reducer 也不做任何处理, 将数据原封不动的输出即可

```java
/*
	k2:Text
	v2:NullWritable
	
	k3:Text
	v3:NullWritable
	
*/
public class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
```

##### Step 4. Main 入口

```java
public class PartitionMain  extends Configured implements Tool {
    public static void main(String[] args) throws  Exception{
        //启动job任务
        int run = ToolRunner.run(new Configuration(), new PartitionMain(), args);
        //退出任务
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {
        //1.创建job任务对象
        Job job = Job.getInstance(super.getConf(), PartitionMain.class.getSimpleName());
        //2.配置job任务对象(八个步骤)
        //2.1设置输入类和文件输入的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/partitioner/input"));
        //TextInputFormat.addInputPath(job,new Path("file:///D:\\partitioner\\input"));
        
        //2.2设置Mapper类和数据类型(K2和v2)
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //2.3指定分区类
        /**
         * 设置我们的分区类，以及我们的reducetask的个数，注意reduceTask的个数一定要与我们的
         * 分区数保持一致
         */
        job.setPartitionerClass(MyPartitioner.class);
        
        //2.4/2.5/2.6默认
        
        //2.7指定reducer类和数据类型(k3和v3)
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置reducerTask的个数
        job.setNumReduceTasks(2);

        //2.8设置输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
         TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/partitioner/out"));
        //TextOutputFormat.setOutputPath(job,new Path("file:///D:\\partitioner\\out"));
        
        //3.等待任务结束
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }
}
```

### 5.1 将单词长度大于等于5的分成一个区,小于5的一个区

#### 5.1.1创建Mypartitoner类继承Partitioner,实现getPartition()方法

![1565338444402](day05-mapreduce-partitioner/1565338444402.png)

#### 5.1.2在main方法添加MyPartitioner

![1565338816992](day05-mapreduce-partitioner/1565338816992.png)

## 6. MapReduce 排序和序列化

* 序列化 (Serialization) 是指把结构化对象转化为字节流

* 反序列化 (Deserialization) 是序列化的逆过程. 把字节流转为结构化对象. 当要在进程间传递对象或持久化对象的时候, 就需要序列化对象成字节流, 反之当要将接收到或从磁盘读取的字节流转换为对象, 就要进行反序列化

* Java 的序列化 (Serializable) 是一个重量级序列化框架, 一个对象被序列化后, 会附带很多额外的信息 (各种校验信息, header, 继承体系等）, 不便于在网络中高效传输. 所以, Hadoop 自己开发了一套序列化机制(Writable), 精简高效. 不用像 Java 对象类一样传输多层的父子关系, 需要哪个属性就传输哪个属性值, 大大的减少网络传输的开销

* Writable 是 Hadoop 的序列化格式, Hadoop 定义了这样一个 Writable 接口. 一个类要支持可序列化只需实现这个接口即可

* 另外 Writable 有一个子接口是 WritableComparable, WritableComparable 是既可实现序列化, 也可以对key进行比较, 我们这里可以通过自定义 Key 实现 WritableComparable 来实现我们的排序功能

数据格式如下

```text
a	1
a	9
b	3
a	7
b	8
b	10
a	5
```

要求:
* 第一列按照字典顺序进行排列
* 第一列相同的时候, 第二列按照升序进行排列

解决思路:
* 将 Map 端输出的 `<key,value>` 中的 key 和 value 组合成一个新的 key (newKey), value值不变
* 这里就变成 `<(key,value),value>`, 在针对 newKey 排序的时候, 如果 key 相同, 就再对value进行排序

##### Step 1. 自定义类型和比较器

```java
public class PairWritable implements WritableComparable<PairWritable> {
    // 组合key,第一部分是我们第一列，第二部分是我们第二列
    private String first;
    private int second;
    public PairWritable() {
    }
    public PairWritable(String first, int second) {
        this.set(first, second);
    }
    /**
     * 方便设置字段
     */
    public void set(String first, int second) {
        this.first = first;
        this.second = second;
    }
    /**
     * 反序列化
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        this.first = input.readUTF();
        this.second = input.readInt();
    }
    /**
     * 序列化
     */
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(first);
        output.writeInt(second);
    }
    /*
     * 重写比较器
     */
    public int compareTo(PairWritable o) {
        //每次比较都是调用该方法的对象与传递的参数进行比较，说白了就是第一行与第二行比较完了之后的结果与第三行比较，
        //得出来的结果再去与第四行比较，依次类推
        System.out.println(o.toString());
        System.out.println(this.toString());
        int comp = this.first.compareTo(o.first);
        if (comp != 0) {
            return comp;
        } else { // 若第一个字段相等，则比较第二个字段
            return Integer.valueOf(this.second).compareTo(
                    Integer.valueOf(o.getSecond()));
        }
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
    public String getFirst() {
        return first;
    }
    public void setFirst(String first) {
        this.first = first;
    }
    @Override
    public String toString() {
        return "PairWritable{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }
}
```

##### Step 2. Mapper

```java
public class SortMapper extends Mapper<LongWritable,Text,PairWritable,IntWritable> {

    private PairWritable mapOutKey = new PairWritable();
    private IntWritable mapOutValue = new IntWritable();

    @Override
    public  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineValue = value.toString();
        String[] strs = lineValue.split("\t");
        //设置组合key和value ==> <(key,value),value>
        mapOutKey.set(strs[0], Integer.valueOf(strs[1]));
        mapOutValue.set(Integer.valueOf(strs[1]));
        context.write(mapOutKey, mapOutValue);
    }
}
```

##### Step 3. Reducer

```java
public class SortReducer extends Reducer<PairWritable,IntWritable,Text,IntWritable> {

    private Text outPutKey = new Text();
    @Override
    public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//迭代输出
        for(IntWritable value : values) {
            outPutKey.set(key.getFirst());
            context.write(outPutKey, value);
        }
    }
}
```

##### Step 4. Main 入口

```java
package com.itheima.partitionersort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author lzs
 * @Date 2019-08-10
 */
public class SecondarySort extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new SecondarySort(), args);

        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), SecondarySort.class.getName());

        //1)配置文件输入类型和路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///H:\\input\\sort-in"));

        //2)设置map类和数据累着
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(PartWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3 4   5   6省略
        //7)配置reducer
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //8)配置文件输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///H:\\input\\sort-out"));

        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }
}

```

## MapReduce 中的计数器

计数器是收集作业统计信息的有效手段之一，用于质量控制或应用级统计。计数器还可辅助诊断系统故障。如果需要将日志信息传输到 map 或 reduce 任务， 更好的方法通常是看能否用一个计数器值来记录某一特定事件的发生。对于大型分布式作业而言，使用计数器更为方便。除了因为获取计数器值比输出日志更方便，还有根据计数器值统计特定事件的发生次数要比分析一堆日志文件容易得多。

hadoop内置计数器列表

| **MapReduce任务计数器** | **org.apache.hadoop.mapreduce.TaskCounter**                  |
| ----------------------- | ------------------------------------------------------------ |
| 文件系统计数器          | org.apache.hadoop.mapreduce.FileSystemCounter                |
| FileInputFormat计数器   | org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter |
| FileOutputFormat计数器  | org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter |
| 作业计数器              | org.apache.hadoop.mapreduce.JobCounter                       |

**每次mapreduce执行完成之后，我们都会看到一些日志记录出来，其中最重要的一些日志记录如下截图**

![1565347322757](day05-mapreduce-partitioner/1565347322757.png)

**所有的这些都是MapReduce的计数器的功能，既然MapReduce当中有计数器的功能，我们如何实现自己的计数器？？？**

> **需求：以上分区代码为案例，统计map接收到的数据记录条数**

##### 第一种方式

**第一种方式定义计数器，通过context上下文对象可以获取我们的计数器，进行记录**
**通过context上下文对象，在map端使用计数器进行统计**

```java
public class PartitionMapper  extends Mapper<LongWritable,Text,Text,NullWritable>{
    //map方法将K1和V1转为K2和V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws Exception{
        //定义计数器
        Counter counter = context.getCounter("MR_COUNT", "MyRecordCounter");
        //每次执行该方法,则计数器的变量值+1
        counter.increment(1L);
        context.write(value,NullWritable.get());
    }
}
```

**运行程序之后就可以看到我们自定义的计数器在map阶段读取了七条数据**

![1565347358883](day05-mapreduce-partitioner/1565347358883.png)	

##### **第二种方式**

**通过enum枚举类型来定义计数器**
统计reduce端数据的输入的key有多少个

```java
public class PartitionerReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
 //定义枚举
    public static enum Counter{
       MY_REDUCE_INPUT_RECORDS,MY_REDUCE_INPUT_BYTES
   }
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //使用枚举来定义计数器
       context.getCounter(Counter.MY_REDUCE_INPUT_RECORDS).increment(1L);
       context.write(key, NullWritable.get());
    }
}
```

![1565347383045](day05-mapreduce-partitioner/1565347383045.png)	

