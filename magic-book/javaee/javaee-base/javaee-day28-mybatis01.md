Mybatis第1天笔记

# 第1章 SSM的学习路线

第一部分：mybatis（3天）

第二部分：spring（4天）

第三部分：springmvc（3天）

 

学习方法

1：思考：我要学习什么？我学习的框架能做什么样的事情？（目标）（路径：构建思路）

   —— 搜索百度、谷歌

   —— 查阅资料

2：入门：根据资料，编写一个helloworld（实现1）

   ——借助网络资料，试着做做，验证你的想法。（先有想法，再去实施）

3：深入研究：根据api、根据网络的资源，根据项目需求去深入了解（实现2）

   ——循序渐进，罗列学习计划（今天学什么，明天学什么）（第一步学习什么？第二步学习什么？...）

4：总结：把别人的东西变成自己的知识（总结：今天所有知识，尽量脱离视频）

   ——辅助记忆，加深印象

   ——如果以后用到了该技术，你知道去哪里找

 

# 第2章 Mabatis课堂介绍。

第一天：mybatis的入门+mybatis的基本操作

​    mybatis环境搭建（xml）

​	操作数据库CRUD的实现

​	mybaits中的参数和结果集封装

​	分析mybatis中两种dao的编写方式

\* 面向接口（推荐）

\* 使用API面向实现类（不推荐）

第二天：mybatis深入

​	连接池的深入

​	动态sql语句的深入

​	表关系的深入（一对多,多对多）  特殊：多对一/一对一

第三天：mybatis中的加载策略和缓存以及基于注解的mybatis开发

​	加载策略（立即加载/延迟加载)

​	缓存（一级缓存/二级缓存）

​	注解开发Mybatis

 

今日学习目标：

1：能够了解什么是框架（第3章）

2：掌握Mybatis框架开发快速入门（第4章）

3：掌握Mybatis框架的基本CRUD操作（代理）（第5章）

4：掌握Mybatis的参数深入（第6章）

5：掌握Mybatis的DAO层实现类开发（了解）（第7章）

 

# 第3章 Mabatis的概述

## 3.1 什么Mabatis

![img](javaee-day28-mybatis01\wpsCB70.tmp.jpg) 

Mybatis（Hibernate）就是一个持久层的的框架。对JDBC做了轻量级封装。

![img](javaee-day28-mybatis01\wpsCB71.tmp.jpg) 

## 3.2 什么是框架

### 3.2.1 什么是框架。

​	JavaEE开发是分层的：表现层 业务层  持久层

![img](javaee-day28-mybatis01\wpsCB72.tmp.jpg) 

##### 	常见的JavaEE 开发框架：

1）解决数据的持久化问题的框架

MyBatis 本是apache的一个开源项目iBatis, 2010年这个项目由apache software foundation 迁移到了google code，并且改名为MyBatis 。2013年11月迁移到Github。

iBATIS一词来源于“internet”和“abatis”的组合，是一个基于Java的持久层框架。iBATIS提供的持久层框架包括SQL Maps和Data Access Objects（DAOs）

作为持久层的框架，还有一个封装程度更高的框架就是 Hibernate，但这个框架因为各种原因目前在国内的流行程度下降太多，现在公司开发也越来越少使用。 目前使用 Spring Data 来实现数据持久化也是一种趋势

2）解决 WEB层问题的 MVC 框架

Spring MVC属于SpringFrameWork的后续产品，已经融合在Spring Web Flow里面。

Spring 框架提供了构建 Web 应用程序的全功能 MVC 模块。

使用 Spring 可插入的 MVC 架构，从而在使用Spring进行WEB开发时，可以选择使用Spring的SpringMVC框架或集成其他MVC开发框架，如Struts1(现在一般不用)，Struts2等。

3）解决技术整合问题的框架

Spring是一个开放源代码的设计层面框架，他解决的是业务逻辑层和其他各层的松耦合问题，因此它将面向接口的编程思想贯穿整个系统应用。

Spring是于2003 年兴起的一个轻量级的Java 开发框架，由Rod Johnson创建。简单来说，Spring是一个分层的JavaSE/EE full-stack(一站式) 轻量级开源框架。

目的：解决企业应用开发的复杂性 

功能：使用基本的JavaBean代替EJB 

范围：任何Java应用

Spring是一个轻量级控制反转(IOC)和面向切面(AOP)的容器框架

 

框架（Framework）是整个或部分系统的可重用设计，表现为一组抽象构件及构件实例间交互的方法;另一种定义认为，框架是可被应用开发者定制的应用骨架。前者是从应用方面而后者是从目的方面给出的定义。 

简而言之，框架其实就是某种应用的半成品，就是一组组件，供你选用完成你自己的系统。简单说就是使用别人搭好的舞台，你来做表演。而且，框架一般是成熟的，不断升级的软件。 

### 3.2.2 mybatis框架

​	明确：它是一个持久层框架，解决项目对数据库的CRUD操作，只要会方法名、sql语句，学mybatis框架很轻松。

### 3.2.3 持久层技术：

​	回顾：jdbc操作数据库。

```java
		//a、注册驱动
		Class.forName("com.mysql.jdbc.Driver");
		//b、获取连接
		Connection conn = DriverManager.getConnecton(url,username,password);
		//c、获取预处理对象
		PreparedStatement pstm = conn.prepareStatement("select * from table ");
		//d、执行方法（可以是增删改，也可以是查询）  假定：查询
		ResultSet rs = pstm.executeQuery();
		//e、封装结果集
		while(rs.next()){
		}
		//f、释放资源
		rs.close();
		pstm.close();
		conn.close();
```

JDBC的代码

```java
public static void main(String[] args) { 
Connection connection = null; 
PreparedStatement preparedStatement = null; 
ResultSet resultSet = null; 
try { 
//加载数据库驱动 
Class.forName("com.mysql.jdbc.Driver"); 

//通过驱动管理类获取数据库链接 
connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mybatis?characterEncoding=utf-8","root", "root"); 
//定义sql语句 ?表示占位符 
String sql = "select * from user where username = ?"; 
//获取预处理statement 
preparedStatement = connection.prepareStatement(sql); 
//设置参数，第一个参数为sql语句中参数的序号（从1开始），第二个参数为设置的参数值 
preparedStatement.setString(1, "王五"); 
//向数据库发出sql执行查询，查询出结果集 
resultSet = preparedStatement.executeQuery(); 
//遍历查询结果集 
while(resultSet.next()){ 
System.out.println(resultSet.getString("id")+" "+resultSet.getString("username")); 
} 
} catch (Exception e) { 
e.printStackTrace(); 
}finally{ 
//释放资源 
if(resultSet!=null){ 
try { 
resultSet.close(); 
} catch (SQLException e) { 
e.printStackTrace(); 
} 
} 
if(preparedStatement!=null){ 
try { 
preparedStatement.close(); 
} catch (SQLException e) { 
e.printStackTrace(); 
} 
} 
if(connection!=null){ 
try { 
connection.close(); 
} catch (SQLException e) { 
// TODO Auto-generated catch block 
e.printStackTrace(); 
} 
} 
    }	
} 
```

上边使用jdbc的原始方法（未经封装）实现了查询数据库表记录的操作。 

##### 总结：JDBC缺陷总结

1：数据库链接创建、释放频繁造成系统资源浪费从而影响系统性能，如果使用数据库连接池可解决此问题。

2：Sql 语句在代码中硬编码，造成代码不易维护，实际应用 sql 变化的可能较大， sql 变动需要改变java 代码。

3：使用 preparedStatement 向占有位符号传参数存在硬编码，因为 sql 语句的 where 条件不一定，可能多也可能少，修改 sql 还要修改代码，系统不易维护。

4：对结果集解析存在硬编码（查询列名）， sql 变化导致解析代码变化，系统不易维护，如果能将数据库记录封装成 pojo 对象解析比较方便。

 

### 3.2.4 mybatis概述

​	它是基于Java编写的持久层框架，使开发者不必关心传统jdbc的api，只关心sql语句本身。

mybatis 通过 xml或注解的方式将要执行的各种 statement 配置起来，并通过 java 对象和 statement 中 sql的动态参数进行映射生成最终执行的 sql 语句，最后由 mybatis 框架执行 sql 并将结果映射为 java 对象并返回。采用 ORM 思想解决了实体和数据库映射的问题，对 jdbc 进行了封装，屏蔽了 jdbc api 底层访问细节，使我们不用与 jdbc api 打交道，就可以完成对数据库的持久化操作。

为了我们能够更好掌握框架运行的内部过程，并且有更好的体验，下面我们将从自定义 Mybatis 框架开始来学习框架。此时我们将会体验框架从无到有的过程体验，也能够很好的综合前面阶段所学的基础。

什么是ORM？

​	ORM ：Object Relational Mapping	对象关系映射（目的：操作对象，就可以操作数据库）

​	通过建立数据库表和Java实体类的对应关系，从而实现操作实体类就相当于操作数据库表。

​	ORM思想对应的框架有：mybatis，hibernate，spring data jpa

现阶段的数据访问层框架。

![img](javaee-day28-mybatis01\wpsCB73.tmp.jpg) 

作为持久层的框架，还有一个封装程度更高的框架就是Hibernate，但这个框架因为各种原因目前在国内的流行程度下降太多，现在公司开发也越来越少使用。目前使用Spring Data JPA来实现数据持久化也是一种趋势。

 

# 第4章 Mybatis框架入门

 

## 4.1 创建Maven工程并引入相关坐标 

创建项目mybaits_day01_quick

2.1.1.1 开发环境的准备及统一 

1.检查JDK环境 

![img](javaee-day28-mybatis01\wpsCB84.tmp.jpg) 

2.检查Tomcat环境（忽略，springmvc用到） 

3.检查maven环境 

![img](javaee-day28-mybatis01\wpsCB85.tmp.jpg) 

## 4.2 导入Maven依赖

````properties
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.itheima</groupId>
    <artifactId>mybatis_day01_quick</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <dependencies>
        <dependency>
            <groupId>org.mybatis</groupId>
            <artifactId>mybatis</artifactId>
            <version>3.4.5</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.18</version>
        </dependency>
        <!-- 日志坐标 -->
        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.12</version>
        </dependency>
        <!--测试-->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.10</version>
        </dependency>
    </dependencies>
</project>
````

## 4.3 创建数据库

创建数据库

```sql
DROP TABLE IF EXISTS `user`;

CREATE TABLE `user` (
  `id` int(11) NOT NULL auto_increment,
  `username` varchar(32) NOT NULL COMMENT '用户名称',
  `birthday` datetime default NULL COMMENT '生日',
  `sex` char(1) default NULL COMMENT '性别',
  `address` varchar(256) default NULL COMMENT '地址',
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

导入数据

```sql
insert  into `user`(`id`,`username`,`birthday`,`sex`,`address`) values (41,'老王','2018-02-27 17:47:08','男','北京'),(42,'小二王','2018-03-02 15:09:37','女','北京金燕龙'),(43,'小二王','2018-03-04 11:34:34','女','北京金燕龙'),(45,'传智播客','2018-03-04 12:04:06','男','北京金燕龙'),(46,'老王','2018-03-07 17:37:26','男','北京'),(48,'小马宝莉','2018-03-08 11:44:00','女','北京修正');
```

## 4.4 创建实体类

创建包：com.itheima.domain

创建类：User.java

```java
public class User implements Serializable {
    private int id;// 主键ID
    private String username;// 用户姓名
    private String sex;// 性别
    private Date birthday;// 生日
    private String address;// 地址

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", sex='" + sex + '\'' +
                ", birthday=" + birthday +
                ", address='" + address + '\'' +
                '}';
    }
}
```

## 4.5 创建接口UserDao.java

创建接口，UserDao.java，用来操作数据库

创建包com.itheima.dao，创建接口UserDao.java

```java
public interface UserDao {
    /
     * 查询所有
     */
    List<User> findAll();
}
```

也有人将它命名UserMapper，这个是因人而异，我们为了大家的编程习惯，叫做Dao。

## 4.6 创建sqlMapConfig.xml

在resources包下，新建sqlMapConfig.xml

![img](javaee-day28-mybatis01\wpsCB86.tmp.jpg) 

l sqlMapConfig.xml

为了更好将数据库连接信息抽取出来，我们原来在C3P0连接池中也已经将数据库连接信息抽取出来，我们现在也一样将数据库的连接信息抽取出来，放到一个xml（SqlMapConfig.xml）文件中，后面再去对此配置文件进行xml解析，这样就可以将配置文件中的信息读取出来，以便在jdbc代码中直接使用这些数据库连接信息

 ```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE configuration
        PUBLIC "-//mybatis.org//DTD Config 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-config.dtd">
<configuration>
    <!--配置mybatis的环境-->
    <environments default="mysql">
        <environment id="mysql">
            <!--配置事务管理策略-->
            <transactionManager type="JDBC"></transactionManager>
            <!--配置数据源-->
            <dataSource type="POOLED">
                <property name="driver" value="com.mysql.jdbc.Driver"/>
                <property name="url" value="jdbc:mysql:///itcastmybatis"/>
                <property name="username" value="root"/>
                <property name="password" value="root"/>
            </dataSource>
        </environment>
    </environments>
    <!--配置映射文件的信息-->
    <mappers>
        <mapper resource="com/itheima/dao/UserDao.xml"></mapper>
    </mappers>
</configuration>
 ```


## 4.7 创建UserDao.xml

因为是maven项目，所有xml的文件应该放在resources下，创建包com.itheima.dao，创建UserDao.xml

 ```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper
        PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
 <mapper namespace="com.itheima.dao.UserDao">
    <!--查询所有-->
    <select id="findAll" resultType="com.itheima.domain.User">
        select * from user
    </select>
</mapper>
 ```

 

## 4.8 创建测试类

在test文件夹下，创建包com.itheima.test，创建类MyBatisTest.java

```java
public class MyBatisTest {
    /
     * 测试mybatis的环境搭建
     */
    public static void main(String[] args) throws IOException {
        // 1.读取配置文件
        InputStream in = Resources.getResourceAsStream("sqlMapConfig.xml");
        // 2.根据配置文件构建SqlSessionFactory
        SqlSessionFactoryBuilder sqlSessionFactoryBuilder = new SqlSessionFactoryBuilder();
        SqlSessionFactory sqlSessionFactory = sqlSessionFactoryBuilder.build(in);
        // 3.使用SqlSessionFactory创建SqlSession
        SqlSession sqlSession = sqlSessionFactory.openSession();
        // 4.使用SqlSession构建Dao的代理对象
        UserDao userDao = sqlSession.getMapper(UserDao.class);
        // 5.执行dao的findAll方法
        List<User> list = userDao.findAll();
        // 第4，第5步骤可以写成
        // List<User> list = sqlSession.selectList("com.itheima.dao.UserDao.findAll");
        for(User user:list){
            System.out.println(user);
        }
        // 5.关闭资源
        sqlSession.close();
        in.close();

    }
}
```

查看测试结果：

![img](javaee-day28-mybatis01\wpsCB87.tmp.jpg) 

 

## 4.9 log4j.properties

如果没有发现日志，引入日志包，在resources文件下创建log4j.properties

```properties
# Set root category priority to INFO and its only appender to CONSOLE.
#log4j.rootCategory=INFO, CONSOLE            debug   info   warn error fatal
log4j.rootCategory=info, CONSOLE, LOGFILE

# Set the enterprise logger category to FATAL and its only appender to CONSOLE.
log4j.logger.org.apache.axis.enterprise=FATAL, CONSOLE

# CONSOLE is set to be a ConsoleAppender using a PatternLayout.
log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %-6r [%15.15t] %-5p %30.30c %x - %m\n

# LOGFILE is set to be a File appender using a PatternLayout.
log4j.appender.LOGFILE=org.apache.log4j.FileAppender
log4j.appender.LOGFILE.File=d:/axis.log
log4j.appender.LOGFILE.Append=true
log4j.appender.LOGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.LOGFILE.layout.ConversionPattern=%d{ISO8601} %-6r [%15.15t] %-5p %30.30c %x - %m\n
```

总结：

Mybatis操作的流程和原理是什么？

使用代理模式

# 第5章 Mabatis的CRUD（操作接口）（掌握）

需求：实现查询所有的用户操作

创建工程：

![img](javaee-day28-mybatis01\wpsCB97.tmp.jpg) 

## 5.1 查询所有

已经完成（略）

## 5.2 保存

### 5.2.1 UserDao.java

为了实现新增操作，我们可以在原有入门示例的UserDao.java类中添加一个用于saveUser()的方法用于用户新增操作。 

UserDao类中新增saveUser()方法，如下：

````java
public interface UserDao {
    /
     * 查询所有
     */
    List<User> findAll();
    
     /
     * 保存用户
     */
    void saveUser(User user);
}
````



### 5.2.2 UserDao.xml

在UserDao.xml文件中加入新增用户的配置，如下： 

添加<insert>标签

```xml
<!--保存用户-->
<insert id="saveUser" parameterType="com.itheima.domain.User">
    insert into user(username,address,sex,birthday)values(#{username},#{address},#{sex},#{birthday});
</insert>
```

我们可以发现，这个sql语句中使用#{}字符，#{}代表占位符，我们可以理解是原来jdbc所学的?，它们都是代表占位符， 具体的值是由User类的username属性来决定的。使用OGNL表达式 

parameterType属性：代表参数的类型，因为我们要传入的是一个类的对象，所以类型就写类的全名称。 

注意： 

这种方式要求<mapper namespace="映射接口所在的包名+类名">,同时还要求<select>,<insert>,<delete>,<update>这些标签中的id属性一定与代理接口中的方法名相同。 

### 5.2.3 MybatisTest.java

测试初始化：

\* 读取SqlMapConfig.xml

\* 构建SqlSessionFactory

\* 打开SqlSession

\* 获取UserDao对象

测试结束

\* 提交事务

\* 释放资源

这样方便测试。

```java
/**
 * 测试mybatis的crud操作
 */
public class MybatisTest {

    private InputStream in;
    private SqlSession sqlSession;
    private UserDao userDao;

    @Before//用于在测试方法执行之前执行
    public void init()throws Exception{
        //1.读取配置文件，生成字节输入流
        in = Resources.getResourceAsStream("SqlMapConfig.xml");
        //2.获取SqlSessionFactory
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(in);
        //3.获取SqlSession对象
        sqlSession = factory.openSession();
        //4.获取dao的代理对象
        userDao = sqlSession.getMapper(UserDao.class);
    }

    @After//用于在测试方法执行之后执行
    public void destroy()throws Exception{
        //提交事务
        sqlSession.commit();
        //6.释放资源
        sqlSession.close();
        in.close();
    }

    /**
     * 测试查询所有
     */
    @Test
    public void testFindAll(){
        //5.执行查询所有方法
        List<User> users = userDao.findAll();
        for(User user : users){
            System.out.println(user);
        }

    }

    /**
     * 测试保存操作
     */
    @Test
    public void testSave(){
        User user = new User();
        user.setUsername("如花1");
        user.setAddress("中粮商务公园");
        user.setSex("男");
        user.setBirthday(new Date());
        System.out.println("保存操作之前："+user);
        //5.执行保存方法
        userDao.saveUser(user);
        System.out.println("保存操作之后："+user);
    }
}
```

如果发现测试没有添加任何记录，原因是什么？ 

这一点和jdbc是一样的，我们在实现增删改时一定要去控制事务的提交，那么在mybatis中如何控制事务提交呢？ 

可以使用:session.commit();来实现事务提交。加入事务提交后的代码如下： 

但是：这时候发现保存操作之前User对象的id和保存操作之后User对象的id都为null，如果我们想在保存之后获取User对象的id呢？

### 5.2.4 问题扩展：新增用户id的返回值 

新增用户后，同时还要返回当前新增用户的id值，因为id是由数据库的自动增长来实现的，所以就相当于我们要在新增后将自动增长auto_increment的值返回。 

Mysql自增主键的返回，配置如下： 

```sql
insert into user(username,address,sex,birthday) values('张三','深圳','女','2018-07-24');
select last_insert_id()
```

配置：UserDao.xml

```
<!--保存用户-->
<insert id="saveUser" parameterType="com.itheima.domain.User">
    <!-- 配置插入操作后，获取插入数据的id -->
    <selectKey keyProperty="id" keyColumn="id" resultType="int" order="AFTER">
        select last_insert_id();
    </selectKey>
    insert into user(username,address,sex,birthday)values(#{username},#{address},#{sex},#{birthday});

</insert>
```

测试类MyBatisTest.java

```java
/**
 * 测试保存操作
 */
@Test
public void testSave(){
    User user = new User();
    user.setUsername("如花2");
    user.setAddress("中粮商务公园");
    user.setSex("男");
    user.setBirthday(new Date());
    System.out.println("保存操作之前："+user);
    //5.执行保存方法
    userDao.saveUser(user);
    System.out.println("保存操作之后："+user);
```

查看：

![img](javaee-day28-mybatis01\wpsCB98.tmp.jpg) 

## 5.3 修改

### 5.3.1 UserDao.java

```java
/**
 * 更新用户
 * @param user
 */
void updateUser(User user);
```

### 5.3.2 UserDao.xml

```xml
<update id="updateUser" parameterType="com.itheima.domain.User">
    update user set username=#{username},address=#{address},sex=#{sex},birthday=#{birthday} where id=#{id}
</update>
```

### 5.3.3 MybatisTest.java

```java
/**
 * 测试更新操作
 */
@Test
public void testUpdate(){
    User user = new User();
    user.setId(50);
    user.setUsername("如花2");
    user.setAddress("中粮商务公园");
    user.setSex("男");
    user.setBirthday(new Date());

    //5.执行保存方法
    userDao.updateUser(user);
```

## 5.4 删除

### 5.4.1 UserDao.java

```java
/**
 * 根据Id删除用户
 * @param userId
 */
void deleteUser(Integer userId);
```

### 5.4.2 UserDao.xml

 ```xml
<!-- 删除用户-->
<delete id="deleteUser" parameterType="java.lang.Integer">
    delete from user where id = #{uid}
</delete>
 ```

其中的#{uid}是占位符，代表参数的值由方法的参数传入进来的。

注意：

1.此处的#{uid}中的id其实只是一个形参，所以它的名称是自由定义的，比如定义成#{abc}也是可以的。

2.关于parameterType的取值问题，对于基本类型我们可以直接写成int,short,double…..也可以写成java.lang.Integer。

3.字符串可以写成string,也可以写成java.lang.String

也就是说：int是java.lang.Integer的别名

​          string是java.lang.String的别名

别名是不区分大小写

### 5.4.3 MybatisTest.java

```java
/**
 * 测试删除操作
 */
@Test
public void testDelete(){
    //5.执行删除方法
    userDao.deleteUser(48);
}
```

## 5.5 主键查询

### 5.5.1 UserDao.java

````java
/**
 * 根据id查询用户信息
 * @param id
 * @return
 */
User findById(Integer id);
````

### 5.5.2 UserDao.xml

 ```xml
<!-- 根据id查询用户 -->
<select id="findById" parameterType="INT" resultType="com.itheima.domain.User">
    select * from user where id = #{uid}
</select>
 ```

### 5.5.3 MybatisTest.java

```java
/**
 * 测试主键ID查询操作
 */
@Test
public void testFindOne(){
    //5.执行查询一个方法
    User  user = userDao.findById(50);
    System.out.println(user);
}
```

## 5.6 模糊查询

现在来实现根据用户名查询用户信息，此时如果用户名想用模糊搜索的话，我就可以想到前面Web课程中所学的模糊查询来实现。 

### 5.6.1 UserDao.java

可以在UserDao类中添加一个findByName()的方法，如下： 

```java
/**
 * 根据名称模糊查询用户信息
 * @param name
 * @return
 */
List<User> findByName(String name);
```

### 5.6.2 UserDao.xml

下面在UserDao.xml文件中加入模糊查询的配置代码，如下： 

```xml
<!-- 根据名称模糊查询 -->
<select id="findByName" parameterType="string" resultType="com.itheima.domain.User">
    select * from user where username like #{name}
</select>
```

注意：此时的#{name}中的因为这时候是普通的参数，所以它的起名是随意的，比如我们改成#{abc}也是可以的。 

### 5.6.3 MybatisTest.java

````java
/**
 * 测试模糊查询操作
 */
@Test
public void testFindByName(){
    //5.执行查询一个方法
    List<User> users = userDao.findByName("%王%");
    for(User user : users){
        System.out.println(user);
    }
}
````

我们在UserDao.xml配置文件中没有加入%来作为模糊查询的条件，所以在传入字符串实参时，就需要给定模糊查询的标识%。配置文件中的#{name}也只是一个占位符，所以SQL语句显示为“？”。

如何将模糊查询的匹配符%写到配置文件中呢？ 

### 5.6.4 模糊查询的另一种配置方式 

第一步：编写UserDao.xml文件，配置如下： 

```xml
<!-- 根据名称模糊查询 -->
<select id="findByName" parameterType="string" resultType="com.itheima.domain.User">
    select * from user where username like '%${value}%'
</select>
```

我们在上面将原来的#{}占位符，改成了${value}。注意如果用模糊查询的这种写法，那么${value}的写法就是固定的，不能写成其它名字。 

第二步：编写测试方法，如下： 

```java
/**
     * 测试模糊查询操作
     */
    @Test
    public void testFindByName(){
        //5.执行查询一个方法
//         List<User> users = userDao.findByName("%王%");
        List<User> users = userDao.findByName("王");
        for(User user : users){
            System.out.println(user);
        }
    }
```

查看控制台输出的语句：

![img](javaee-day28-mybatis01\wpsCB99.tmp.jpg) 

### 5.6.5 #{}与${}的区别 

-  **#{}表示一个占位符号** 

  通过#{}可以实现preparedStatement向占位符中设置值，自动进行java类型和jdbc类型转换，#{}可以有效防止sql注入。 #{}可以接收简单类型值或pojo属性值。 如果parameterType传输单个简单类型值，**#{}括号中可以是value或其它名称**。 

-  **${}表示拼接sql串** 

  通过${}可以将parameterType 传入的内容拼接在sql中且不进行jdbc类型转换， ${}可以接收简单类型值或pojo属性值，如果parameterType传输单个简单类型值，**${}括号中只能是value**。

那么为什么一定要写成${value}呢？我们一起来看TextSqlNode类的源码： 

![img](javaee-day28-mybatis01\wpsCB9A.tmp.jpg) 

这就说明了源码中指定了读取的key的名字就是”value”，所以我们在绑定参数时就只能叫value的名字了。  

## 5.7 查询数量（聚合函数）

### 5.7.1 UserDao.java

```java
/**
 * 查询总用户数
 * @return
 */
int findTotal();
```

### 5.7.2 UserDao.xml

```xml
<!-- 获取用户的总记录条数 -->
<select id="findTotal" resultType="int">
    select count(*) from user;
</select>
```

### 5.7.3 MybatisTest.java

```java
/**
 * 测试查询总记录条数
 */
@Test
public void testFindTotal(){
    //5.执行查询一个方法
    int count = userDao.findTotal();
    System.out.println(count);
}
```

# 第6章 Mybatis的参数深入 

1：Mybatis的映射文件其实就是与DAO相对应，因为DAO中的方法有输入参数及返回结果，那么在Mybatis的映射文件中自然也就有与之对应的参数和返回结果。 

2：在Mybatis的映射文件中参数用**parameterType**来代表，它的值可以是基本类型，也可以是包装的对象，这一点我们第二天学习中就使用过。 

3：在Mybatis的映射文件中返回结果用**resultType或resultMap**来代表。

  resultType：当查询字段和封装实体的属性名称一致的情况下 

  resultMap：当查询字段和封装实体的属性名称不一致的情况下

## 6.1 了解OGNL

OGNL表达式：

```
Object Graphic Navigation Language
对象		图		导航	   	语言
```

​	它是通过对象的取值方法来获取数据。在写法上把get给省略了。

​	比如：我们获取用户的名称

​		类中的写法：user.getUsername();

​		OGNL表达式写法：user.username

​	mybatis中为什么能直接写username,而不用user.呢：

​		因为在parameterType中已经提供了属性所属的类，所以此时不需要写对象名

**Mybatis使用ognl表达式解析对象字段的值，#{}或者${}括号中的值为pojo属性名称。** 

## 6.2 Mybatis的参数 

### 6.2.1 parameterType(输入类型) 

传递简单类型 （略）

int -- int 或者 java.lang.Integer

String -- string 或者java.lang.String

...

别名不区分大小写。

传递pojo对象 

   com.itheima.domain.User-- username、address、sex、birthday、id

### 6.2.2 QueryVo.java

开发中通过pojo传递查询条件 ，查询条件是综合的查询条件，不仅包括用户查询条件还包括其它的查询条件（比如将用户购买商品信息也作为查询条件），这时可以使用包装对象传递输入参数。 

Pojo类中包含pojo。 

**需求：根据用户名查询用户信息，查询条件放到QueryVo的user属性中。** 

在com.itheima.domain中，创建QueryVO.java

```java
public class QueryVo {

    private User user;

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}
```

### 6.2.3 UserDao.java

```java
/**
 * 根据queryVo中的条件查询用户
 * @param vo
 * @return
 */
List<User> findUserByVo(QueryVovo);
```

### 6.2.4 UserDao.xml

````xml
<!-- 根据queryVo的条件查询用户 -->
<select id="findUserByVo" parameterType="com.itheima.domain.QueryVo" resultType="com.itheima.domain.User">
    select * from user where username like #{user.username}
</select>
````

如果我们使用的是包装类作为参数，比如这个示例的QueryVo类作为findUserByVo()方法的参数，那么在使用时，因为QueryVo类中有一个User类的user对象，而这个user对象中才能找到username属性，所以我们在访问属性时，就使用OGNL表达式才访问对象的属性，即#{user.username}。

### 6.2.5 MybatisTest.java

```java
/**
 * 测试使用QueryVo作为查询条件
 */
@Test
public void testFindByVo(){
    QueryVo vo = new QueryVo();
    User user = new User();
    user.setUsername("%王%");
    vo.setUser(user);
    //5.执行查询一个方法
    List<User> users = userDao.findUserByVo(vo);
    for(User u : users){
        System.out.println(u);
    }
}
```

## 6.3 Mybatis的输出结果封装

### 6.3.1 resultType(输出类型) 输出简单类型 

看下边的例子输出整型： 

UserDao.xml文件 

```xml
<!-- 获取用户的总记录条数 -->
<select id="findTotal" resultType="int">
    select count(*) from user;
</select>
```

MybatisTest.java文件

```java
/**
 * 测试查询总记录条数
 */
@Test
public void testFindTotal(){
    //5.执行查询一个方法
    int count = userDao.findTotal();
    System.out.println(count);
}
```

输出简单类型必须查询出来的结果集有一条记录，最终将第一个字段的值转换为输出类型。 

### 6.3.2 resultType(输出类型) 简单对象的数据类型

让数据库的字段和实体类的属性一致：

User类中的属性：和数据库的字段一致。

````java
public class User {
    private Integer id;
    private String username;
    private String sex;
    private String address;
    private Date birthday;
}
````

![img](javaee-day28-mybatis01\wpsCBAB.tmp.jpg) 

所有定义映射文件的时候，可以在resultType的类型上指定com.itheima.domain.User

```xml
<!--查询所有-->
<select id="findAll" resultType="com.itheima.domain.User">
    select * from user
</select>
```

### 6.3.3 数据库中的字段和对象中的属性不一致

第一步：在com.itheima.domain中，修改User.java

```java
/**
 */
public class User implements Serializable {

    private Integer userId;
    private String userName;
    private String userAddress;
    private String userSex;
    private Date userBirthday;

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserAddress() {
        return userAddress;
    }

    public void setUserAddress(String userAddress) {
        this.userAddress = userAddress;
    }

    public String getUserSex() {
        return userSex;
    }

    public void setUserSex(String userSex) {
        this.userSex = userSex;
    }

    public Date getUserBirthday() {
        return userBirthday;
    }

    public void setUserBirthday(Date userBirthday) {
        this.userBirthday = userBirthday;
    }

    @Override
    public String toString() {
        return "User{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", userAddress='" + userAddress + '\'' +
                ", userSex='" + userSex + '\'' +
                ", userBirthday=" + userBirthday +
                '}';
    }
}
```

第二步：在包com.itheima.dao中的UserDao.java（不需要修改

第三步：在资源中com.itheima.dao中，改造UserDao.xml

```xml
<!-- 查询所有 -->
<select id="findAll" resultType="com.itheima.domain.User">
    select * from user;
</select>

<!--保存用户-->
<insert id="saveUser" parameterType="com.itheima.domain.User">
    <!-- 配置插入操作后，获取插入数据的id -->
    <selectKey keyProperty="userId" keyColumn="id" resultType="int" order="AFTER">
        select last_insert_id();
    </selectKey>
    insert into user(username,address,sex,birthday)values(#{userName},#{userAddress},#{userSex},#{userBirthday});

</insert>
```

第四步：测试MybatisTest.java

```java
/**
 * 测试查询所有
 */
@Test
public void testFindAll(){
    //5.执行查询所有方法
    List<User> users = userDao.findAll();
    for(User user : users){
        System.out.println(user);
    }

}

/**
 * 测试保存操作
 */
@Test
public void testSave(){
    User user = new User();
    user.setUserName("如花2");
    user.setUserAddress("中粮商务公园");
    user.setUserSex("男");
    user.setUserBirthday(new Date());
    System.out.println("保存操作之前："+user);
    //5.执行保存方法
    userDao.saveUser(user);
    System.out.println("保存操作之后："+user);
}
```

但是在查询的时候，发现：查询结果无法封装到实体

![img](javaee-day28-mybatis01\wpsCBAC.tmp.jpg) 

这是如何解决呢？

需求：如果返回的列名与实体类的属性不一致时，我们就不能封装结果集到指定的实体对象。 

解决方案一：

通过改别名的方式。 修改UserDao.xml

````xml
<!-- 查询所有 -->
<select id="findAll" resultType="com.itheima.domain.User">
    select id as userId,username as userName,address as userAddress,sex as userSex,birthday as userBirthday from user;
    <!--select * from user;-->
</select>
````

解决方案二：（推荐）

使用resultMap

### 6.3.4 定义resultMap 

由于上边的userDao.xml中sql查询列和Users.java类属性不一致，需要定义resultMap：

第一步：修改UserDao.xml

```xml
<!-- 查询所有 -->
<select id="findAll" resultMap="userMap">
    <!--select id as userId,username as userName,address as userAddress,sex as userSex,birthday as userBirthday from user; -->
    select * from user;
</select>
```

第二步：在UserDao.xml中的<mapper>下定义：

```java
<!-- 配置 查询结果的列名和实体类的属性名的对应关系 -->
<resultMap id="userMap" type="com.itheima.domain.User">
    <!-- 主键字段的对应 -->
    <id property="userId" column="id"></id>
    <!--非主键字段的对应-->
    <result property="userName" column="username"></result>
    <result property="userAddress" column="address"></result>
    <result property="userSex" column="sex"></result>
    <result property="userBirthday" column="birthday"></result>
</resultMap>
```

<id />：此属性表示查询结果集的唯一标识，非常重要。如果是多个字段为复合唯一约束则定义多个<id />。 

property：表示User类的属性。 

column：表示sql查询出来的字段名。 

column和property放在一块儿表示将sql查询出来的字段映射到指定的pojo类属性上。 

<result />：普通属性（普通字段），即pojo的属性。 

# 第7章 Mybatis实现DAO接口的实现类开发（了解） 

使用Mybatis开发Dao，通常有两个方法，即原始Dao开发方式（定义实现Dao接口的实现类）和Mapper接口代理开发方式（通用）。而现在主流的开发方式是接口代理开发方式，这种方式总体上更加简便。我们的课程讲解也主要以接口代理开发方式为主。 

 

## 7.1 Mybatis实现DAO的传统开发方式 

SqlSession中封装了对数据库的操作，如：查询、插入、更新、删除等。 

通过SqlSessionFactory创建SqlSession，而SqlSessionFactory是通过SqlSessionFactoryBuilder进行创建。 

 

### 7.1.1 创建工程mybatis_day02_dao

![img](javaee-day28-mybatis01\wpsCBAD.tmp.jpg) 

### 7.1.2 导入maven的坐标

```xml
<packaging>jar</packaging>
<dependencies>
    <dependency>
        <groupId>org.mybatis</groupId>
        <artifactId>mybatis</artifactId>
        <version>3.4.5</version>
    </dependency>

    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>5.1.6</version>
    </dependency>

    <dependency>
        <groupId>log4j</groupId>
        <artifactId>log4j</artifactId>
        <version>1.2.12</version>
    </dependency>

    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>4.10</version>
    </dependency>
</dependencies>
```

### 7.1.3 导入java类、资源文件、测试类

![img](javaee-day28-mybatis01\wpsCBBE.tmp.jpg) 

## 7.2 查询所有

第一步：调整User.java，与数据库的列名一致

```java
public class User implements Serializable {

    private Integer id;
    private String username;
    private String address;
    private String sex;
    private Date birthday;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", address='" + address + '\'' +
                ", sex='" + sex + '\'' +
                ", birthday=" + birthday +
                '}';
    }
}
```

第二步：调整UserDao.xml

```java
<!-- 查询所有
   resultType="com.itheima.domain.User"：指定返回的结果类型
   resultMap="userMap"：指定resultMap定义的id属性
-->
   <select id="findAll" resultType="com.itheima.domain.User">
       select * from user;
   </select>
```

第三步：创建UserDao.java的实现类UserDaoImpl.java

```java
public class UserDaoImpl implements UserDao {

    private SqlSessionFactory factory;

    public UserDaoImpl(SqlSessionFactory sqlSessionFactory){
        this.factory = sqlSessionFactory;
    }

    public List<User> findAll() {
        // 根据SqlSessionFactory获取SqlSession对象
        SqlSession sqlSession = factory.openSession();
        // 调用SqlSession中的方法，实现查询列表
        List<User> list = sqlSession.selectList("com.itheima.dao.UserDao.findAll"); // 参数就是能获取匹配信息的key
        // 释放资源
        sqlSession.close();
        return list;
    }
}
```

第四步：调整MybatisTest.java

````java
/**
 * 测试mybatis的crud操作
 */
public class MybatisTest {

    private InputStream in;
    private UserDao userDao;

    @Before//用于在测试方法执行之前执行
    public void init()throws Exception{
        //1.读取配置文件，生成字节输入流
        in = Resources.getResourceAsStream("SqlMapConfig.xml");
        //2.获取SqlSessionFactory
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(in);
        //3.获取dao的实现类对象
        userDao = new UserDaoImpl(factory);
    }

    @After//用于在测试方法执行之后执行
    public void destroy()throws Exception{
        in.close();
    }

    /**
     * 测试查询所有
     */
    @Test
    public void testFindAll(){
        //4.执行查询所有方法
        List<User> users = userDao.findAll();
        for(User user : users){
            System.out.println(user);
        }
    }
}
````

## 7.3 新增保存

第一步：UserDao.xml

```xml
<!--保存用户-->
<insert id="saveUser" parameterType="com.itheima.domain.User">
    <!-- 配置插入操作后，获取插入数据的id -->
    <selectKey keyProperty="id" keyColumn="id" resultType="int" order="AFTER">
        select last_insert_id();
    </selectKey>
    insert into user(username,address,sex,birthday)values(#{username},#{address},#{sex},#{birthday});
</insert>
```

第二步：UserDaoImpl.java

```
public void saveUser(User user) {
    // 根据SqlSessionFactory获取SqlSession对象
    SqlSession sqlSession = factory.openSession();
    // 调用SqlSession中的方法，实现新增
    sqlSession.insert("com.itheima.dao.UserDao.saveUser",user); // 参数就是能获取匹配信息的key
    // 提交事务
    sqlSession.commit();
    // 释放资源
    sqlSession.close();
}
```

第三步：MybatisTest.java

```java
/**
 * 测试保存操作
 */
@Test
public void testSave(){
    User user = new User();
    user.setUsername("如花1");
    user.setAddress("中粮商务公园");
    user.setSex("男");
    user.setBirthday(new Date());
    System.out.println("保存操作之前："+user);
    //5.执行保存方法
    userDao.saveUser(user);
    System.out.println("保存操作之后："+user);
```

## 7.4 修改保存

第一步：UserDao.xml

```xml
<!-- 更新用户 -->
<update id="updateUser" parameterType="com.itheima.domain.User">
    update user set username=#{username},address=#{address},sex=#{sex},birthday=#{birthday} where id=#{id}
</update>
```

第二步：UserDaoImpl.java

```java
public void updateUser(User user) {
    // 根据SqlSessionFactory获取SqlSession对象
    SqlSession sqlSession = factory.openSession();
    // 调用SqlSession中的方法，实现修改
    sqlSession.update("com.itheima.dao.UserDao.updateUser",user); // 参数就是能获取匹配信息的key
    // 提交事务
    sqlSession.commit();
    // 释放资源
    sqlSession.close();
}
```

第三步：MybatisTest.java

```java
/**
 * 测试更新操作
 */
@Test
public void testUpdate(){
    User user = new User();
    user.setId(50);
    user.setUsername("石榴姐");
    user.setAddress("北京市顺义区");
    user.setSex("女");
    user.setBirthday(new Date());

    //5.执行更新方法
    userDao.updateUser(user);
}
```

## 7.5 删除

第一步：UserDao.xml

```xml
<!-- 删除用户-->
<delete id="deleteUser" parameterType="java.lang.Integer">
    delete from user where id = #{uid}
</delete>
```

第二步：UserDaoImpl.java

```java
public void deleteUser(Integer userId) {
    // 根据SqlSessionFactory获取SqlSession对象
    SqlSession sqlSession = factory.openSession();
    // 调用SqlSession中的方法，实现删除
    sqlSession.update("com.itheima.dao.UserDao.deleteUser",userId); // 参数就是能获取匹配信息的key
    // 提交事务
    sqlSession.commit();
    // 释放资源
    sqlSession.close();
}
```

第三步：MybatisTest.java

```java
/**
 * 测试删除操作
 */
@Test
public void testDelete(){
    //5.执行删除方法
    userDao.deleteUser(56);
}
```

## 7.6 主键查询

第一步：UserDao.xml

```java
<!-- 根据id查询用户 -->
<select id="findById" parameterType="INT" resultType="com.itheima.domain.User">
    select * from user where id = #{uid}
</select>
```

第二步：UserDaoImpl.java

````java
public User findById(Integer id) {
    // 根据SqlSessionFactory获取SqlSession对象
    SqlSession sqlSession = factory.openSession();
    // 调用SqlSession中的方法，实现查询一个对象
    User user = sqlSession.selectOne("com.itheima.dao.UserDao.findById",id); // 参数就是能获取匹配信息的key
    // 释放资源
    sqlSession.close();
    return user;
}
````

第三步：MybatisTest.java

```java
/**
 * 测试主键ID查询操作
 */
@Test
public void testFindOne(){
    //5.执行查询一个方法
    User user = userDao.findById(50);
    System.out.println(user);
}
```

## 7.7 模糊查询

第一步：UserDao.xml

````xml
 <!-- 根据名称模糊查询 -->
<select id="findByName" parameterType="string" resultType="com.itheima.domain.User">
      select * from user where username like #{name}
</select>
````

第二步：UserDaoImpl.java

```java
public List<User> findByName(String name) {
    // 根据SqlSessionFactory获取SqlSession对象
    SqlSession sqlSession = factory.openSession();
    // 调用SqlSession中的方法，实现按照名称模糊查询
    List<User> list = sqlSession.selectList("com.itheima.dao.UserDao.findByName",name); // 参数就是能获取匹配信息的key
    // 释放资源
    sqlSession.close();
    return list;
}
```

第三步：MybatisTest.java

```java
/**
 * 测试模糊查询操作
 */
@Test
public void testFindByName(){
    //5.执行查询一个方法
    List<User> users = userDao.findByName("%王%");
    for(User user : users){
        System.out.println(user);
    }
}
```

## 7.8 查询数量

第一步：UserDao.xml

```xml
<!-- 获取用户的总记录条数 -->
<select id="findTotal" resultType="int">
    select count(*) from user;
</select>
```

第二步：UserDaoImpl.java

````java
public int findTotal() {
    // 根据SqlSessionFactory获取SqlSession对象
    SqlSession sqlSession = factory.openSession();
    // 调用SqlSession中的方法，实现按照名称模糊查询
    int value = sqlSession.selectOne("com.itheima.dao.UserDao.findTotal"); // 参数就是能获取匹配信息的key
    // 释放资源
    sqlSession.close();
    return value;
}
````

第三步：MybatisTest.java

```java
/**
 * 测试查询总记录条数
 */
@Test
public void testFindTotal(){
    //5.执行查询一个方法
    int count = userDao.findTotal();
    System.out.println(count);
```

小结：通过本次课程，学员应当熟练掌握Mybatis的基本操作。 

 

 

 

 

 