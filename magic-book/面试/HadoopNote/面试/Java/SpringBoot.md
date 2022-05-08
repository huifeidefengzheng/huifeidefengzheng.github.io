# SpringBoot

Q1：什么是SpringBoot
SpringBoot是一个快速整合第三方框架，简化了XML配置，完全采用注解化；并且内置了Web服务器（Jetty和Tomcat），最终以Java应用程序进行执行。

Q2：为什么要用SpringBoot
1.传统的Web项目都是打成war包放入tomcat的webapps中进行运行，传统的SSM项目的配置文件特别多，如整合数据库访问层、业务逻辑层、进行事务配置，这些都需要在配置文件中进行配置；
2.而使用Spring Boot让我们的Spring应用变的更轻量化，并且省去了复杂的XML配置；学习起来也更容易、开发效率更高，SpringBoot的Web组件默认集成的是SpringMVC框架；
3.相较于传统框架，SpringBoot开发效率高，使用统一的版本管理（Maven继承原理）避免了jar冲突和各种依赖的搜寻。

Q3：SpringBoot启动方式

1.使用@ComponentScan开启扫包功能；通过@EnableAutoConfiguration开启自动装配，它可以帮助SpringBoot应用将所有符合条件的@Configuration配置都加载到当前SpringBoot创建并使用的IoC容器；
2.直接使用@SpringBootApplication注解，它等同于@Configuration、@EnableAutoConfiguration、@ComponentScan三个注解；扫包的范围是当前包及其所有子包。

Q4：SpringBoot与SpringMVC 区别
1.SpringBoot是一个快速开发的框架，能够快速的整合第三方框架，简化XML配置，全部采用注解形式；
2.内置Tomcat容器，帮助开发者能够实现快速开发；
3.SpringBoot的Web组件默认集成的是SpringMVC框架。

Q5：SpringBoot与SpringCloud区别

1.SpringBoot是快速开发框架，而SpringCloud是一套微服务解决框架；微服务框架是要做RPC通讯的，并且需要做服务中心，要有Server和Client，SpringCloud基于SpringBoot实现微服务框架；
2.在微服务架构中，通讯的协议采用RESTful风格协议，即http+json，SpringCloud采用Feign技术；
3.SpringCloud依赖与SpringBoot组件，使用SpringMVC编写Http协议接口，同时SpringCloud是一套完整的微服务解决框架；
4.SpringBoot可以作为微服务的基础框架、但他不是微服务框架

Q6：SpringBoot中用那些注解
1.@RestController：表示修饰该Controller所有的方法返回JSON格式，直接可以编写Restful接口，等同于@Controller加@ResponseBody两个注解；
2 @EnableAutoConfiguration：通过@EnableAutoConfiguration开启自动装配，它可以帮助SpringBoot应用将所有符合条件的@Configuration配置都加载到当前SpringBoot创建并使用的IoC容器；
3.@ComponentScan：开启扫包功能。

Q4：@EnableAutoConfiguration作用
通过@EnableAutoConfiguration开启自动装配，它可以帮助SpringBoot应用将所有符合条件的@Configuration配置都加载到当前SpringBoot创建并使用的IoC容器。

Q5：@SpringBootApplication原理
@SpringBootApplication注解主要就是将@Configuration、@EnableAutoConfiguration、@ComponentScan三个注解整合为了一个。

Q6：SpringBoot热部署使用什么？
Devtools

Q7：热部署原理是什么？
在发现代码有更改之后，重新启动应用，但是速度比手动停止后再启动还要更快（更快指的不是节省出来的手工操作的时间）；
其深层原理是使用了两个ClassLoader，一个Classloader加载那些不会改变的类（第三方Jar包），另一个ClassLoader加载会更改的类，称为restart ClassLoader
这样在有代码更改的时候，原来的restart ClassLoader被丢弃，重新创建一个restart ClassLoader，由于需要加载的类相比较少，所以实现了较快的重启时间（5秒以内）。

Q8：热部署原理与热加载区别是什么
热部署直接重新加载整个应用，热加载在运行时重新加载class。
Q9：你们项目中异常是如何处理
使用try-catch捕获异常并返回相关错误信息和错误码。
Q10：SpringBoot如何实现异步执行
启动类加上@EnableAsync后，在需要异步执行的方法上使用@Sync注解。
Q11：SpringBoot多数据源拆分的思路
针对不同的业务场景分成不同的包，并对每个包指定不同的数据源。
Q12：SpringBoot多数据源事务如何管理
使用时指定事务管理者transactionManager。
Q13：SpringBoot如何实现打包
Jar类型打包方式
1.使用mvn celan  package 打包
2.使用java –jar 包名
war类型打包方式
1.使用mvn celan package 打包
2.使用java –jar 包名
Q14：SpringBoot性能如何优化
1.启动优化：指定扫包路径而不是直接使用@SpringBootApplication；
2.将默认的Tomcat替换为Undertow来作为Servlet容器；
3.JVM参数调优。

Q15：SpringBoot2.0新特性

1.以Java 8为基准，不再支持6和7；
2.内嵌容器包结构调整；
3.Servlet-specific的server properties调整；
4.Actuator默认映射到/application；
5.不再支持Spring Loaded；
6.支持Quartz Scheduler；
7.支持OAuth 2.0；
8.支持Spring WebFlux；
9.版本要求：
 要求Jetty最低版本为9.4
 要求Tomcat最低版本为8.5
 要求Hibernate最低版本为5.2
 要求Gradle最低版本为3.4
 SendGrid最低支持版本是3.2

Q16：SpringBoot执行流程
首先新建一个SpringApplication对象，然后通过run方法进行启动；
启动后扫包，对于有spring相关注解的类通过反射为其创建代理对象，并交由spring容器管理。
Q17：SpringBoot底层实现原理
1.基于SpringMVC无配置文件（纯Java）完全注解化+内置tomcat-embed-core实现SpringBoot框架，Main函数启动；
2.SpringBoot核心快速整合第三方框架原理：Maven继承依赖关系；
3.SpringBoot内嵌入tomcat-embed-core；
4.SpringBoot采用SpringMVC注解版本实现无配置效果。
Q18：SpringBoot装配Bean的原理
通过@EnableAutoConfiguration开启自动装配，它可以帮助SpringBoot应用将所有符合条件的@Configuration配置都加载到当前SpringBoot创建并使用的IoC容器。
