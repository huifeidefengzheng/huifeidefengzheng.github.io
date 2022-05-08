SpringMVC第1天笔记

# 第1章 课程计划。

1、了解Spring MVC的基本概念

   \* 处理MVC

   \* 对比Struts2开发

   \* SpringMVC的优势。

2、Spring MVC入门（重点-Spring配置）

   请求--->控制器--->响应页面

3、常用注解

   \* REST风格的编程（掌握）

   *传递参数

 

今日学习目标：

1：能够实现SpringMVC的环境搭建：第3章

2：能够说出入门案例的执行过程：第3章，3.3

3：掌握@RequestMapping的使用：第3章，3.5

4：掌握SpringMVC的参数绑定：第3章，3.6

5：掌握SpringMVC的自定义类型转换器的使用：第3章，3.6.6

6：掌握SpringMVC的常用注解（第4章）

# 第2章  SpringMVC的基本概念 

 

## 2.1  关于三层架构和MVC 

### 2.1.1  三层架构 

我们的开发架构一般都是基于两种形式，一种是C/S架构，也就是客户端/服务器，另一种是B/S架构，也就是浏览器服务器。在JavaEE开发中，几乎全都是基于B/S架构的开发。那么在B/S架构中，系统标准的三层架构包括：表现层、业务层、持久层。三层架构在我们的实际开发中使用的非常多，所以我们课程中的案例也都是基于三层架构设计的。 

三层架构中，每一层各司其职，接下来我们就说说每层都负责哪些方面： 

表现层： 

也就是我们常说的web层。它负责接收客户端请求，向客户端响应结果，通常客户端使用http协议请求web层，web需要接收http请求，完成http响应。 

表现层包括展示层和控制层：控制层负责接收请求，展示层负责结果的展示。 

表现层依赖业务层，接收到客户端请求一般会调用业务层进行业务处理，并将处理结果响应给客户端。 

表现层的设计一般都使用MVC模型。（MVC是表现层的设计模型，和其他层没有关系） 

业务层： 

也就是我们常说的service层。它负责业务逻辑处理，和我们开发项目的需求息息相关。web层依赖业务层，但是业务层不依赖web层。 

业务层在业务处理时可能会依赖持久层，如果要对数据持久化需要保证事务一致性。（也就是我们说的，事务应该放到业务层来控制） 

持久层： 

也就是我们是常说的dao层。负责数据持久化，包括数据层即数据库和数据访问层，数据库是对数据进行持久化的载体，数据访问层是业务层和持久层交互的接口，业务层需要通过数据访问层将数据持久化到数据库中。通俗的讲，持久层就是和数据库交互，对数据库表进行增删改查的。 

![img](javaee-day34-springMVC01/wps1.jpg) 

 

### 2.1.2  MVC模型 

MVC全名是Model View Controller，是模型(model)－视图(view)－控制器(controller)的缩写，是一种用于设计创建 Web 应用程序表现层的模式。MVC中每个部分各司其职： 

Model（模型）： 

通常指的就是我们的数据模型。作用一般情况下用于封装数据。 

View（视图）： 

通常指的就是我们的jsp或者html。作用一般就是展示数据的。 

通常视图是依据模型数据创建的。 

Controller（控制器）： 

是应用程序中处理用户交互的部分。作用一般就是处理程序逻辑的。 

它相对于前两个不是很好理解，这里举个例子： 

例如： 

我们要保存一个用户的信息，该用户信息中包含了姓名，性别，年龄等等。 

这时候表单输入要求年龄必须是1~100之间的整数。姓名和性别不能为空。并且把数据填充到模型之中。 

此时除了js的校验之外，服务器端也应该有数据准确性的校验，那么校验就是控制器的该做的。 

当校验失败后，由控制器负责把错误页面展示给使用者。 

如果校验成功，也是控制器负责把数据填充到模型，并且调用业务层实现完整的业务需求。 

## 2.2  SpringMVC概述 

### 2.2.1 SpringMVC是什么 

SpringMVC是一种基于Java的实现MVC设计模型的请求驱动类型的轻量级Web框架，属于 Spring FrameWork 的后续产品，已经融合在Spring Web Flow里面。Spring 框架提供了构建 Web 应用程序的全功能 MVC 模块。使用 Spring 可插入的 MVC 架构，从而在使用Spring进行WEB开发时，可以选择使用 Spring 的 Spring MVC 框架或集成其他MVC开发框架，如Struts1(现在一般不用)，Xwork、Struts2等。 

SpringMVC已经成为目前最主流的 MVC 框架之一，并且随着Spring3.0的发布，全面超越 Struts2，成为最优秀的 MVC 框架。 

它通过一套注解，让一个简单的Java类成为处理请求的控制器，而无须实现任何接口。同时它还支持RESTful编程风格的请求。 

 

### 2.2.2 SpringMVC在三层架构的位置 

![img](javaee-day34-springMVC01/wps2.jpg) 

 

### 2.2.3 SpringMVC的优势 （面试）

1、清晰的角色划分： 

前端控制器（DispatcherServlet） 

请求到处理器映射器（HandlerMapping） 

处理器适配器（HandlerAdapter） 

视图解析器（ViewResolver） 

处理器或页面控制器（Controller） 

验证器（ Validator） 

命令对象（Command 请求参数绑定到的对象就叫命令对象） 

表单对象（Form Object 提供给表单展示和提交到的对象就叫表单对象）。 

2、分工明确，而且扩展点相当灵活，可以很容易扩展，虽然几乎不需要。 

3、由于命令对象就是一个POJO，无需继承框架特定API，可以使用命令对象直接作为业务对象。 

4、和Spring 其他框架无缝集成，是其它Web框架所不具备的。 

5、可适配，通过HandlerAdapter可以支持任意的类作为处理器。 

6、可定制性，HandlerMapping、ViewResolver等能够非常简单的定制。 

7、功能强大的数据验证、格式化、绑定机制。 

8、利用Spring提供的Mock对象能够非常简单的进行Web层单元测试。 

9、本地化、主题的解析的支持，使我们更容易进行国际化和主题的切换。 

10、强大的JSP标签库，使JSP编写更容易。 

………………还有比如RESTful风格的支持、简单的文件上传、约定大于配置的契约式编程支持、基于注解的零配置支持等等。 

 

### 2.2.4 SpringMVC和Struts2的优略分析 （面试）

共同点： 

它们都是表现层框架，都是基于MVC模型编写的。 

它们的底层都离不开原始ServletAPI（HttpServletRequest、HttpServletResponse...）。 

它们处理请求的机制都是一个核心控制器。 

 

区别： 

Spring MVC 的入口是 Servlet, 而 Struts2 是 Filter 

Spring MVC 是基于方法设计的，而Struts2是基于类，Struts2每次执行都会创建一个动作类。所以Spring MVC 会稍微比 Struts2 快些。 

Spring MVC 使用更加简洁,同时还支持 JSR303, 处理 ajax 的请求更方便 ，而struts2处理ajax的时候，需要导入一个struts2的插件包，并通过返回类型指定ajax的json数据。

(JSR303 是一套JavaBean参数校验的标准，它定义了很多常用的校验注解，我们可以直接将这些注解加在我们JavaBean的属性上面，就可以在需要校验的时候进行校验了。) （回去之后，有兴趣可以搜一下springMVC校验机制）

Struts2 的OGNL 表达式使页面的开发效率相比Spring MVC 更高些，但执行效率并没有比JSTL提升，尤其是struts2的表单标签，远没有html执行效率高。 

 

 

# 第3章 SpringMVC的入门 

## 3.1  SpringMVC的入门案例 

## 3.2  开发准备

第一步：创建web工程

![img](javaee-day34-springMVC01/wps3.jpg) 

项目名：springmvc_day01_quickStart

![img](javaee-day34-springMVC01/wps4.jpg) 

指定：

Key：archetypeCatalog

Value：internal

解决web项目使用idea创建工程后，加载过慢的问题。

![img](javaee-day34-springMVC01/wps5.jpg) 

第二步：导入jar包

````xml
<!-- 版本锁定 -->
<properties>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  <maven.compiler.source>1.8</maven.compiler.source>
  <maven.compiler.target>1.8</maven.compiler.target>
  <spring.version>5.0.2.RELEASE</spring.version>
</properties>



<dependencies>
  <dependency>
    <groupId>org.springframework</groupId>
    <artifactId>spring-context</artifactId>
    <version>${spring.version}</version>
  </dependency>

  <dependency>
    <groupId>org.springframework</groupId>
    <artifactId>spring-web</artifactId>
    <version>${spring.version}</version>
  </dependency>

  <dependency>
    <groupId>org.springframework</groupId>
    <artifactId>spring-webmvc</artifactId>
    <version>${spring.version}</version>
  </dependency>

  <dependency>
    <groupId>javax.servlet</groupId>
    <artifactId>servlet-api</artifactId>
    <version>2.5</version>
    <scope>provided</scope>
  </dependency>

  <dependency>
    <groupId>javax.servlet.jsp</groupId>
    <artifactId>jsp-api</artifactId>
    <version>2.0</version>
    <scope>provided</scope>
  </dependency>
</dependencies>
````

第三步：配置web.xml

在web.xml配置文件中核心控制器DispatcherServlet

````xml
<!-- SpringMVC的核心控制器 -->
<servlet>
    <servlet-name>dispatcherServlet</servlet-name>
    <servlet-class>org.springframework.web.servlet.DispatcherServlet</servlet-class>
    <!-- 配置Servlet的初始化参数，读取springmvc的配置文件，创建spring容器 -->
    <init-param>
        <param-name>contextConfigLocation</param-name>
        <param-value>classpath:springmvc.xml</param-value>
    </init-param>
    <!-- 配置servlet启动时加载对象 -->
    <load-on-startup>1</load-on-startup>
</servlet>
<servlet-mapping>
    <servlet-name>dispatcherServlet</servlet-name>
    <url-pattern>/</url-pattern>
</servlet-mapping>
````

第四步：配置编写springmvc.xml的配置文件

在resources下创建springmvc.xml

````xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:mvc="http://www.springframework.org/schema/mvc"
       xmlns:context="http://www.springframework.org/schema/context"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="
        http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans.xsd
        http://www.springframework.org/schema/mvc
        http://www.springframework.org/schema/mvc/spring-mvc.xsd
        http://www.springframework.org/schema/context
        http://www.springframework.org/schema/context/spring-context.xsd">

    <!-- 1：配置spring创建容器时要扫描的包，可以使用@Controller注解 -->
    <context:component-scan base-package="com.itheima"></context:component-scan>

    <!-- 2：配置视图解析器 -->
    <bean id="viewResolver" class="org.springframework.web.servlet.view.InternalResourceViewResolver">
        <property name="prefix" value="/WEB-INF/pages/"></property>
        <property name="suffix" value=".jsp"></property>
    </bean>

    <!-- 3：配置spring开启注解mvc的支持，自动开启处理器映射器和处理器适配器 -->
    <mvc:annotation-driven></mvc:annotation-driven>
</beans>
````

第五步：创建index.jsp

```html
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>Title</title>
</head>
<body>
<h3>入门案例</h3>

<a href="hello">入门案例</a>

</body>
</html>
```

第六步：创建包com.itheima.controller，创建类HelloController.java

````java
/**
 * 控制器
 */
@Controller
public class HelloController {

    // 请求参数
    @RequestMapping(path = "/hello")
    public String sayHello(){
        System.out.println("Hello SpringMVC!!");
        return "success";// 响应结果
    }
}
````

第七步：在WEB-INF下创建pages的文件夹，创建success.jsp

````html
<body>
<h1>访问成功</h1>
</body>
````

启动tomcat测试：

【配置tomcat服务器】

![img](javaee-day34-springMVC01/wps6.jpg) 

【配置tomcat服务器】

![img](javaee-day34-springMVC01/wps7.jpg) 

## 3.3  入门案例的执行过程分析

1：入门案例的执行流程

（1）当启动Tomcat服务器的时候，因为配置了load-on-startup标签，所以会创建DispatcherServlet对象，就会加载springmvc.xml配置文件

（2）开启了注解扫描，由于配置了@Controller，那么HelloController对象就会被创建

（3）从index.jsp发送请求，请求会先到达DispatcherServlet核心控制器，根据配置@RequestMapping注解找到执行的具体方法

（4）根据执行方法的返回值，再根据配置的视图解析器，去指定的目录下查找指定名称success的JSP文件

（5）Tomcat服务器渲染页面，做出响应

2：入门案例执行流程

![img](javaee-day34-springMVC01/wps8.jpg) 

3：SpringMVC官方提供图形

![img](javaee-day34-springMVC01/wps9.jpg) 

4：入门案例中的组件分析

（1）前端控制器（DispatcherServlet）

用户请求到达前端控制器，它就相当于mvc模式中的c，dispatcherServlet是整个流程控制的中心，由它调用其它组件处理用户的请求，dispatcherServlet的存在降低了组件之间的耦合性。 

（2）处理器映射器（HandlerMapping）

HandlerMapping负责根据用户请求找到Handler即处理器，SpringMVC提供了不同的映射器实现不同的映射方式，例如：配置文件方式，实现接口方式，注解方式等。 

返回执行链，包含两部分内容：

a) 处理器对象：Handler

b) HandlerInterceptor（拦截器）的集合

（3）处理器适配器（HandlAdapter）

通过HandlerAdapter对处理器进行执行，这是适配器模式的应用，通过扩展适配器可以对更多类型的处理器进行执行。 

前端控制器通过HandlerAdapter（处理器适配器）对Handler进行适配包装

 不同的适配器，查找不同的Handler，（即不同请求查找所对应Controller中处理的方法）

![img](javaee-day34-springMVC01/wps10.jpg) 

（4）处理器（Handler）

它就是我们开发中要编写的具体业务控制器。由DispatcherServlet把用户请求转发到Handler。由Handler对具体的用户请求进行处理。 

（5）视图解析器（View Resolver）

View Resolver负责将处理结果生成View视图，View Resolver首先根据逻辑视图名解析成物理视图名即具体的页面地址，再生成View视图对象，最后对View进行渲染将处理结果通过页面展示给用户。 

（6）视图（View）

SpringMVC框架提供了很多的View视图类型的支持，包括：jstlView、freemarkerView、pdfView等。我们最常用的视图就是jsp。 

一般情况下需要通过页面标签或页面模版技术将模型数据通过页面展示给用户，需要由程序员根据业务需求开发具体的页面。 

 

 

## 3.4  <mvc:annotation-driven>说明 

在SpringMVC的各个组件中，处理器映射器、处理器适配器、视图解析器称为SpringMVC的三大组件。 

使用<mvc:annotation-driven>自动加载RequestMappingHandlerMapping（处理映射器）和RequestMappingHandlerAdapter（处理适配器），可用在SpringMVC.xml配置文件中使用<mvc:annotation-driven>替代注解处理器和适配器的配置。 

它就相当于在xml中配置了：

````xml
<!-- 上面的标签相当于 如下配置-->
<!-- Begin -->
<!-- HandlerMapping -->
<bean class="org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping"></bean>
<bean class="org.springframework.web.servlet.handler.BeanNameUrlHandlerMapping"></bean>
        <!-- HandlerAdapter -->
<bean class="org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter"></bean>
<bean class="org.springframework.web.servlet.mvc.HttpRequestHandlerAdapter"></bean>
<bean class="org.springframework.web.servlet.mvc.SimpleControllerHandlerAdapter"></bean>
        <!-- HadnlerExceptionResolvers -->
<bean class="org.springframework.web.servlet.mvc.method.annotation.ExceptionHandlerExceptionResolver"></bean>
<bean class="org.springframework.web.servlet.mvc.annotation.ResponseStatusExceptionResolver"></bean>
<bean class="org.springframework.web.servlet.mvc.support.DefaultHandlerExceptionResolver"></bean>
        <!-- End -->
````

查看：spring-webmvc的jar包中 的AnnotationDriverBeanDefinitionParser.java的类。

![img](javaee-day34-springMVC01/wps11.jpg) 

注意： 

一般开发中，我们都需要在springmvc.xml写上\<mvc:annotation-driven>，因为它会帮助我们处理注解的一些默认配置。

## 3.5  RequestMapping注解 

2.4.1 使用说明 

源码： 

````java
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Mapping
public @interface RequestMapping {
    String name() default "";

    @AliasFor("path")
    String[] value() default {};

    @AliasFor("value")
    String[] path() default {};

    RequestMethod[] method() default {};

    String[] params() default {};

    String[] headers() default {};

    String[] consumes() default {};

    String[] produces() default {};
}
````

作用： 

用于建立请求URL和处理请求方法之间的对应关系

出现位置： 

当该注解标注在类上： 

请求URL的第一级访问目录。此处不写的话，就相当于应用的根目录。写的话需要以/开头。 

它出现的目的是为了使我们的URL可以按照模块化管理: 

例如： 

账户模块： 

/account/add 

/account/update 

/account/delete 

... 

订单模块： 

/order/add 

/order/update 

/order/delete 

...

红色的部分就是把RequsetMappding写在类上，使我们的URL更加精细。 

修改HelloController.java

```java
@Controller
@RequestMapping(path = "/user")
public class HelloController {

    // 请求参数
    @RequestMapping(path = "/hello")
    public String sayHello(){
        System.out.println("Hello SpringMVC!!");
        return "success";// 响应结果
    }

    // 请求参数
    @RequestMapping(path = "/testRequestMapping")
    public String testRequestMapping(){
        System.out.println("Hello SpringMVC!!，测试testRequestMapping方法");
        return "success";// 响应结果
    }
}
```

访问：<http://localhost:8080/springmvc_day01_quickStart/user/testRequestMapping>

 

类上：

请求URL的第一级访问目录。 

方法上： 

请求URL的第二级访问目录。 

属性： 

value：用于指定请求的URL。它和path属性的作用是一样的。 

method：用于指定请求的方式。 

params：用于指定限制请求参数的条件。它支持简单的表达式。要求请求参数的key和value必须和配置的一模一样。 

例如： 

params = {"accountName"}，表示请求参数必须有accountName 

params = {"accountName=zhangsan"}，表示请求参数中accountName的值必须是zhangsan。 

params = {"accountName!zhangsan"}，表示请求参数中accountName的值不能是zhangsan。 

headers：用于指定限制请求消息头的条件。 

注意： 

以上四个属性只要出现2个或以上时，他们的关系是与的关系。

````java
// 请求参数
@RequestMapping(path = "/testRequestMapping",method = {RequestMethod.GET},params = {"username=haha"},headers = "accept")
public String testRequestMapping(){
    System.out.println("Hello SpringMVC!!，测试testRequestMapping方法");
    return "success";// 响应结果
}
````

<http://localhost:8080/springmvc_day01_quickStart/user/testRequestMapping?username=haha>

 

要求：

1：必须是Get请求

2：必须有username=haha的参数

3：请求头必须包含accept的属性

 

## 3.6  请求参数的绑定（重点）

### 3.6.1 请求参数的绑定说明

（1）绑定机制

【1】表单提交的数据都是k=v格式的 username=haha&password=123

【2】SpringMVC的参数绑定过程是把表单提交的请求参数，作为控制器中方法的参数进行绑定的

【3】要求：提交表单的name和参数的名称是相同的

（2）支持的数据类型

【1】基本数据类型和字符串类型

【2】实体类型（JavaBean）

【3】集合数据类型（List、map集合等）

### 3.6.2 基本数据类型和字符串类型

（1）提交表单的name和参数的名称是相同的

（2）区分大小写

案例：

第一步：创建param.jsp

```html
<body>
<h3>传递参数</h3>

<a href="param/testParam?username=张三&age=18">入门案例</a>

</body>
```

第二步：创建ParamController.java

```java
@Controller
@RequestMapping(path = "/param")
public class ParamController {

    // 请求参数
    @RequestMapping(path = "/testParam")
    public String testParam(String username,Integer age){
        System.out.println("params测试：username:"+username+"   age:"+age);
        return "success";// 响应结果
    }
}
```

### 3.6.3 实体类型（JavaBean）

（1）提交表单的name和JavaBean中的属性名称需要一致

（2）如果一个JavaBean类中包含其他的引用类型，那么表单的name属性需要编写成：对象.属性 例如：address.name

案例：

第一步：创建 com.itheima.domain，创建类User.java

````java
public class User {
    private String username;
    private Integer age;
}
````

测试：

在Controller中定义

```java
@RequestMapping(value = "/testParam")
public String testParam(User user){
    System.out.println("欢迎执行ParamController中的testParam方法！user:"+user);
    return "success";// 执行视图解析器  /WEB-INF/page/success.jsp
}
```

访问：

![img](javaee-day34-springMVC01/wps12.jpg) 

第二步：创建Account.java

指定账号对应的用户

````java
public class Account {
    private String name;
    private String password;
    private Double money;
    private User user;
}
````

第三步：param.jsp

```html
<form action="param/saveAccount" method="post">
    账号：<input type="text" name="name"/><br>
    密码：<input type="text" name="password"/><br>
    金额：<input type="text" name="money"/><br>
    用户姓名：<input type="text" name="user.username"/><br>
    用户年龄：<input type="text" name="user.age"/><br>
    <input type="submit" value="提交"/>
</form>
```

第四步：ParamController.java

```java
@RequestMapping(path = "/saveAccount")
public String testParam(Account account){
    System.out.println(account);
    return "success";// 响应结果
}
```

### 3.6.4 请求参数中文乱码的解决

在web.xml中配置Spring提供的过滤器类

````xml
  <!-- 配置过滤器，解决中文乱码的问题 -->
    <filter>
        <filter-name>characterEncodingFilter</filter-name>
        <filter-class>org.springframework.web.filter.CharacterEncodingFilter</filter-class>
        <!-- 指定字符集 -->
        <init-param>
            <param-name>encoding</param-name>
            <param-value>UTF-8</param-value>
        </init-param>
    </filter>
    <filter-mapping>
        <filter-name>characterEncodingFilter</filter-name>
        <url-pattern>/*</url-pattern>
    </filter-mapping>
````

### 3.6.5 给集合属性数据封装（了解）

（1）JSP页面编写方式：list[0].属性

（2）JSP页面编写方式：map[‘one’].属性

第一步：param.jsp

```html
<form action="param/saveAccount" method="post">
    账号：<input type="text" name="name"/><br>
    密码：<input type="text" name="password"/><br>
    金额：<input type="text" name="money"/><br>
    用户姓名：<input type="text" name="user.username"/><br>
    用户年龄：<input type="text" name="user.age"/><br>

    用户姓名（list)：<input type="text" name="list[0].username"/><br>
    用户年龄（list)：<input type="text" name="list[0].age"/><br>
    用户姓名（list)：<input type="text" name="list[1].username"/><br>
    用户年龄（list)：<input type="text" name="list[1].age"/><br>

    用户姓名（map)：<input type="text" name="map['one'].username"/><br>
    用户年龄（map)：<input type="text" name="map['one'].age"/><br>
    用户姓名（map)：<input type="text" name="map['two'].username"/><br>
    用户年龄（map)：<input type="text" name="map['two'].age"/><br>
    <input type="submit" value="提交"/>
</form>
```

第二步：在Account.java中添加：

```java
private List<User> list;
private Map<String,User> map;
```

第三步：测试

### 3.6.6 自定义类型转换器（了解）

（1）表单提交的任何数据类型全部都是字符串类型，但是后台定义Integer类型，数据也可以封装上，说明SpringMVC框架内部会默认进行数据类型转换。

（2）如果想自定义数据类型转换，可以实现Converter的接口

第一步：在User.java中添加：

```java
public class User {
    private String username;
    private Integer age;
    private Date birthday;
}
```

第二步：param.jsp

````html
<form action="param/saveUser" method="post">
    姓名：<input type="text" name="username"/><br>
    年龄：<input type="text" name="age"/><br>
    生日：<input type="text" name="birthday"/><br>
    <input type="submit" value="提交"/>
</form>
````

第三步：ParamController.java

```java
@RequestMapping(path = "/saveUser")
public String saveUser(User user){
    System.out.println(user);
    return "success";// 响应结果
}
```

测试发现

默认支持的日期格式是：2010/11/12，如果输入2010-11-12呢？则会报错，怎么办呢？

【1】自定义类型转换器

````java
package com.itheima.utils;

import org.springframework.core.convert.converter.Converter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 把字符串转换成日期的转换器
 */
public class StringToDateConverter implements Converter<String, Date> {

    /**
     * 进行类型转换的方法
     */
    public Date convert(String source) {
        // 判断
        if(source == null) {
            throw new RuntimeException("参数不能为空");
        }
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            // 解析字符串
            Date date = df.parse(source);
            return date;
        } catch (Exception e) {
            throw new RuntimeException("类型转换错误");
        }
    }
}
````

【2】注册自定义类型转换器，在springmvc.xml配置文件中编写配置

````xml
<!-- 注册自定义类型转换器 -->
<bean id="conversionService" class="org.springframework.context.support.ConversionServiceFactoryBean">
    <property name="converters">
        <set>
            <bean class="com.itheima.utils.StringToDateConverter"></bean>
        </set>
    </property>
</bean>

<!-- 配置spring开启注解mvc的支持 -->
<mvc:annotation-driven conversion-service="conversionService"></mvc:annotation-driven>
````

再次访问：没有问题。

### 3.6.7 在控制器中使用原生的ServletAPI对象

只需要在控制器的方法参数定义HttpServletRequest和HttpServletResponse对象

第一步：param.jsp

```html
<br>
<a href="param/testServlet">测试ServletAPI</a>
```

第二步：paramController.java

````java
@RequestMapping(path = "/testServlet")
public String testServlet(HttpServletRequest request, HttpServletResponse response){
    System.out.println("request:"+request);
    System.out.println("session:"+request.getSession());
    System.out.println("application:"+request.getSession().getServletContext());
    System.out.println("response:"+response);
    return "success";// 响应结果
}
````

测试：

![img](javaee-day34-springMVC01/wps13.jpg) 

# 第4章 常用注解 

## 4.1 @RequestParam注解

作用： 

把请求中指定名称的参数给控制器中的形参赋值。 

属性： 

value：请求参数中的名称。 

required：请求参数中是否必须提供此参数。默认值：true。表示必须提供，如果不提供将报错。 

defaultValue：表示默认值，如果不传递值

使用案例：

第一步：创建anno.jsp

```html
<body>
<h3>RequestParam入门案例</h3>

<a href="anno/testRequestParam?name=哈哈&age=22">RequestParam</a>

</body>
```

第二步：创建AnnoController.java

```java
@Controller
@RequestMapping(path = "/anno")
public class AnnoController {

    // 请求参数
    @RequestMapping(path = "/testRequestParam")
    public String testRequestParam(@RequestParam(value = "name") String username,
                                   @RequestParam(value="age",required = true)Integer age){
        System.out.println("Hello SpringMVC!!，测试@RequestParam");
        System.out.println(username);
        System.out.println(age);
        return "success";// 响应结果
    }
}
```

## 4.2 @RequestBody注解

作用： 

1：@RequestBody主要用来接收前端传递给后端的json字符串中的数据的(请求体中的数据的)

2：用于获取请求体内容。直接使用得到是key=value&key=value...结构的数据。 get请求方式不适用。

 

属性： 

required：是否必须有请求体。默认值是:true。

当取值为true时,get请求方式会报错。

如果取值为false，get请求得到是null。

测试案例：

anno.jsp

```html
<h3>@RequestBody</h3>
post请求jsp代码： <br>
<!-- request body注解 -->
<form action="anno/testRequestBody" method="post">
    用户名称：<input type="text" name="username" ><br/>
    用户密码：<input type="password" name="password" ><br/>
    用户年龄：<input type="text" name="age" ><br/>
    <input type="submit" value="保存">
</form>
get请求jsp代码： <br>
<a href="anno/testRequestBody?body=test">requestBody注解get请求</a>
```

AnnoController.java

````java
// 请求参数
@RequestMapping(path = "/testRequestBody")
public String testRequestBody(@RequestBody(required = false) String body){
    System.out.println("Hello SpringMVC!!，测试@RequestParam");
    System.out.println(body);
    return "success";// 响应结果
}
````

输出结果：

Post请求：

![img](javaee-day34-springMVC01/wps14.jpg) 

Get请求：

![img](javaee-day34-springMVC01/wps15.jpg) 

后续在Controller拿到json数据的时候，可以使用@RequestBody获取json数据，然后可以将json的字符串，转换成javabean（后续讲）；使用@ResponseBody，可以做响应json数据，将javabean转换成json数据返回。

 

## 4.3 @PathVariable注解 

使用说明 

作用： 

用于绑定url中的占位符。例如：请求url中 /delete/{id}/{name}，这个{id},{name}就是url占位符。

传统方式：/delete?id=3&name=zhangsan

rest风格：/delete/3/zhangsan 

url支持占位符是spring3.0之后加入的。是springmvc支持restful风格URL的一个重要标志。 

属性： 

value：用于指定url中占位符名称。 

required：是否必须提供占位符。 

测试案例：

anno.jsp

````html
<h3>@PathVariable注解</h3>
<!-- PathVariable注解 -->
<a href="anno/testPathVariable/100">pathVariable注解</a>
````

AnnoController.java

````java
// 请求参数
@RequestMapping(path = "/testPathVariable/{uid}")
public String testPathVariable(@PathVariable(value = "uid") Integer id){
    System.out.println("Hello SpringMVC!!，测试@PathVariable");
    System.out.println(id);
    return "success";// 响应结果
}
````

测试结果：

![img](javaee-day34-springMVC01/wps16.jpg) 

 

### 4.3.1 REST风格URL 

什么是rest： 

REST（英文：Representational State Transfer，简称REST，说的意思是：表现层状态转变，或者叫做 “表述性状态转移”）描述了一个架构样式的网络系统，比如 web 应用程序。它首次出现在 2000 年 Roy Fielding 的博士论文中，他是 HTTP 规范的主要编写者之一。在目前主流的三种Web服务交互方案中，REST相比于SOAP（Simple Object Access protocol，简单对象访问协议）以及XML-RPC更加简单明了，无论是对URL的处理还是对Payload的编码，REST都倾向于用更加简单轻量的方法设计和实现。值得注意的是REST并没有一个明确的标准，而更像是一种设计的风格。 

它本身并没有什么实用性，其核心价值在于如何设计出符合REST风格的网络接口。 

restful的优点 

它结构清晰、符合标准、易于理解、扩展方便，所以正得到越来越多网站的采用。 

restful的特性： 

资源（Resources）：网络上的一个实体，或者说是网络上的一个具体信息。 

它可以是一段文本、一张图片、一首歌曲、一种服务，总之就是一个具体的存在。可以用一个URI（统一资源定位符）指向它，每种资源对应一个特定的 URI 。要获取这个资源，访问它的URI就可以，因此 URI 即为每一个资源的独一无二的识别符。 

 

表现层（Representation）：把资源具体呈现出来的形式，叫做它的表现层 （Representation）。 

比如，文本可以用 txt 格式表现，也可以用 HTML 格式、XML 格式、JSON 格式表现，甚至可以采用二进制格式。 

状态转化（State Transfer）：每发出一个请求，就代表了客户端和服务器的一次交互过程。 

HTTP协议，是一个无状态协议，即所有的状态都保存在服务器端。因此，如果客户端想要操作服务器，必须通过某种手段，让服务器端发生“状态转化”（State Transfer）。而这种转化是建立在表现层之上的，所以就是 “表现层状态转化”。具体说，就是 HTTP 协议里面，四个表示操作方式的动词：GET、POST、PUT、DELETE。它们分别对应四种基本操作：GET 用来获取资源，POST 用来新建资源，PUT 用来更新资源，DELETE 用来删除资源。 

传统的实例：唯一的URL地址（统一资源定位符），找到对应的资源

​       Url                        操作                         后台访问的方法

/account/findAll：        查询所有account           	findAll()

/account/findById?id=1：  查询id=1的account       	findById()

/account/delete?id=1：    删除id=1的account        	delete()

/account/update：         更新account                   update()

/account/save：           新增account                   save()

 

restful的示例： URI（统一资源标识符）

​     Uri          http（表现层）的协议      操作                                 后台访问的方法        

/account    	HTTP协议 GET ：       	查询所有 account            		findAll()

/account/1    HTTP协议 GET ：      	得到 id = 1 的 account    			findById()

/account/1   	HTTP协议 DELETE： 		删除 id = 1的 account   			delete()

/account 		HTTP协议 PUT： 		更新id = 1的 account         		update()

/account 		HTTP协议 POST： 		新增 account                 		save()

 

 

### 4.3.2 基于HiddentHttpMethodFilter的示例（了解）

作用： 

由于浏览器 form 表单只支持 GET 与 POST 请求，而DELETE、PUT 等 method 并不支持，Spring3.0 添加了一个过滤器，可以将浏览器请求改为指定的请求方式，发送给我们的控制器方法，使得支持 GET、POST、PUT 与DELETE 请求。 

使用方法： 

第一步：在web.xml中配置该过滤器。 

```xml
<filter>
    <filter-name>hiddenHttpMethodFilter</filter-name>
    <filter-class>org.springframework.web.filter.HiddenHttpMethodFilter</filter-class>
</filter>
<filter-mapping>
    <filter-name>hiddenHttpMethodFilter</filter-name>
    <url-pattern>/*</url-pattern>
</filter-mapping>
```

第二步：请求方式必须使用post请求。 

第三步：按照要求提供_method请求参数，该参数的取值就是我们需要的请求方式。 

源码分析：

![img](javaee-day34-springMVC01/wps17.jpg) 

案例：

anno.jsp

resfful风格：jsp中示例代码：

````html
<!-- 保存 -->
<form action="anno/testPathVariable" method="post">
    用户名称：<input type="text" name="username"><br/>
    <!-- <input type="hidden" name="_method" value="POST"> -->
    <input type="submit" value="保存"></form>
<hr/>
<!-- 更新 -->
<form action="anno/testPathVariable" method="post">
    用户名称：<input type="text" name="username"><br/>
    <input type="hidden" name="_method" value="PUT">
    <input type="submit" value="更新">
</form>
<hr/>
<!-- 删除 -->
<form action="anno/testPathVariable/1" method="post">
    <input type="hidden" name="_method" value="DELETE">
    <input type="submit" value="删除">
</form>
<hr/>
<!-- 查询一个 -->
<form action="anno/testPathVariable/1" method="post">
    <input type="hidden" name="_method" value="GET">
    <input type="submit" value="查询">
</form>
<hr/>
````

AnnoController.java

````java
// 请求参数
@RequestMapping(path = "/testPathVariable",method = RequestMethod.POST)
public String save(User user){
    System.out.println("Hello SpringMVC!!，测试@PathVariable，新增-请求方式Post");
    System.out.println(user);
    return "success";// 响应结果
}
// 请求参数
@RequestMapping(path = "/testPathVariable",method = RequestMethod.PUT)
public String update(User user){
    System.out.println("Hello SpringMVC!!，测试@PathVariable，更新-请求方式Put");
    System.out.println(user);
    return "success";// 响应结果
}
// 请求参数
@RequestMapping(path = "/testPathVariable/{uid}",method = RequestMethod.DELETE)
public String delete(@PathVariable(value = "uid") Integer id){
    System.out.println("Hello SpringMVC!!，测试@PathVariable，删除-请求方式Delete");
    System.out.println(id);
    return "success";// 响应结果
}
// 请求参数
@RequestMapping(path = "/testPathVariable/{uid}",method = RequestMethod.GET)
public String findById(@PathVariable(value = "uid") Integer id){
    System.out.println("Hello SpringMVC!!，测试@PathVariable，查询一个-请求方式Get");
    System.out.println(id);
    return "success";// 响应结果
}
````

测试结果：

![img](javaee-day34-springMVC01/wps18.jpg) 

PS：如果PUT请求和DELETE请求响应的时候报错

![img](javaee-day34-springMVC01/wps19.jpg) 

但是我们可以在控制台看到结果，说明执行了对应的方法，但是在响应到Jsp页面的时候，Jsp页面不支持PUT和DELETE请求的响应，所以抛出异常，我们可以使用@ResponseBody（后面讲）表示不再响应页面，即视图解析器将失效，以文本的形式返回，用在ajax的应用场景（返回文本或者json）

````java
// 修改是PUT协议
@RequestMapping(value = "/testPathVariable",method = RequestMethod.PUT)
public @ResponseBody String update(User user){
    System.out.println("put请求，修改");
    System.out.println(user);
    return "success";
}

// 删除是DELETE协议
@RequestMapping(value = "/testPathVariable/{id}",method = RequestMethod.DELETE)
public @ResponseBody String delete(@PathVariable(name = "id") Integer id,User user){
    System.out.println("delete请求，删除");
    System.out.println(id);
    System.out.println(user);
    return "success";
}
````

### 4.3.3 如果不想使用基于HiddentHttpMethodFilter，使用idea完成测试

打开idea，Tools下TestRESTful...

![img](javaee-day34-springMVC01/wps20.jpg) 

GET请求：

![img](javaee-day34-springMVC01/wps21.jpg) 

DELETE请求：

![img](javaee-day34-springMVC01/wps22.jpg) 

POST请求：

![img](javaee-day34-springMVC01/wps23.jpg) 

PUT请求：

![img](javaee-day34-springMVC01/wps24.jpg) 

## 4.4 @RequestHeader注解（了解）

作用： 

用于获取请求消息头。 

属性： 

value：提供消息头名称 

required：是否必须有此消息头 

注： 

在实际开发中一般不怎么用。 

案例：

anno.jsp

````html
<h3>测试@RequestHeader注解</h3>
<!-- RequestHeader注解 -->
<a href="anno/testRequestHeader">获取请求消息头</a>
````

AnnoController.java

````java
// 请求参数
@RequestMapping(path = "/testRequestHeader")
public String testRequestHeader(@RequestHeader(value="accept")String requestHeader){
    System.out.println("Hello SpringMVC!!，测试@RequestHeader");
    System.out.println(requestHeader);
    return "success";// 响应结果
}
````

测试结果：

![img](javaee-day34-springMVC01/wps25.jpg) 

 

## 4.5 @CookieValue 注解（了解）

作用： 

用于把指定cookie名称的值传入控制器方法参数。 

属性： 

value：指定cookie的名称。 

required：是否必须有此cookie。 

 

案例：

anno.jsp

```html
<h3>测试@CookieValue注解</h3>
<!-- CookieValue注解 -->
<a href="anno/testCookieValue">CookieValue注解</a>
```

AnnoController.java

````java
@RequestMapping(path = "/testCookieValue")
public String testCookieValue(@CookieValue(value = "JSESSIONID")String cookieValue){
    System.out.println("Hello SpringMVC!!，测试@CookieValue");
    System.out.println(cookieValue);
    return "success";// 响应结果
}
````

测试结果：

![img](javaee-day34-springMVC01/wps26.jpg) 

查看浏览器，右键点击地址栏

![img](javaee-day34-springMVC01/wps27.jpg) 

 

## 4.6 @ModelAttribute注解（了解）

作用： 

该注解是SpringMVC4.3版本以后新加入的。它可以用于修饰方法和参数。 

出现在方法上，表示当前方法会在控制器的方法执行之前，先执行。它可以修饰没有返回值的方法，也可以修饰有具体返回值的方法。 

出现在参数上，获取指定的数据给参数赋值。 

属性： 

value：用于获取数据的key。key可以是POJO的属性名称，也可以是map结构的key。 

应用场景： 

当表单提交数据不是完整的实体类数据时，保证没有提交数据的字段使用数据库对象原来的数据。 

例如： 

我们在编辑一个用户时，用户有一个创建信息字段，该字段的值是不允许被修改的。在提交表单数据是肯定没有此字段的内容，一旦更新会把该字段内容置为null，此时就可以使用此注解解决问题。 

![img](javaee-day34-springMVC01/wps28.jpg) 

 

### 4.6.1 基于POJO属性的基本使用（掌握）

anno.jsp

````html
<h3>测试@ModelAttribute注解</h3>
需求： 修改用户信息，要求用户的日期不能修改 jsp的代码：
<!-- 修改用户信息 -->
<form action="anno/testModelAttribute" method="post">
    用户名称：<input type="text" name="username" ><br/>
    用户年龄：<input type="text" name="age" ><br/>
    <input type="submit" value="保存">
</form>
````

AnnoController.java

```java
// ModelAttribute的用法
@ModelAttribute // 在执行的方法之前运行
public void showModel(User user) {
    System.out.println("执行了showModel方法"+user);
user.setBirthday(new Date());
}

/** * 模拟修改用户方法 * @param user * @return */
@RequestMapping("/testModelAttribute")
public String testModelAttribute(User user) {
    System.out.println("控制器中处理请求的方法：修改用户："+user);
    return "success";
}
```

测试结果：

![img](javaee-day34-springMVC01/wps29.jpg) 

 

 

### 4.6.2 基于Map的应用场景一：ModelAttribute修饰方法带返回值（了解） 

 

anno.jsp

````html
<h3>测试@ModelAttribute注解</h3>
需求： 修改用户信息，要求用户的日期不能修改 jsp的代码：
<!-- 修改用户信息 -->
<form action="anno/testModelAttribute" method="post">
    用户名称：<input type="text" name="username" ><br/>
    用户年龄：<input type="text" name="age" ><br/>
    <input type="submit" value="保存">
</form>
````

AnnoController.java

```java
@ModelAttribute // 在执行的方法之前运行
public User showModel(String username) {
    //模拟去数据库查询 
    User user = findUserByName(username);
    System.out.println("执行了showModel方法"+user);
    return user;
}

/** * 模拟修改用户方法 * @param user * @return */
@RequestMapping("/testModelAttribute")
public String testModelAttribute(User user) {
    System.out.println("控制器中处理请求的方法：修改用户："+user);
    return "success";
}

/** * 模拟去数据库查询 * @param username * @return User*/
private User findUserByName(String username) {
    User user = new User();
    user.setUsername(username);
    user.setAge(19);
    user.setBirthday(new Date());
    return user;
}
```

测试结果：

![img](javaee-day34-springMVC01/wps30.jpg) 

PS：传递username和age的属性值的时候，以页面传递的值为准；如果页面没有传递的参数，可以通过showModel的方法获取其他的参数值。

 

### 4.6.3 基于Map的应用场景二：ModelAttribute修饰方法不带返回值（了解） 

anno.jsp

<h3>测试@ModelAttribute注解</h3>
需求： 修改用户信息，要求用户的日期不能修改 jsp的代码：

````html
<h3>测试@ModelAttribute注解</h3>
需求： 修改用户信息，要求用户的日期不能修改 jsp的代码：
<!-- 修改用户信息 -->
<form action="anno/testModelAttribute" method="post">
    用户名称：<input type="text" name="username" ><br/>
    用户年龄：<input type="text" name="age" ><br/>
    <input type="submit" value="保存">
</form>
````

AnnoController.java

```java
@ModelAttribute // 在执行的方法之前运行
public void showModel(String username, Map<String,User> maps) {
    //模拟去数据库查询
    User user = findUserByName(username);
    maps.put("abc",user);
    System.out.println("执行了showModel方法"+user);
}

/** * 模拟修改用户方法 * @param user */
@RequestMapping("/testModelAttribute")
public String testModelAttribute(@ModelAttribute(value = "abc") User user) {
    System.out.println("控制器中处理请求的方法：修改用户："+user);
    return "success";
}

/** * 模拟去数据库查询 * @param username * @return User*/
private User findUserByName(String username) {
    User user = new User();
    user.setUsername(username);
    user.setAge(19);
    user.setBirthday(new Date());
    return user;
}
```

测试结果：

![img](javaee-day34-springMVC01/wps31.jpg) 

 



## 4.7 @SessionAttributes注解（了解）

作用： 

用于多次执行控制器方法间的参数共享。 放置到类的上面。

属性： 

value：用于指定存入的属性名称 

type：用于指定存入的数据类型。 

相当于将数据存放到Session中。

 

anno.jsp

````html
<h3>测试@SessionAttributes注解</h3>
<!-- SessionAttribute注解的使用 -->
<a href="anno/sessionAttributePut">存入SessionAttribute</a> <hr/>
<a href="anno/sessionAttributeGet">取出SessionAttribute</a> <hr/>
<a href="anno/sessionAttributeClean">清除SessionAttribute</a>
````

AnnoController.java

````java
 @Controller
@RequestMapping(path = "/anno")
@SessionAttributes(value ={"username","password"},types={Integer.class})
public class AnnoController {
    /**
     * * 把数据存入SessionAttribute
     * * @param model
     * * @return
     * * Model是spring提供的一个接口，该接口有一个实现类ExtendedModelMap *
     * 该类继承了ModelMap，而ModelMap就是LinkedHashMap子类 */
    @RequestMapping("/sessionAttributePut")
    public String testPut(Model model){
        System.out.println("把数据存入SessionAttribute");
        model.addAttribute("username", "泰斯特");
        model.addAttribute("password","123456");
        model.addAttribute("age", 31);
        //跳转之前将数据保存到username、password和age中，因为注解@SessionAttribute中有这几个参数
        return "success";
    }
    /**
     * * 获取SessionAttribute
     */
    @RequestMapping("/sessionAttributeGet")
    public String testGet(ModelMap model){
        System.out.println("获取SessionAttribute");
        System.out.println(model.get("username")+";"+model.get("password")+";"+model.get("age"));
        return "success";
    }
    /**
     * * 清空SessionAttribute
     */
    @RequestMapping("/sessionAttributeClean")
    public String complete(SessionStatus sessionStatus){
        System.out.println("清空SessionAttribute");
        sessionStatus.setComplete();
        return "success";
    }
}
````

测试结果：

先存入-->再获取（有值）-->再清空-->再获取（null）

这是实现了数据的共享

![img](javaee-day34-springMVC01/wps32.jpg) 