# 第3天 java基础进阶

1. 反射机制的概述和字节码对象的获取方式
2. **反射操作构造方法、成员方法、成员属性**
3. JavaBean的概述&BeanUtils的使用
4. **自定义BeanUtils工具类**

## 反射机制概述、字节码对象的获取方式、反射操作构造方法、成员方法、成员属性

## 反射机制的概述和字节码对象的获取方式

### 反射介绍

> JAVA反射机制是在**运行**状态中,对于任意一个类,都能够知道这个类的所有属性和方法
> 对于任意一个对象,都能够调用它的任意一个方法,这种动态获取的以及动态调用对象的方法的功能称为java语言的反射机制.
> 简单来说, 就可以把.class文件比做动物的尸体, 而反射技术就是对尸体的一种解剖.
> 通过反射技术, 我们可以拿到该字节码文件中所有的东西, 例如成员变量, 成员方法, 构造方法, 而且还包括私有

### 字节码文件获取的三种方式

1. 对象名.getCalss(); 方法来自于Object 对象已经存在的情况下, 可以使用这种方式
2. 类名.class 类名.class这是一个静态的属性, 只要知道类名, 就可以获取Class.forName("com.itheima_01.Student");  通过Class类中的静态方法, 指定字符串, 该字符串是类的全类名(包名+类名)
3. 此处将会抛出异常都系 ClassNotFoundException 防止传入错误的类名

### 案例代码

```java
package com.itheima_01;
/*
* 反射：
* 在运行时，我们可以获取任意一个类的所有方法和属性
* 在运行时，让我们调用任意一个对象的所有方法和属性
* 反射的前提：
* 要获取类的对象（Class对象）
*/
public class ReflectDemo {
public static void main(String[] args) throws ClassNotFoundException {
// 通过Object的getClass()方法获取，必须要有对象
Student s = new Student();
Class clazz = s.getClass();
// 通过类名获取字节码对象
Class clazz2 = Student.class;
// static Class<?> forName(String className)
Class clazz3 = Class.forName("com.itheima_01.Student");//推荐使用
System.out.println(clazz == clazz2);
System.out.println(clazz == clazz3);
System.out.println(clazz);
}
}
```

### 问题: 字节码对象是用来描述什么的

> 用来描述.class文件的.
面向对象阶段的时候讲过java中描述事物都是通过类的形式
而字节码文件也可以看做为一种事物, 如何描述这种事物? 那就看看这个事物是由什么组成的了
1. 成员变量
2. 成员方法
3. 构造方法

## 反射操作构造方法

### 通过获取的构造创建对象

步骤:

  1. 获得Class对象
  2. 获得构造
  3. 通过构造对象获得实例化对象

```java
package com.itheima_01;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
/*
* 通过反射获取构造方法并使用
* Constructor<?>[] getConstructors()
* Constructor<T> getConstructor(Class<?>... parameterTypes)
* T newInstance()
*
*Constructor：
* T newInstance(Object... initargs)
*/
public class ReflectDemo2 {
public static void main(String[] args) throws ReflectiveOperationException {
Class clazz = Class.forName("com.itheima_01.Student");
//method(clazz);
//Constructor<T> getConstructor(Class<?>... parameterTypes)
//method2(clazz);
//method3(clazz);
Object obj = clazz.newInstance();
System.out.println(obj);
}

private static void method3(Class clazz)
throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
Constructor c = clazz.getConstructor(String.class,int.class);//获取有参构造，参数1类型为String，参数2类型为int
System.out.println(c);
Object obj = c.newInstance("lisi",30);
System.out.println(obj);
}

private static void method2(Class clazz)
throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
Constructor c = clazz.getConstructor();//获取无参构造
System.out.println(c);
Object obj = c.newInstance();
System.out.println(obj);
}

private static void method(Class clazz) {
//Constructor<?>[] getConstructors() :获取所有public修饰的构造方法
Constructor[] cs = clazz.getConstructors();
for (int i = 0; i < cs.length; i++) {
System.out.println(cs[i]);
}
}
}
```

### 问题: 直接通过Class类中的newInstance()和获取getConstructor()有什么区别

> newInstance()方法, 只能通过空参的构造方法创建对象
>
> getConstructor(Class<T>... parameterTypes)方法, 方法接受一个可变参数, 可以根据传入的类型来匹配对应的构造方法

总结

Constructor<?>[] getConstructors()
                 Constructor<T> getConstructor(Class<?>... parameterTypes) 
                 方法1: 获取该类中所有的构造方法, 返回的是一个数组
                 方法2: 方法接受一个可变参数, 可以根据传入的类型, 来匹配对应的构造方法

## 反射操作公共成员变量

### 反射public成员变量(字段)

## **通过反射运行public变量流程**

**1. 通过反射获取该类的字节码对象**
Class clazz = Class.forName("com.heima.Person");
**2. 创建该类对象**
Object p = clazz.newInstance();
**3. 获取该类中需要操作的字段(成员变量)**
getField(String name) --> 方法传入字段的名称.
注意: 此方法只能获取公共的字段
Field f = clazz.getField("age");
**4. 通过字段对象中的方法修改属性值**

void set(Object obj, Object value) --> 参数1): 要修改那个对象中的字段, 参数2): 将字段修改为什么值.

f.set(p, 23);

## 案例代码

```java
package com.itheima_01;
import java.lang.reflect.Field;
/*
* 通过反射获取成员变量并使用
* Field[] getFields()
* Field getField(String name)
* Field[] getDeclaredFields()
* Field getDeclaredField(String name)
* Field:
* Object get(Object obj)
* void set(Object obj, Object value)
*/
public class ReflectDemo3 {
public  static void main(String[] args) throws ReflectiveOperationException {
//获取学生类的字节码对象
Class clazz = Class.forName("com.itheima_01.Student");
//获取学生类的对象
Object stu = clazz.newInstance();
//Field getField(String name) :根据字段名称获取公共的字段对象
Field f = clazz.getField("age");//获取成员变量对象
//System.out.println(f);
//void set(Object obj, Object value)
f.set(stu,28);//通过成员变量对象，修改指定对为指定的值
//Object get(Object obj)
Object age = f.get(stu);//通过对象获取成员变量的值
System.out.println(age);
System.out.println(stu);
}
private static void method(Class clazz) {
//Field[] getFields() :获取公共的成员变量
Field[] fs = clazz.getFields();
for (int i = 0; i < fs.length; i++) {
System.out.println(fs[i]);
}
System.out.println("----------");
//getDeclaredFields() ：获取所有的成员变量
Field[] fs2 = clazz.getDeclaredFields();
for (int i = 0; i < fs2.length; i++) {
System.out.println(fs2[i]);
}
}
}
```

### 方法总结

通过反射获取成员变量并使用  
Field[] getFields()              --> 返回该类所有(公共)的字段
Field getField(String name)      --> 返回指定名称字Field[] getDeclaredFields()      --> 暴力反射获取所有字段(包括私有)
Field getDeclaredField(String name) --> 暴力反射获取指定名称字段
---------------马上讲-----------------

Field:
Object get(Object obj)          --> Field对象调用, 返回传入对象的具体字段
void set(Object obj, Object value) -->  Field对象调用
参数1: 要修改的对象
参数2: 将此对象的字段修改为什么值.

## 反射操作私有成员变量

### 反射private成员变量(字段)

## **反射private属性执行流程**

1. 获取学生类字节码对象
2. 获取学生对象
3. 通过getDeclaredField方法获取私有字段
4. 通过setAccessible让jvm不检查权限
5. 通过set方法设置对象为具体的值

###　**案例代码**

```java
package com.itheima_01;
import java.lang.reflect.Field;
/*
* 通过反射获取私有成员变量并使用
* Field[] getDeclaredFields()
* Field getDeclaredField(String name)
*/
public class ReflectDemo4 {
public  static void main(String[] args) throws ReflectiveOperationException {
//获取学生类的字节码对象
Class clazz = Class.forName("com.itheima_01.Student");
//获取学生对象
Object stu = clazz.newInstance();
//获取私有的字段对象
Field f = clazz.getDeclaredField("name");
f.setAccessible(true);//设置反射时取消Java的访问检查,暴力访问
//System.out.println(f);
f.set(stu, "lisi");
Object name = f.get(stu);
System.out.println(name);
}
}
```

###　**方法总结**

Field[] getDeclaredFields()      --> 暴力反射获取所有字段(包括私有)
Field getDeclaredField(String name) --> 暴力反射获取指定名称字段
void setAccessible(boolean flag) --> 让jvm不检查权限

## 通过反射获取成员方法并使用

### 反射获取普通成员方法

###　**反射public方法执行流程**

1. 获取学生类字节码对象
2. 反射手段创建学生对象
3. 调用getMethod方法获取Method对象, 方法形参接受方法的名字
4. 调用Method方法中的invoke()将方法运行

### **案例代码**

```java
package com.itheima_01;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
/*
* 通过反射获取成员方法并使用
* Method getMethod(String name, Class<?>... parameterTypes)
* Method:
* Object invoke(Object obj, Object... args)
*
*/
public class ReflectDemo5 {
public static void main(String[] args) throws ReflectiveOperationException {
//获取学生类的字节码对象
Class clazz = Class.forName("com.itheima_01.Student");
//获取学生类的对象
Object stu = clazz.newInstance();
//获取无参有返回值的方法
Method m = clazz.getMethod("getName");
Object obj = m.invoke(stu);
System.out.println(obj);
}

private static void method2(Class clazz, Object stu)
throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
//获取有参无返回值的方法
Method m = clazz.getMethod("setName", String.class);
m.invoke(stu, "lisi");
System.out.println(stu);
}

private static void method(Class clazz, Object stu)
throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
//获取无参无返回值的方法
Method m = clazz.getMethod("method");
m.invoke(stu);
}
}
```

### **方法总结**

> Class:
> Method getMethod(String name, Class<?>... parameterTypes)  
> // 此方法由字节码对象调用
> // 参数1: 要反射的方法名称
>// 参数2: 此方法需要接受的参数类型(注意,传入的都是字节码)
> Method:
>Object invoke(Object obj, Object... args)  
>// 方法由Method对象调用
>// 参数1: 要由那个对象调用方法
> // 参数2: 方法需要的具体实参(实际参数)

### 问题: 私有的成员方法怎么玩?

// 获取字节码对象
Class clazz = Class.forName("com.heima.Student");

// 创建学生对象
Object stu = clazz.newInstance();

// 暴力反射获取方法
Method method = [clazz.getDeclaredMethod("method")]{.underline};

// 让jvm不检查权限
method.setAccessible(true);

// 执行方法
method.invoke(stu);

## JavaBean的概述、BeanUtils的使用

## JavaBean的概述和规范

**JavaBean的概述:**

##　**将需要操作的多个属性封装成JavaBean, 简单来说就是用于封装数据的**

**规范：**

> 类使用公共进行修饰
>
> 提供私有修饰的成员变量
>
> 为成员变量提供公共getter和setter方法
>
> 提供公共无参的构造

### 实例代码

```java
package com.itheima_02;
import java.io.Serializable;
/*
* JavaBean:用于封装数据
* 类使用公共进行修饰
* 提供私有修饰的成员变量
* 为成员变量提供公共getter和setter方法
* 提供公共无参的构造
* 实现序列号接口
*
*/
public class Person implenments Serializable {
private static final long serialVersionUID = 1049712678750452511L;
private String name;
private int age;
private String gender;
public Person() {
super();
}
public String getName() {
return name;
}
public void setName(String name) {
this.name = name;
}
public int getAge() {
return age;
}
public void setAge(int age) {
this.age = age;
}
public String getGender() {
return gender;
}
public void setGender(String gender) {
this.gender = gender;
}
@Override
public String toString() {
return "Person [name=" + name + ", age=" + age + ", gender=" + gender + "]";
}
}
```

## BeanUtils的概述

##　**BeanUtils的由来**

> 之前我们使用的类都是来自Java编写好的源代码
> 而这个BeanUtils却是一个叫做Apache的组织编写.
那么这个组织编写的代码当中, 有一个系列可以很方便的提高我们今后的开发效率.
这个系列为Commons, BeanUtils就是其中之一

### 准备工作

1. 导入两个jar包；

> commons-beanutils-1.8.3.jar
> commons-logging-1.1.1.jar

2. 将jar包Build path 配置到当前的classpath环境变量中

## **BeanUtils的常用方法**
> static void    setProperty(Object bean, String name, Object value)
> static String getProperty(Object bean, String name)
> static void    populate(Object bean, Map properties)

setProperty 用来给对象中的属性赋值(了解)

- 参数1: 需要设置属性的对象

- 参数2: 需要修改的属性名称

- 参数3: 需要修改的具体元素

getProperty 用来获取对象中的属性(了解)

- 参数1: 要获取的javaBean对象

- 参数2: 对象中的哪个属性

Populate 用来给对象中的属性赋值(掌握)

- 参数1: 要设置属性的对象

- 参数2: 将属性以Map集合的形式传入

Key : 属性的名称
Value: 属性具体的值

### **实例代码**

```java
package com.itheima_02;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.beanutils.BeanUtils;
/*
* BeanUtils：Apache commons提供的一个组件，主要功能就是为了简化JavaBean封装数据的操作
* static void setProperty(Object bean, String name, Object value)
* static String getProperty(Object bean, String name)
* static void populate(Object bean, Map properties)
*
* 注意：BeanUtils的setProperty和getProperty方法底层并不是直接操作成员变量，而是操作和成员变量名有关的get和set方法
*/
public class BeanUtilsDemo {
public static void main(String[] args) throws ReflectiveOperationException {
//static void populate(Object bean, Map properties)
Person p = new Person();
Map<String,Object> map = new HashMap<String,Object>();
map.put("name", "lisi");
map.put("age", 18);
map.put("gender", "male");
BeanUtils.populate(p,map);
System.out.println(p);
}

private static void method() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
Person p = new Person();
//System.out.println(p);
//static void setProperty(Object bean, String name, Object value) ：给JavaBean对象的成员变量进行赋值
BeanUtils.setProperty(p, "name", "zhangsan");
//BeanUtils.setProperty(p, "age", 18);
//System.out.println(p);
//static String getProperty(Object bean, String name)
String name = BeanUtils.getProperty(p, "name");
System.out.println(name);
}
}
```

方法总结

> 三个方法底层是通过反射实现, 而且反射操作的是setXxx方法和getXxx方法.
>
> 所以编写JavaBean的时候一定要注意格式

## 自定义BeanUtils的赋值和获取方法实现.

### 功能分析

> **定义MyBeanUtils工具类, 实现与BeanUtils相同的功能**
>
> public static void setProperty(Object bean,String name,Object value)
>
> // 设置任意对象的, 任意属性, 为任意的值
>
> public static String getProperty(Object bean,String name)
>
> // 获取任意对象的任意属性
>
> public static void populate(Object bean,Map map)
>
> // 修改任意对象中的属性, 为传入Map集合中的键和值

Ps: 下个知识点

实例代码

```java
package com.itheima_03;
import java.lang.reflect.Field;
public class MyBeanUtils {
private MyBeanUtils() {}
//public static void setProperty(Object bean,String name,Object value)
public  static void setProperty(Object bean,String name,Object value) throws ReflectiveOperationException {
//根据JavaBean对象获取对应的字节码对象
Class clazz = bean.getClass();
//根据字节码对象获取对应的Field对象
Field f = clazz.getDeclaredField(name);
//设置权限，让虚拟机不进行访问的检查
f.setAccessible(true);
//赋值
f.set(bean, value);
}


//public static String getProperty(Object bean,String name)
public static String getProperty(Object bean,String name) throws ReflectiveOperationException {
Class clazz = bean.getClass();
Field f = clazz.getDeclaredField(name);
f.setAccessible(true);
Object obj = f.get(bean);
return obj.toString();
}
}
```

## 自定义BeanUtils的populate方法实现

### **功能分析**

> public static void populate(Object bean,Map map)

// 修改任意对象中的属性, 为传入Map集合中的键和值
思路:

1. 获取传入对象的字节码对象
2. 获取map集合中所有的键和值
3. 调用Class中的getDeclaredField()方法将每一个键传入, 得到Field对象
4. 通过Field对象中的set方法赋值
5. Try catch捕获getDeclaredField方法可能发生的异常.(为了方式传入错误的值)

实例代码

```java
//public static void populate(Object bean,Map map)
public  static void populate(Object bean,Map map) throws ReflectiveOperationException {
//通过JavaBean对象来获取对应的字节码对象
Class clazz = bean.getClass();
//获取Map中所有的key
Set keys = map.keySet();
for (Object key : keys) {
try {
//根据key来获取对应的Field对象
Field f = clazz.getDeclaredField(key.toString());
//根据key来获取Map中对应的value
Object value = map.get(key);
f.setAccessible(true);
f.set(bean, value);
} catch(NoSuchFieldException e) {
//e.printStackTrace();
}
}
}
```
