# PYTHON基础

- 不可变序列：字符串、元组()
  - 不可变序列：没有增、删、改的操作
- 可变序列：列表[]、字典{key:value}、集合{}
  - 可变序列：可以执行增删改操作，对象地址不发生更改

## 1. 数字(int): 整形，浮点，整数类型定义的时候变量名后面直接跟数字，**数字类型是不可变数据类型**

```python
>>> age = 20
>>> type(age)
<class 'int'>
#数字的操作类型主要在程序中起到一个判断作用
num1=b'4' #bytes
num2=u'4' #Unicode  #python3中不用管，可以忽略
num3='四' #中文数字
num4='Ⅳ' #罗马数字

#isdigt
#作用就是判断是否是数字，一般就用isdigt就搞定了
print(num1.isdigit()) #True
print(num2.isdigit()) #True
print(num3.isdigit()) #False
print(num4.isdigit()) #False

#isdecimal:uncicode #bytes类型无isdecimal方法

print(num2.isdecimal()) #True
print(num3.isdecimal()) #False
print(num4.isdecimal()) #False

#isnumberic:unicode,中文数字,罗马数字
#bytes类型无isnumberic方法

print(num2.isnumeric()) #True
print(num3.isnumeric()) #True
print(num4.isnumeric()) #True
```

## 2. 字符串（str）,定义字符串的时候需要用引号引起来,可以用单，双，三引号，三引号多表示多行字符串，这样就可以省掉“\n”换行符换行。

字符串的基本操作主要有：copy，拼接，查找，统计，检测，切片，大小写等。这里需要注意**字符串是****不可变类型**，上述操作并没有改变原来的字符串，只是创建了一个新的字符串，原来的字符串任然在内存中！

```python
a = "alexa"
a.find("a")#查找a第一次出现位置索引，如果没有返回-1
a.rfind('a')#查找a最后一次出现的位置索引。如果没有返回-1
a.index('a')
a = "alex"
b = a
print(a,b)
alex alex

a = "alex"
b = "egon"
print(a + b)
#alexegon

#这里也可以用join来实现字符串的拼接效果，可以指定连接符号如（？， —，*) 之类的，当然也可以是空格!
a = "alex"
b = "egon"
print(a.join(b))
#ealexgalexoalexn

#这里的查找是按照下标索引进行的，用到index
>>> a = 'alex'
>>> a.index('l')
1
>>> a.index('a')#返回值是0，下标是从0开始的
0
>>> a.index('A')#找不到会报错
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ValueError: substring not found

#统计 这里用到count
>>> name = 'alex'
>>> name.count('l')#统计name中l的个数
1

#切片 这里是按照索引来切的，切片就是取一个范围
>>> word = "keep doing"
>>> word[6:10]#注意使用中括号来做取值范围，顾头不顾尾，不包含10
'oing'
>>> word[5:9]
'doin'

#检测
>>> name = "alex"#检测’l‘有没有在name里面，返回布尔值
>>> "l"in name
True
>>> num = "12345678"
>>> num.isdigit()#检测“num”是不是由整形数字组成，浮点数都不行
True
>>> name.isidentifier()#检测name是否可以被用来做变量名
True
>>> name.find("l")#检测能否从name中找到“l”,找到返回1，找不到返回-1这点和index是 有区别的
1
>>> name.find('N')
-1
>>> name.index('N')#找不到报错
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ValueError: substring not found
>>> name.index("l")
1

#大小写
>>> name = "I am alex"
>>> name.swapcase()#大小写互换
'i AM ALEX'
>>> name.capitalize()#首字母大写，其它都小写
'I am alex'
>>> name.upper()#全部大写
'I AM ALEX'
>>> name.lower()#全部小写
'i am alex'

s='hello,python'
#居中对齐
print(s.center(20,'*'))
#左对齐
print(s.ljust(20,'*'))
print(s.ljust(20))
#右对齐
print(s.rjust(20,'*'))
print(s.rjust(10))
#右对齐，使用0填充
print(s.zfill(20))

#移除空格“strip”，替换“replace”，分割“split，format格式化输出的三种玩法
>>> name = "     alex"
>>> print(name.strip())
alex
#replace 替换
>>> name='alex say :i have one tesla,my name is alex'
>>> name.replace("alex","SB",1)#替换第一个就写1
'SB say :i have one tesla,my name is alex'
>>>
#split分割
>>> name = "a*b*c*gh*kjk*"
>>> name.split('*')#以*为分割符号
['a', 'b', 'c', 'gh', 'kjk', '']
>>> name = "h*b*c*gh*gj*hkjhj"
>>> name.split('*')
['h', 'b', 'c', 'gh', 'gj', 'hkjhj']
>>>

#format的三种玩法
>>> res = '{},{},{}'.format("egon",18,"male")
>>> print(res)
egon,18,male
>>> res = "{1},{0},{1}".format("egon",18,"male")#中括号里面是索引
>>> print(res)
18,egon,18
>>> res = "{0}{1}{2}".format("egon",18,"male")
>>> print(res)
egon18male
>>> res = "{name} {age} {sex}".format(sex="male",name = "egon",age=18)
>>> print(res)
egon 18 male


1 str.startswith(prefix[,start[,end]]) #是否以prefix开头 
2 str.endswith(suffix[,start[,end]]) #以suffix结尾 
3 str.isalnum()    #是否全是字母和数字，并至少有一个字符 
4 str.isalpha()    #是否全是字母，并至少有一个字符 
5 str.isdigit()    #是否全是数字，并至少有一个字符 
6 str.isspace()    #是否全是空白字符，并至少有一个字符 
7 str.islower()    #S中的字母是否全是小写 
8 str.isupper()    #S中的字母是否便是大写 
9 str.istitle()    #S是否是首字母大写的

#以上返回时布尔值True
str.ljust(width,[fillchar])     #输出width个字符，str左对齐，不足部分用fillchar填充，默认的为空格。
 
str.rjust(width,[fillchar]) #右对齐 

str.center(width, [fillchar]) #中间对齐 

str.zfill(width) #把str变成width长，并在右对齐，不足部分用0补足

str.replace(oldstr, newstr, [count])    #把str中的oldstar替换为newstr，count为替换次数。这是替换的通用形式，还有一些函数进行特殊字符的替换 

str.strip([chars])    #把str中前后chars中有的字符全部去掉，可以理解为把str前后chars替换为None 

str.lstrip([chars])    #把str前面的去掉

str.rstrip([chars])    #把str后面的去掉

str.expandtabs([tabsize])    #把str中的tab字符替换没空格，每个tab替换为tabsize个空格，默认是8个

str.split([sep, [maxsplit]])    #以sep为分隔符，把str分成一个list。maxsplit表示分割的次数。默认的分割符为空白字符 


str.splitlines([keepends])   #把str按照行分割符分为一个list，keepends是一个bool值，如果为真每行后而会保留行分割符。
str.maketrans(from, to)    #返回一个256个字符组成的翻译表，其中from中的字符被一一对应地转换成to，所以from和to必须是等长的。 

str.translate(table[,deletechars])   # 使用上面的函数产后的翻译表，把S进行翻译，并把deletechars中有的字符删掉。需要注意的是，如果str为unicode字符串，那么就不支持 deletechars参数，可以使用把某个字符翻译为None的方式实现相同的功能。此外还可以使用codecs模块的功能来创建更加功能强大的翻译表。

# ==比较是value，is比较的内存地址
a='Python'
b='Python'
print(a==b)
print(a is b)

# 字符串的切片操作
s='hello,python'
s1=s[:5] #hello 没有指定开始位置，从0-5
s2=s[6:] #python 没有指定结束位置，从6-结束

print(s[1:5:1])#从1开始到5位置，步长为1
print(s[1:5:2])#从1开始到5位置，步长为2
print(s[::-1])# 从最后一个位置开始，到第一个元素结束，

f1=3.1415926
print(:.3f)

s='天涯共此时'
#编码
print(s.encode(encoding='GBK'))#一个中文占两个字节
print(s.encode(encoding='UTF-8'))#一个中软占三个字节
#解码
byte=s.encode(encoding='GBK')
print(byte.decode(encoding="GBK"))
by=s.encode(encoding='UTF-8')
print(by.decode(encoding="UTF-8"))

```

## 3. 列表操作

```python
# 创建列表：
lst=['hello',1,1,1.0]
lst1=list(['word',3,4.2,'list'])

# 列表的crud
inde=lst[0]



```

## 4. 字典类型

```python
# 字典类型查找效率很快,键不重复，值可以重复，无序，空间换时间
# 创建字典 1.使用花括号{}
scores={'张三':100,'李四':99,'王五':55}
# 创建字典 2.使用内置函数dict()
person=dict(name='jack',age=20)
# 空字典
emp={}
print(emp)

# 获取字典中的值
scores={'张三':100,'李四':99,'王五':55}
print(scores['张三']) # 100
print(scores.get('张三')) #100
print(scores.get('陈刘'))#NONE
# 键的判断
print('张三' in scores) #True
int('张三' not in scores) #False
# 删除操作
del scores['张三']
# scores.clear() #清空字典
scores['马刘']=98 # 新增元素

#获取字典的键
keys=scores.keys()
print(keys)
print(list(keys))#转换成列表

#获取所有的值
values=scores.values()
print(values)
print(list(values))#转换成列表

# 字典的遍历
for item in scores:
    print(item,socres[item],socres.get(item))

# 字典的生成
items=['Fruits,'Books','Others']
prices=[99,98,89]
d={item:price for item,price in zip(items,prices)}
print(d)
```

## 5.元组类型

```python
# 元组创建方式
# 1.直接使用小括号
t=('Python','hello',90)
# 2.使用内置函数tuple()
t=tuple(('Python','hello',90))
# 3.只包含一个元组的元素需要使用逗号和小括号
t=(10,)

#遍历元组
for item in t:
    print(item)
```

## 6.集合类型

集合的元素不允许重复

```python
# 集合的创建
# 1.使用花括号{}
s={'python' ,'java'}
# 2.使用set()
s=set(range(6))
print(s)
s=set(1,2,2,3)
print(s)#{1,2,4}
#定义空集合
s=set()

#判断操作
s={10,20,30,405,60}
print(10 in s)# True
print(100 in s)#False
#集合新增
s.add(1000)#一次添加一个元素
print(s)#{10,20,30,405,60,100}
s.update({200,3000,400})#一次至少添加一个元素
s.update((200,3000,400))
#删除操作 s.remove(500) KeyError: 500
s.remove(200)
s.discard(500)
s.discard(300)
s.clear() #清空集合

#判断两个集合是否相等
s={1,2,3,4}
s2={4,3,2,1}
print(s==s2)#True
print(s!=s2)#False
#子集
s1={10,20,30,40,50}
s2={10,20}
s3={10,44}
print(s2.issubset(s1)) #True
print(s3.issubset(s1))#False
# 超集
print(s1.issuperset(s2))#True
print(s1.issuperset(s3))#False
#交集
print(s2.isdisjoint(s3))#True
print(s1.intersection(s2))#{10,20}
print(s1 & s2)#{10,20} &与intersection()等价

#并集
print(s1.union(s2))
print(s1 | s2)
#差集
print(s1.difference(s2))#{30,40,50}
print(s1-s2)
#对称差集
print（s1.symmetric_difference(s2))
print(s1^ s1)
#集合生成式
se={i*i for i in range(10)}
print(se)
```

## 7.函数操作

```python
def calc(a,b):
    c=a+b
    return c
#调用函数
res=cale(10,20)
print(res)#30






```



