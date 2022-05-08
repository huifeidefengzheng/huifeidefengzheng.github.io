---
title: mysql--常用的日期函数
date: 2019-09-01 11:10:46
tags: mysql
categories: mysql
---

# mysql--常用的日期函数

now()                  2019-08-08 17:09:45      获取**当前**日期和时间      

Date(date)                                                  获取**指定日期**

CURDATE()          2019-08-08                    获取**当前日期**

CURTIME()          17:17:19                         获取**当前时间**

(日期/时间转换为字符串）函数：

```properties
date_format(date,format), time_format(time,format)

select date_format('2008-08-08 22:23:01', '%Y%m%d%H%i%s')

20080808222301
```



（字符串转换为日期）函数：str_to_date(str, format)

```properties
select str_to_date('08/09/2008', '%m/%d/%Y'); -- 2008-08-09

select str_to_date('08/09/08' , '%m/%d/%y'); -- 2008-08-09

select str_to_date('08.09.2008', '%m.%d.%Y'); -- 2008-08-09

select str_to_date('08:09:30', '%h:%i:%s'); -- 08:09:30
```

（时间、秒）转换函数：time_to_sec(time), sec_to_time(seconds)

```properties
select time_to_sec('01:00:05'); -- 3605

select sec_to_time(3605); -- '01:00:05'
```



日期、时间相减函数：datediff(date1,date2), timediff(time1,time2)

datediff(date1,date2)：两个日期相减 date1 - date2，返回天数。

```properties
select datediff('2008-08-08', '2008-08-01');      -- 7

select datediff('2008-08-01', '2008-08-08');      -- -7
```

 								timediff(time1,time2)：两个日期相减 time1 - time2，返回 time 差值。

```properties
select timediff('2008-08-08 08:08:08', '2008-08-08 00:00:00'); -- 08:08:08

select timediff('08:08:08', '00:00:00'); -- 08:08:08
```

注意：timediff(time1,time2) 函数的两个参数类型必须相同。

DAYOFWEEK(date)

​     SELECT DAYOFWEEK(‘2016-01-16')

​     SELECT DAYOFWEEK(‘2016-01-16 00:00:00')

​     -> 7 (表示返回日期date是星期几，记住：星期天=1，星期一=2， ... 星期六=7)

WEEKDAY(date)

​     SELECT WEEKDAY(‘2016-01-16')

​     SELECT WEEKDAY(‘2016-01-16 00:00:00')

​     -> 5     (星期一=0,星期二=1...星期日=6)

DAYOFMONTH(date)

​     SELECT DAYOFMONTH(‘2016-01-16')

​     SELECT DAYOFMONTH(‘2016-01-16 00:00:00')

​     -> 16 (表示返回date是当月的第几天，1号就返回1，... ,31号就返回31)

DAYOFYEAR(date)

​     SELECT DAYOFYEAR(‘2016-03-31')

​     SELECT DAYOFYEAR(‘2016-03-31 00:00:00')

​     -> 91 (表示返回date是当年的第几天，01.01返回1，... ,12.31就返回365)

MONTH(date)

​     SELECT MONTH(‘2016-01-16')

​     SELECT MONTH(‘2016-01-16 00:00:00')

​     -> 1 (表示返回date是当年的第几月，1月就返回1，... ,12月就返回12)

DAYNAME(date)

​     SELECT DAYNAME(‘2016-01-16')

​     SELECT DAYNAME(‘2016-01-16 00:00:00')

​     -> Saturday (表示返回date是周几的英文全称名字)

MONTHNAME(date)

​     SELECT MONTHNAME(‘2016-01-16')

​     SELECT MONTHNAME(‘2016-01-16 00:00:00')

​     -> January (表示返回date的是当年第几月的英文名字)

QUARTER(date)

​     SELECT QUARTER(‘2016-01-16')

​     SELECT QUARTER(‘2016-01-16 00:00:00')

​     -> 1 (表示返回date的是当年的第几个季度，返回1,2,3,4)

WEEK(date，index)

​     SELECT WEEK(‘2016-01-03')

​     SELECT WEEK(‘2016-01-03', 0)

​     SELECT WEEK(‘2016-01-03', 1)

​     -> 1 (该函数返回date在一年当中的第几周，date(01.03)是周日，默认是以为周日作为一周的第一天，函数在此处返回1可以有两种理解：1、第一周返回0，第二周返回1，.... ,2、以当年的完整周开始计数，第一周返回1，第二周返回2，... ，最后一周返回53)

​     -> 1 (week()默认index就是0. 所以结果同上)

​     -> 0 (当index为1时，表示一周的第一天是周一，所以，4号周一才是第二周的开始日)

YEAR(date)

​     SELECT YEAR(‘70-01-16')

​     SELECT YEAR(‘2070-01-16')

​     SELECT YEAR(‘69-01-16 00:00:00')

​     -> 1970 (表示返回date的4位数年份)

​     -> 2070

​     -> 1969

注意的是：如果年份只有两位数，那么自动补全的机制是以默认时间1970.01.01为界限的，>= 70 的补全 19，< 70 的补全 20



HOUR(time)

​     SELECT HOUR(‘11:22:33')

​     SELECT HOUR(‘2016-01-16 11:22:33')

​    -> 11

​     -> 11 

返回该date或者time的hour值，值范围（0-23）



MINUTE(time)

​     SELECT MINUTE(‘11:22:33')

​     SELECT MINUTE(‘2016-01-16 11:44:33')

​     -> 22

​     -> 44

返回该time的minute值，值范围（0-59



SECOND(time)

​     SELECT SECOND(‘11:22:33')

​     SELECT SECOND(‘2016-01-16 11:44:22')

​     -> 33

​     -> 22

返回该time的minute值，值范围（0-59）



PERIOD_ADD(month，add)

​     SELECT PERIOD_ADD(1601,2)

​     SELECT PERIOD_ADD(191602,3)

​     SELECT PERIOD_ADD(191602,-3)

​     -> 201603

​     -> 191605

​     -> 191511

该函数返回对month做增减的操作结果，month的格式为yyMM或者yyyyMM,返回的都是yyyyMM格式的结果，add可以传负值



PERIOD_DIFF(monthStart，monthEnd)

​     SELECT PERIOD_DIFF(1601,1603)

​     SELECT PERIOD_DIFF(191602,191607)

​     SELECT PERIOD_DIFF(1916-02,1916-07)

​     SELECT PERIOD_DIFF(1602,9002)

​     -> -2

​     -> -5

​     -> 5

​     -> 312

该函数返回monthStart - monthEnd的间隔月数

DATE_ADD(date，INTERVAL number type)，同 ADDDATE()

SELECT DATE_ADD(“2015-12-31 23:59:59”,INTERVAL 1 SECOND)

SELECT DATE_ADD(“2015-12-31 23:59:59”,INTERVAL 1 DAY)

SELECT DATE_ADD(“2015-12-31 23:59:59”,INTERVAL “1:1” MINUTE_SECOND)

SELECT DATE_ADD(“2016-01-01 00:00:00”,INTERVAL “-1 10” DAY_HOUR)

-> 2016-01-01 00:00:00

-> 2016-01-01 23:59:59

-> 2016-01-01 00:01:00

-> 2015-12-30 14:00:00

为日期增加(减少)一个时间间隔：date_add()

set @dt = now();

select date_add(@dt, interval 1 day); -- add 1 day

select date_add(@dt, interval 1 hour); -- add 1 hour

select date_add(@dt, interval 1 minute); --...

select date_add(@dt, interval 1 second);

select date_add(@dt, interval 1 microsecond);

select date_add(@dt, interval 1 week);

select date_add(@dt, interval 1 month);

select date_add(@dt, interval 1 quarter);

select date_add(@dt, interval 1 year);

select date_add(@dt, interval -1 day); -- sub 1 day

为日期增加一个时间间隔

adddate(), addtime()函数，可以用 date_add() 来替代。下面是 date_add() 实现 addtime() 功能示例：

mysql> set @dt = '2008-08-09 12:12:33';

mysql> select date_add(@dt, interval '01:15:30' hour_second); 2008-08-09 13:28:03

mysql> select date_add(@dt, interval '1 01:15:30' day_second); 2008-08-10 13:28:03

为日期减去一个时间间隔：date_sub()

select date_sub('1998-01-01 00:00:00', interval '1 1:1:1' day_second); 1997-12-30 22:58:59



## 数学函数

abs(X)               返回x的**绝对值**

sqrt(X)               返回x的**非-2次方根**

mod(x,y)          返回x/y后的**余数**

ceiling(x)          返回不小于x的**最小整数(向上取整)**

floor(x)            返回不大于x的**最大整数(向下取整)**

round(x,y)            对x进行**四舍五入**操作,小数点后保留y位

truncate(x,y)          舍去x中小数点y位后面的数

sign(x)               返回x的符号,-1,0或1

ceil(2.44)          3,**向上取整**

round(2.44)          2,**四舍五入取整**

## 字符串函数

length(str)               返回字符串str的**长度**

concat(s1,s2,...)          返回**字符串拼接**后的新字符串

trim(str)                    **删除**字符串两侧的**空格**

replace(str,s1,s2)          使用s2**替换**str中的s1

substring(str,n,len)          返回str的**子串**,从n开始,长度为len

reverse(str)               字符串**反转**

locate(s1,str)          返回子串s1在str中的**起始位置**

**left(str,int)          左截取**

RIGHT(str,int)     函数从指定字符串的右侧提取给定数量的字符。 例如，RIGHT('SQL Server'，6)返回：'Server'。

## 条件函数

if(expr,v1,v2)     如果expr表达式为true返回v1,否则返回v2

ifnull(v1,v2)          如果v1不为null,返回v1,否则返回v2

(case expr      when     v1      then      r1

​                        when     v2     then      r2

​                    ...

​                    else  r

​                    end)

## 加密函数

md5(str)          					 对字符串str进行MD5加密

encode(str,pwd_str)           使用pwd作为密码对str进行加密

decode(str,pwd_str )          使用pwd作为密码对str进行解密