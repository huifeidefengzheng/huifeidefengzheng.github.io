# 第四天BootStrap

### 0.1学习目标

（1）能够独立编写并使用BootStrap栅格系统

（2）能够独立编写并使用BootStrap提供的样式

（3）能够独立完成并使用BootStrap综合案例

### 0.2 学习指南

1\. BootStrap的作用

2\. BootStrap环境搭建（重点）

3\. BootStrap栅格系统（重点）

4\. BootStrap提供的样式(重点)

5\. BootStrap综合案例

## 1、bootstrap概述

### 1.1、什么是bootstrap？bootstrap的作用？

> Bootstrap，基于 HTML、CSS、JAVASCRIPT 的**前端框架**。
>
> 该框架已经预定义了一套CSS样式和与样式对应的JS代码。（对应的样式有对应的特效）
>
> 开发人员只需要编写HTML结构，添加bootstrap固定的class样式，就可以轻松完成指定效果的实现。
>
> 作用：

1.  Bootstrap 使得 Web 开发更加快捷，高效。

2.  BootStrap支持响应式开发，解决了移动互联网前端开发问题

+---------------------------------------------------------------------------------------------------------------------------------------+
| 课外了解知识：                                                                                                                        |
|                                                                                                                                       |
| 该框架由Twitter 公司的设计师Mark Otto和Jacob Thornton合作开发。                                                                       |
|                                                                                                                                       |
| Bootstrap基础入门使用的都是自带CSS样式，高级开发中需要使用HTML5、CSS3、动态CSS语言Less 进行自定义开发，JavaEE课程中学习是"基础入门"。 |
|                                                                                                                                       |
| 中文官网：http://www.bootcss.com/                                                                                                     |
+---------------------------------------------------------------------------------------------------------------------------------------+

### 1.2、什么是响应式布局？响应式布局解决的问题？

-   响应式布局：一个网站的展示能够兼容多个终端(手机、iPad、PC等)，而不需要为每个终端单独做一个展示版本。

-   此概念专为解决移动互联网浏览而诞生的。

> 响应式布局，使得网站仅适用一套样式，就可以在不同分辨率下展示出不同的舒适效果，大大降低了网站开发维护成本，并且能带给用户更好的体验性
>
> 未使用响应式开发：
>
> ![](javaee-day12-BootStrap/image3.png){width="5.768055555555556in" height="2.6590277777777778in"}
>
> 使用了响应式开发：
>
> ![](javaee-day12-BootStrap/image4.png){width="5.768055555555556in" height="2.6354166666666665in"}

## 2、bootstrap环境搭建

### 2.1、下载资源

-   中文官网地址：http://d.bootcss.com/bootstrap-3.3.5.zip

![](javaee-day12-BootStrap/image5.png){width="5.992361111111111in" height="1.2090277777777778in"}

### 2.2、目录结构

![](javaee-day12-BootStrap/image6.png){width="4.1715277777777775in" height="1.8659722222222221in"}

Javaee使用bootStrap开发，主要使用dist发布版目录。

dist目录结构如下：

> ![](javaee-day12-BootStrap/image7.png){width="2.9402777777777778in" height="1.26875in"}
>
> bootstrap/
>
> ├── css/
>
> │ ├── bootstrap.css //bootstrap完整版的CSS文件。
>
> │ ├── bootstrap.css.map
>
> │ ├── **bootstrap.min.css** //bootstrap压缩版CSS文件。
>
> │ ├── bootstrap.min.css.map/
>
> │ ├── bootstrap-theme.css //主题文件
>
> │ ├── bootstrap-theme.css.map
>
> │ ├── bootstrap-theme.min.css
>
> │ └── bootstrap-theme.min.css.map
>
> ├── js/
>
> │ ├── bootstrap.js // bootstrap完整版的脚本文件。
>
> │ └── **bootstrap.min.js** // bootstrap压缩版的脚本文件。
>
> └── fonts/
>
> ├── glyphicons-halflings-regular.eot //字体 （字体图标）
>
> ├── glyphicons-halflings-regular.svg
>
> ├── glyphicons-halflings-regular.ttf
>
> ├── glyphicons-halflings-regular.woff
>
> └── glyphicons-halflings-regular.woff2

注：完整版用于源码学习，但因为文件大小问题，不适合网络间传递。

压缩版用于网络发布，压缩版和完整版的唯一区别，仅为压缩版将代码间的大量空格和回车换行删除掉了，节约了大量的空间，功能上完全相同，适用于网络间快速传递。但因为没有了空格和换行，源代码难以阅读。

### 2.3、bootstrap的通用简洁模板

```html
<!DOCTYPE html>
<html lang="zh-CN">
	<head>
		<meta charset="utf-8">
		<meta http-equiv="X-UA-Compatible" content="IE=edge">
		<meta name="viewport" content="width=device-width,initial-scale=1" />
		<title>Bootstrap 模板</title>

		<link href="../lib/bootstrap/css/bootstrap.min.css" rel="stylesheet">
		<script src="../lib/jquery/jquery-1.11.0.js"></script>
		<script src="../lib/bootstrap/js/bootstrap.min.js"></script>

	</head>

	<body>
		<h1>你好，世界！</h1>
	</body>
</html>
```

viewport：视口，即浏览器上网页的可视区域

![](javaee-day12-BootStrap/image8.png){width="2.8833333333333333in" height="2.261111111111111in"}

视口作用：用于**移动设备**将 大型页面进行比例缩放显示。

视口的常见设置（了解）：

| width=device-width | 视口的宽度，大多手机浏览器视口的宽度是980。device-width 表示采用设备的宽度例如：手机是5.5寸，那么视口也采用5.5寸宽度 |
| ------------------ | ------------------------------------------------------------ |
| initial-scale=1    | 移动设备上，打开页面时的初始化缩放级别。取值：1-51表示100%，5表示500% |
| minimum-scale=1    | 移动设备上，页面可以 最小缩放的级别。                        |
| maximum-scale=1    | 移动设备上，页面可以 最大缩放的级别。                        |
| user-scalable=no   | 移动设备上，页面禁止缩放。如果设置“user-scalable=no”，则“minimum-scale”和“maximum-scale”无效 |

## 3、布局容器

BootStrap必须需要至少一个布局容器，才能为页面内容进行封装和方便的样式控制。

相当于一个画板。

帮助手册位置：全局CSS样式\-\-\-\-\-\--》概览\-\-\-\-\-\--》布局容器

任意元素使用了布局容器的样式，都会成为一个布局容器，建议使用div作为布局容器

| .container       | 类用于固定宽度并支持响应式布局的容器。【特点：居中，两端留白】<div class="container"> ... </div> |
| ---------------- | ------------------------------------------------------------ |
| .container-fluid | 类用于 100% 宽度，占据全部视口（viewport）的容器。<div class="container-fluid"> ... </div> |

为了展示效果明显，我们为div加入了边框样式：

style=\"border:1px solid red;\"

示例1：

![](javaee-day12-BootStrap/image9.png){width="5.768055555555556in" height="0.7506944444444444in"}

效果1：

![](javaee-day12-BootStrap/image10.png){width="5.768055555555556in" height="0.27847222222222223in"}

示例2：

![](javaee-day12-BootStrap/image11.png){width="5.768055555555556in" height="0.6465277777777778in"}

效果2：

![](javaee-day12-BootStrap/image12.png){width="5.768055555555556in" height="0.23194444444444445in"}

## 4、栅格系统

### 4.1、简述栅格系统

为了方便在布局容器中进行网页的布局操作。

BootStrap提供了一套专门用于响应式开发布局的栅格系统。

栅格系统将一行分为**12列**，通过设定元素占用的列数来 布局元素在页面上的展示位置。

帮助手册位置：全局CSS样式\-\-\-\--栅格系统

![](javaee-day12-BootStrap/image13.png){width="6.911111111111111in" height="1.2458333333333333in"}

作用：

可以让开发人员更加轻松进行网页布局，并且轻松进行响应式开发。

### 4.2、栅格系统的特点及入门案例

-   栅格特点

    -   "行（row）"必须包含在 **.container** （固定宽度）或 .container-fluid （100% 宽度）中

    -   行使用的样式"**.row**"，列使用样式"**col-\*-\***" 元素内容应当放置于"列（column）"内

    -   基本的书写方式必须是：**容器\-\--行\-\--列\-\--内容**

> HTML表格：定义一个表格\-\-\--行\-\-\-\--单元格

-   栅格参数："**col-屏幕尺寸-占用列数**"

> 列元素的书写顺序，决定布局顺序，先写的列元素会被先布局到行上。
>
> 列元素的占用列数，定义列元素的大小

为了方便显示元素大小，我们为展示元素都赋予了相同样式：

border:1px solid red;height:100px;

示例1：一个元素占一行

![](javaee-day12-BootStrap/image14.png){width="5.768055555555556in" height="0.6180555555555556in"}

效果1：

![](javaee-day12-BootStrap/image15.png){width="5.768055555555556in" height="0.4861111111111111in"}

示例2：两个元素占一行

![](javaee-day12-BootStrap/image16.png){width="5.768055555555556in" height="0.7694444444444445in"}

效果2：

![](javaee-day12-BootStrap/image17.png){width="5.768055555555556in" height="0.5451388888888888in"}

示例3：三个元素占一行

![](javaee-day12-BootStrap/image18.png){width="5.768055555555556in" height="0.8951388888888889in"}

效果3：

![](javaee-day12-BootStrap/image19.png){width="5.768055555555556in" height="0.5527777777777778in"}

示例4：四个元素占一行

![](javaee-day12-BootStrap/image20.png){width="5.768055555555556in" height="1.0444444444444445in"}

效果4：

![](javaee-day12-BootStrap/image21.png){width="5.768055555555556in" height="0.5673611111111111in"}

注：

-   一个row下，如果设置的col列数总和小于等于12，那么该row下元素在一行排列；

-   一个row下，如果设置的col列数总和大于12，那么超出的元素会另起一行排列；

-   行和列可以进行无限嵌套，嵌套方式必须为 列\-\--行\-\--列\-\-\--行。。。。

-   一个row元素下，有12列的

### 4.3、栅格屏幕尺寸设置

![](javaee-day12-BootStrap/image22.png){width="6.358333333333333in" height="2.798611111111111in"}

屏幕尺寸简述：

-   large : lg \-\-\-\-\-\--大屏幕，一般PC尺寸

-   medium : md \-\-\-\-\-\-\--中等屏幕，小PC尺寸

-   small: sm : sm \-\-\-\--小屏幕 ，iPad尺寸

-   x small : xs \-\-\-\--超小屏幕，智能手机尺寸

为了方便显示元素大小，我们为展示元素都赋予了相同样式：

border:1px solid red;height:100px;

示例：

![](javaee-day12-BootStrap/image23.png){width="5.768055555555556in" height="0.7333333333333333in"}

效果：

![](javaee-day12-BootStrap/image24.png){width="5.768055555555556in" height="0.7465277777777778in"}

![](javaee-day12-BootStrap/image25.png){width="5.768055555555556in" height="0.9076388888888889in"}

![](javaee-day12-BootStrap/image26.png){width="5.768055555555556in" height="2.285416666666667in"}

### 4.4、设置屏幕尺寸时的注意事项

若设置了某个屏幕尺寸的样式，那么比该尺寸大的屏幕，会沿用该设置；比该尺寸小的屏幕，会默认一个元素占12列的设置。

例如：设置了col-md-4，那么相当于也设置了col-lg-4。

其他屏幕尺寸均默认为col-sm-12,col-xs-12

### 4.5、列偏移

通常情况下我们需要将元素居中显示，需要左边空出一定的空白区域，这里我们就可以使用列偏移来达到效果。

------------------------- ----------------------------------
  .col-屏幕尺寸-offset-\*   在指定屏幕尺寸下，向右偏移\*个列
------------------------- ----------------------------------

## 5、响应式工具

为针对性地在移动页面上展示和隐藏不同的内容，bootStrap提供响应式工具。

可以让开发人员通过该工具决定，在何种屏幕尺寸下，隐藏或者显示某些元素

帮助手册位置：全局CSS样式\-\--响应式工具

![](javaee-day12-BootStrap/image27.png){width="5.768055555555556in" height="2.4027777777777777in"}

## 6、列表

BootStrap同样提供了实用的列表样式供开发人员使用。

帮助手册位置：全局CSS样式\-\-\--排版\-\-\--列表

-------------- --------------------------
  .list-inline   将列表所有元素放置于一行
-------------- --------------------------

示例：

![](javaee-day12-BootStrap/image28.png){width="3.1347222222222224in" height="1.113888888888889in"}

效果：

![](javaee-day12-BootStrap/image29.png){width="5.768055555555556in" height="0.3506944444444444in"}

## 7、按钮

BootStrap提供了丰富的按钮样式供开发人员使用。

帮助手册位置：全局CSS样式\-\-\--按钮

**任何HTML元素加上以下样式都会变成对应按钮**

| .btn btn-default | 示例：<a class="btn btn-default">Link</a>效果：![img](javaee-day12-BootStrap/wps44.jpg) |
| ---------------- | ------------------------------------------------------------ |
| .btn btn-primary | 示例：<a class="btn btn-primary">（首选项）Primary</a>效果：![img](javaee-day12-BootStrap/wps45.jpg) |
| .btn btn-success | 示例：<a class="btn btn-success">（成功）Success</a>效果：![img](javaee-day12-BootStrap/wps46.jpg) |
| .btn btn-info    | 示例：<a class="btn btn-info">（一般信息）Info</a>效果： ![img](javaee-day12-BootStrap/wps47.jpg) |
| .btn btn-warning | 示例：<a class="btn btn-warning">（警告）Warning</a>效果： ![img](javaee-day12-BootStrap/wps48.jpg) |
| .btn btn-danger  | 示例：<a class="btn btn-danger">（危险）Danger</a>效果： ![img](javaee-day12-BootStrap/wps49.jpg) |
| .active          | 表示按钮被点击的样式示例：<a class=”btn btn-danger active”>（危险）Danger</a>效果：![img](javaee-day12-BootStrap/wps50.jpg) |
| .disabled        | 表示按钮被禁用的样式示例：<a class=”btn btn-danger disabled”>（危险）Danger</a>效果：![img](javaee-day12-BootStrap/wps51.jpg) |



## 8、导航条

BootStrap已经提供了完整的导航条实例，通常情况下，我们仅需进行简单修改即可使用。

帮助手册位置：组件\-\-\-\-\-\--导航条

![](javaee-day12-BootStrap/image38.png){width="5.768055555555556in" height="0.2625in"}

## 9、轮播图

BootStrap已经提供了完整的轮播图实例，通常情况下，我们仅需进行简单修改即可使用。

帮助手册位置：JavaScript插件\-\-- Carousel

轮播图DIV的定时换图属性：

data-interval=\"毫秒值\"

注意：**多个轮播图必须修改轮播图的ID**。

## 10、排版-对齐方式

BootStrap提供统一的排版方式设置，方便开发人员对内容板式进行调整

帮助手册位置：全局CSS样式\-\-\--排版\-\-\--对齐

会将元素内所有的内容都进行排版设置

-------------- --------------------
  .text-left     使元素内容靠左显示
  .text-center   使元素内容居中显示
  .text-right    使元素内容靠右显示

-------------- --------------------

示例：

![](javaee-day12-BootStrap/image39.png){width="3.2284722222222224in" height="0.7180555555555556in"}

效果：

![](javaee-day12-BootStrap/image40.png){width="5.768055555555556in" height="1.2055555555555555in"}

## 11、表单元素

BootStrap同样提供了丰富的表单控件供开发人员来选择。

帮助手册位置：全局CSS样式\-\-\-\--表单

示例1：基本实例

![](javaee-day12-BootStrap/image41.png){width="5.768055555555556in" height="1.5215277777777778in"}

效果1：

![](javaee-day12-BootStrap/image42.png){width="5.768055555555556in" height="0.40208333333333335in"}

示例2：表单名和表单输入项共用一行

![](javaee-day12-BootStrap/image43.png){width="5.768055555555556in" height="1.8791666666666667in"}

效果2：

![](javaee-day12-BootStrap/image44.png){width="5.768055555555556in" height="0.5770833333333333in"}

示例3：校验状态-出错样式

![](javaee-day12-BootStrap/image45.png){width="5.768055555555556in" height="0.8930555555555556in"}

效果3：

![](javaee-day12-BootStrap/image46.png){width="5.768055555555556in" height="0.40208333333333335in"}

## 12、分页条

![](javaee-day12-BootStrap/image47.png){width="6.009722222222222in" height="3.0444444444444443in"}

BootStrap为我们还准备了分页条的样式组件。

帮助手册位置：组件\-\-\-\-\-\-\-\-\-\--分页

示例1：

![](javaee-day12-BootStrap/image48.png){width="2.390972222222222in" height="1.9791666666666667in"}

效果1：

![](javaee-day12-BootStrap/image49.png){width="2.488888888888889in" height="0.5930555555555556in"}

示例2：

![](javaee-day12-BootStrap/image50.png){width="3.0180555555555557in" height="2.5555555555555554in"}

效果2：（鼠标放入禁用按钮，鼠标变为禁用）

![](javaee-day12-BootStrap/image51.png){width="2.5409722222222224in" height="0.5618055555555556in"}

## 13、综合案例

### 13.1、案例需求

![](javaee-day12-BootStrap/image52.jpeg){width="6.216666666666667in" height="3.0145833333333334in"}

请做出如图所示的页面。

要求：

1.  页面顶部的三部分在PC屏幕上显示为一行，在移动设备屏幕上显示为一部分一行；

2.  导航条在大屏幕展示全部内容，在移动设备上需要将内容能够折叠/展开；

3.  用户名/密码/确认密码不能为空，密码需和确认密码一致，如果不符合，阻止注册操作，并将错误信息展示给用户看。

> onsubmit

### 13.2、需求分析

![](javaee-day12-BootStrap/image53.png){width="5.768055555555556in" height="3.145138888888889in"}

### 13.3、案例实现

```html
<script>
    
    	//密码和确认密码一致性校验
    	//前提：密码和确认密码必须填写
    	function checkPwdAndRepwd(f1,f2){
    		if(f1&&f2){
    			//密码和确认密码不为空，进行非空校验
    			//1、密码和确认密码 值拿到
    			var pwd=document.getElementById("password").value;
    			var repwd=document.getElementById("repassword").value;
    			var msg=document.getElementById("repasswordMsg");
    			var div=document.getElementById("repasswordDiv");
    			//2、一致性判断
    			if(pwd==repwd){
    				div.className="form-group";
    			  msg.innerHTML="";
    				return true;
    			}else{
    				div.className+=" has-error";
    			  msg.innerHTML="必须和密码一致";
    				return false;
    			}
    		}else{
    			//密码和确认密码有一个为空，直接返回false
    			return false;
    		}
    	}
    
      //非空校验
    	function checkNotNull(nid){
    		//1、获取表单输入项 元素对象
    		var nodex=document.getElementById(nid);
    		//获取对应的错误信息回显 label元素
    		var msg=document.getElementById(nid+"Msg");
    		//获取对应的DIV
    		var div=document.getElementById(nid+"Div");
    		//2、对进行非空判断
    		var reg = /^\s*$/;//如果有0~多个空白符，就为true
    		if(reg.test(nodex.value)){
    			//信息不合格
    			div.className+=" has-error";
    			msg.innerHTML="不能为空";
    			return false;
    		}else{
    			//信息合格
    			div.className="form-group";
    			msg.innerHTML="";
    			return true;
    		}
    	}
    
    	//表单校验方法
    	function checkForm(){
    		//用户名
    		var flag1=checkNotNull("username");
    		//密码
    		var flag2=checkNotNull("password");
    		//确认密码
    		var flag3=checkNotNull("repassword");
    		//一致性校验
    		var flag4=checkPwdAndRepwd(flag2,flag3);
    		return flag1&&flag2&&flag3&&flag4;
    	}
    </script>
  </head>
  <body>
		   <div class="container">
		   	 <!-- 网站头部 -->
		   	 <div class="row">
		   	 	<div class="col-md-4">
		   	 		<img src="../img/logo2.png" />
		   	 	</div>
		   	 	<div class="col-md-4">
		   	 		<img src="../img/header.png" />
		   	 	</div>
		   	 	<div class="col-md-4">
		   	 		<ul class="list-inline" style="margin-top:10px;">
		   	 			<li><a href="" class="btn btn-primary">登录</a></li>
		   	 			<li><a href="" class="btn btn-primary">注册</a></li>
		   	 			<li><a href="" class="btn btn-danger">购物车</a></li>
		   	 		</ul>
		   	 	</div>
		   	 </div>
		   	 <!--  导航条 -->
		   	 <nav class="navbar navbar-inverse">
				  <div class="container-fluid">
				    <div class="navbar-header">
				      <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1" aria-expanded="false">
				        <span class="sr-only">Toggle navigation</span>
				        <span class="icon-bar"></span>
				        <span class="icon-bar"></span>
				        <span class="icon-bar"></span>
				      </button>
				      <a class="navbar-brand" href="#">首页</a>
				    </div>
				
				    <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
				      <ul class="nav navbar-nav">
				        <li class="active"><a href="#">手机数码<span class="sr-only">(current)</span></a></li>
				        <li><a href="#">电脑办公</a></li>
				        <li class="dropdown">
				          <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">更多<span class="caret"></span></a>
				          <ul class="dropdown-menu">
				            <li><a href="#">母婴用品</a></li>
				            <li><a href="#">汽车配件</a></li>
				            <li role="separator" class="divider"></li>
				          </ul>
				        </li>
				      </ul>
				      <form class="navbar-form navbar-right" role="search">
				        <div class="form-group">
				          <input type="text" class="form-control" placeholder="Search">
				        </div>
				        <button type="submit" class="btn btn-default">Submit</button>
				      </form>
				    </div>
				  </div>
					</nav>
		   	 <!-- 注册页面主体-->
		   	 <div class="row" style="background-image: url(../img/regist_bg.jpg)">
		   	 	<div class="col-sm-8 col-sm-offset-2" style="border:5px solid gainsboro;background-color:white;">
		   	 		<!-- 表单部分 -->
		   	 		<div class="row">
		   	 			<div class="col-sm-8 col-sm-offset-2">
		   	 				<font color="#204D74" size="4">会员注册</font>
		   	 			</div>
		   	 		</div>
		   	 		<form class="form-horizontal" onsubmit="return checkForm()">
						  <div id="usernameDiv" class="form-group">
						    <label class="col-sm-2 control-label">用户名</label>
						    <div class="col-sm-8">
						      <input type="text" class="form-control" id="username" name="username" placeholder="请输入用户名">
						    </div>
						    <label id="usernameMsg" class="col-sm-2 control-label"></label>
						  </div>
						  
						  <div id="passwordDiv" class="form-group">
						    <label class="col-sm-2 control-label">密码</label>
						    <div class="col-sm-8">
						      <input type="password" class="form-control" id="password" name="password" placeholder="请输入密码">
						    </div>
						    <label id="passwordMsg" class="col-sm-2 control-label"></label>
						  </div>
						  <div id="repasswordDiv" class="form-group">
						    <label class="col-sm-2 control-label">确认密码</label>
						    <div class="col-sm-8">
						      <input type="password" class="form-control" id="repassword" placeholder="请输入确认密码">
						    </div>
						    <label id="repasswordMsg" class="col-sm-2 control-label"></label>
						  </div>
						  <div class="form-group">
						    <label class="col-sm-2 control-label">email</label>
						    <div class="col-sm-8">
						      <input type="text" class="form-control" id="email" name="email" placeholder="请输入email">
						    </div>
						  </div>
						  <div class="form-group">
						    <label class="col-sm-2 control-label">姓名</label>
						    <div class="col-sm-8">
						      <input type="text" class="form-control" id="name" name="name" placeholder="请输入姓名">
						    </div>
						  </div>
						  <div class="form-group">
						    <label class="col-sm-2 control-label">性别</label>
						    <div class="col-sm-8">
						      <input type="radio" name="sex" value="man" checked="checked"/>男
						      <input type="radio" name="sex" value="woman"/>女
						    </div>
						  </div>
						  <div class="form-group">
						    <div class="col-sm-offset-2 col-sm-10">
						      <input type="submit" class="btn btn-danger btn-lg" value=" 注 册 "/>
						    </div>
						  </div>
						</form>
		   	 	</div>
		   	 </div>
		   	 <!-- 网站底部 -->
		   	 <div class="row">
		   	 	<div class="col-xs-12">
		   	 		<img src="../img/footer.jpg" width="100%"/>	
		   	 	</div>
		   	 </div>
		   	 <div class="row">
		   	 	<div class="col-xs-12 text-center">
		   	 		<ul class="list-inline">
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 			<li><a href="">联系我们</a></li>
		   	 		</ul>
		   	 	</div>
		   	 </div>
		   	 <div class="row">
		   	 	<div class="col-xs-12 text-center">
		   	 		Copyright © 2005-2020 黑马商城 版权所有 
		   	 	</div>
		   	 </div>
		   </div>
  </body>
```

