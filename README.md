# VeighNa框架的天勤TQSDK数据服务接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-3.8.2.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.10|3.11|3.12|3.13-blue.svg"/>
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于天勤TQSDK模块的3.8.2版本开发，支持以下中国金融市场的K线数据：

* 期货：
  * CFFEX：中国金融期货交易所
  * SHFE：上海期货交易所
  * DCE：大连商品交易所
  * CZCE：郑州商品交易所
  * INE：上海国际能源交易中心
* 股票：
  * SSE：上海证券交易所
  * SZSE：深圳证券交易所

注意：需要使用相应的数据服务权限，可以通过[该页面](https://www.shinnytech.com/tqsdk_professional/)申请试用或者付费购买。


## 安装

安装环境推荐基于4.0.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com)】。

直接使用pip命令：

```
pip install vnpy_tqsdk
```


或者下载源代码后，解压后在cmd中运行：

```
pip install .
```


## 使用

在VeighNa中使用天勤TQSDK时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|datafeed.name|名称|是|tqsdk|
|datafeed.username|用户名|是|test|
|datafeed.password|密码|是|12345678|
