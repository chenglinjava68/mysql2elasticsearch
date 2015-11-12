# mysql2elasticsearch

mysql 数据导入 elasticsearch

## 说明

将mysql数据导入elasticsearch

采用两种方式

1. 全量导表
2. 根据binlog将增量数据进行导入

### 全表导

根据某个id进行for 一批一批的采用es bulk的方式快速导入es

需注意的是mysql中某些字段是二进制的

可以自定义配置文件里的selectstr 用hex等方式转化

### 根据binlog导数据

采用了[python-mysql-replication](https://github.com/noplay/python-mysql-replication) 获取binlog增量数据

导入kafka （将binlog导入kafka有诸多好处，不一一提了）

再订阅需要的表将增量数据导入es

(
扩展：同时还可以将kafka里的binlog数据原样导入es，便于binglog分析

或者订阅binlog执行其他异步任务等等
)

**PS： mysql binlog格式 需要采用 row base格式**
