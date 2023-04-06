# 人脸档案同步项目
## 项目简介
  此程序同步mongo对应的集合到雷霆中心点档案表。
  
## 程序简介
1. 使用flinkCDC技术首次全量同步之后监控增量同步。
2. Source 使用官方的MongoDBSource
3. Sink 使用 `https://github.com/itinycheng/flink-connector-clickhouse/tree/release-1.14`
* 下载对应版本项目后 编译` mvn clean install -DskipTests`
* jar包导入本地maven环境 `mvn install:install-file -Dfile=<path-to-jar> -DgroupId=<group-id> -DartifactId=<artifact-id> -Dversion=<version> -Dpackaging=jar`
例如: `mvn install:install-file -Dfile='D:\IdeaProject\flink-connector-clickhouse\target\flink-connector-clickhouse-1.14.3-SNAPSHOT.jar' -DgroupId='org.apache.flink' -DartifactId='flink-connector-clickhouse' -Dversion='1.14.3-SNAPSHOT' -Dpackaging='jar'`
* 添加依赖
```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-clickhouse</artifactId>
	<version>1.14.3-SNAPSHOT</version>
</dependency>
```