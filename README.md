# 人脸档案同步项目
## 项目简介
  此程序同步mongo对应的集合到雷霆中心点档案(face_profile)表。
  
## 程序简介
1. 使用`debazium`监控mongo数据变化同步到kafka, 消费kafka数据同步到雷霆对应表。
2. 故障恢复，即当kafka的该topic数据过期。应该使用该系统故障恢复功能, 即配置文件`system_recovery`属性设置为true。
3. 当启用故障恢复时, 会从mongo拉去最新的全量数据和kafka的数据进行union操作, 写入到雷霆。 
4. Sink 使用 `https://github.com/itinycheng/flink-connector-clickhouse/tree/release-1.14`
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