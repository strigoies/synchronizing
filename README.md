# 人脸档案同步项目
## 项目简介
  此程序同步mongo的face_group和face_group_plate 集合到雷霆的face_profile和face_profile_plate表。

## 程序简介
1. 使用`debazium`监控mongo数据变化同步到kafka, 消费kafka数据同步到雷霆对应表。
2. Sink 使用 `https://github.com/itinycheng/flink-connector-clickhouse/tree/release-1.14`
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



## 程序启动

1. 若有 `flinkdaemon` 程序，则使用其启动即可，注意jar包版本要设置正确
2. 若选择命令行启动(注意选择正确的jar包版本和配置文件路径启动)：

```shell
# 人脸聚类档案同步任务启动命令
flink run -d face-profile-synchronizing-1.1.0-beta.jar --jobName faceProfileSynchronizing --config ./config.yaml
# 驾乘人脸聚类档案同步任务启动命令
flink run -d face-profile-synchronizing-1.1.0-beta.jar --jobName faceProfilePlateSynchronizing --config ./config.yaml
```


## 雷霆档案表建表语句
详见文档 `install-notice.md`

