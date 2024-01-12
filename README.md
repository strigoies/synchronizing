# 人脸档案同步项目
## 项目简介
  此程序同步mongo的face_group和face_group_plate 集合到clickhouse和ArangoDB的face_profile和face_profile_plate表。

## 程序简介
1. 使用`debazium`监控mongo数据变化同步到kafka, 消费kafka数据同步到雷霆对应表。
2. Sink 使用 jdbc `com.clickhouse`


## 程序启动

1. 若有 `flinkdaemon` 程序，则使用其启动即可，注意jar包版本要设置正确
2. 若选择命令行启动(注意选择正确的jar包版本和配置文件路径启动)：

```shell
# 人脸聚类档案同步任务启动命令
flink run -d face-profile-synchronizing-1.1.0-beta.jar --jobName faceProfileSynchronizing --config ./config.yaml
# 驾乘人脸聚类档案同步任务启动命令
flink run -d face-profile-synchronizing-1.1.0-beta.jar --jobName faceProfilePlateSynchronizing --config ./config.yaml
```


## Clickhouse档案表建表语句
详见文档 `install-notice.md`

