[face-profile-synchronizing](http://git.yisa.com.cn/IntelligentComputing/face-profile-synchronizing) 

# 程序注意事项：

## 数据库方面：

1. 创建本地表

```sql
# 修改基础表face_profile的创建方法
create table yisa_oe.face_profile on cluster distable
(
    `group` UInt64,
    `center` String,
    `new_id` FixedString(16),
    `group_count` UInt32,
    `personnel_name` String,
    `personnel_id_number` String,
    `personnel_photo_url` String,
    `cosine_similarity` Float32,
    `special_type` UInt8,
    `associated_time` UInt32,
    `insert_time` UInt32,
    `source_ids` Array(UInt8),
    `centers` Array(String),
    `household_code` UInt32,
    `household_address` String,
    `birthday` UInt32,
    `gender` UInt8,
    `high_quality_id` FixedString(16),
    `is_deleted` Int8,
    PROJECTION face_profile_projection
 (
     SELECT
         group,
         argMax(center, insert_time) as center,
         argMax(new_id, insert_time) as new_id,
         argMax(group_count, insert_time) as group_count,
         argMax(personnel_name, insert_time) as personnel_name,
         argMax(personnel_id_number, insert_time) as personnel_id_number,
         argMax(personnel_photo_url, insert_time) as personnel_photo_url,
         argMax(cosine_similarity, insert_time) as cosine_similarity,
         argMax(special_type, insert_time) as special_type,
         argMax(associated_time, insert_time) as associated_time,
         argMax(source_ids, insert_time) as source_ids,
         argMax(centers, insert_time) as centers,
         argMax(household_code, insert_time) as household_code,
         argMax(household_address, insert_time) as household_address, 
         argMax(birthday, insert_time) as birthday,  
         argMax(gender, insert_time) as gender,  
         argMax(high_quality_id, insert_time) as high_quality_id,  
         argMax(is_deleted, insert_time) as is_deleted 
     GROUP BY group
 )
)
ENGINE = ReplacingMergeTree()
PARTITION BY group % 100
ORDER BY group
SETTINGS index_granularity=8192

```

2. 分布式表创建方法

```sql
CREATE TABLE yisa_oe.face_profile_insert_all on cluster distable
AS yisa_oe.face_profile
ENGINE = Distributed('distable', 'yisa_oe', 'face_profile', cityHash64(group))
```

3. 创建视图层

```sql
CREATE VIEW yisa_oe.face_profile_view_all on cluster distable AS 
   SELECT
       group,
       argMax(center, insert_time) as center,
       argMax(new_id, insert_time) as new_id,
       argMax(group_count, insert_time) as group_count,
       argMax(personnel_name, insert_time) as personnel_name,
       argMax(personnel_id_number, insert_time) as personnel_id_number,
       argMax(personnel_photo_url, insert_time) as personnel_photo_url,
       argMax(cosine_similarity, insert_time) as cosine_similarity,
       argMax(special_type, insert_time) as special_type,
       argMax(associated_time, insert_time) as associated_time,
       argMax(source_ids, insert_time) as source_ids,
       argMax(centers, insert_time) as centers,
       argMax(household_code, insert_time) as household_code,
       argMax(household_address, insert_time) as household_address, 
       argMax(birthday, insert_time) as birthday,  
       argMax(gender, insert_time) as gender,  
       argMax(high_quality_id, insert_time) as high_quality_id,  
       argMax(is_deleted, insert_time) as is_deleted 
FROM face_profile_insert_all
GROUP BY group
SETTINGS allow_experimental_projection_optimization=1, force_optimize_projection=0
```

4. 后端接口查询该表时，需要查询 `face_profile_view` 表，并且不能使用 prewhere 语法。

查询案例：`SELECT group, center from face_profile_view where group = 10`



## 中间件Kafka方面

本程序需要消费 由`debezium`监控的mongo中 人脸聚类 集合 的topic。该topic清理日志策略为compact。

**第一次启动该程序时，需要将配置文件中 kafka 的 offset 设置为 earliest。**

可以通过命令查看：

```shell
kafka-topics.sh --describe --bootstrap-server 192.168.11.16:9092 --topic mongo-shard.yisa_oe.face_profile
```



## 其他

程序中设置了task级别错误自动重启，所以不应看程序是否挂到来判断程序是否成功启动，应该看程序是否一直在`running`状态(web ui进行查看)。