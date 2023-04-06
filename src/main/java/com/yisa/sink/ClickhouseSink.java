package com.yisa.sink;

import com.yisa.model.FullDocument;
import com.yisa.utils.ConfigEntity;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ClickhouseSink {
    public static void getClickhouseSink(StreamExecutionEnvironment env, ConfigEntity.LightningDB lightningDB, SingleOutputStreamOperator<FullDocument> outputStream) {
        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(String.format("CREATE TABLE face_profile (\n" +
                        "                `group` DECIMAL,\n" +
                        "                `center` BYTES,\n" +
                        "                `group_count` BIGINT,\n" +
                        "                `personnel_name` STRING,\n" +
                        "                `personnel_id_number` STRING,\n" +
                        "                `personnel_photo_url` STRING,\n" +
                        "                `cosine_similarity` FLOAT,\n" +
                        "                `special_type` SMALLINT,\n" +
                        "                `associated_time` BIGINT,\n" +
                        "                `centers` ARRAY<BYTES>,\n" +
                        "                `source_ids` ARRAY<SMALLINT>,\n" +
                        "                `household_code` BIGINT,\n" +
                        "                `household_address` STRING,\n" +
                        "                `birthday` BIGINT,\n" +
                        "                `gender` SMALLINT,\n" +
                        "                `high_quality_id` BYTES,\n" +
                        "                `is_deleted` SMALLINT\n" +
                        // "                PRIMARY KEY (`group`) NOT ENFORCED\n" +
                        "        ) WITH (\n" +
                        "                'connector' = 'clickhouse',\n" +
                        "                'url' = 'clickhouse://%s',\n" +
                        "                'username' = '%s',\n" +
                        "                'password' =  '%s',\n" +
                        "                'database-name' = '%s',\n" +
                        "                'table-name' = '%s',\n" +
                        "                'sink.batch-size' = '%d',\n" +
                        "                'sink.flush-interval' = '%d',\n" +
                        "                'sink.max-retries' = '%d'\n" +
                        "        )",
                lightningDB.getHosts(),
                lightningDB.getUsername(),
                lightningDB.getPassword(),
                lightningDB.getDatabase(),
                lightningDB.getFaceProfileDistributedTable(),
                lightningDB.getBatchSize(),
                lightningDB.getFlushInterval(),
                lightningDB.getMaxRetries()
        ));

        // 数据流转化为表
        tEnv.fromDataStream(outputStream);
        // 注册表并使 javaBean 字段名称转化为clickhouse对应表名称
        tEnv.createTemporaryView("outputTable", outputStream,
                Schema.newBuilder()
                        // .columnByExpression("group", "group")
                        // .columnByExpression("center", "center")
                        .columnByExpression("group_count", "groupCount")
                        .columnByExpression("personnel_name", "personnelName")
                        .columnByExpression("personnel_id_number", "personnelIdNumber")
                        .columnByExpression("personnel_photo_url", "personnelPhotoUrl")
                        .columnByExpression("cosine_similarity", "cosineSimilarity")
                        .columnByExpression("special_type", "specialType")
                        .columnByExpression("associated_time", "associatedTime")
                        // .columnByExpression("centers", "centers")
                        .columnByExpression("source_ids", "sourceIds")
                        .columnByExpression("household_code", "householdCode")
                        .columnByExpression("household_address", "householdAddress")
                        // .columnByExpression("birthday", "birthday")
                        // .columnByExpression("gender", "gender")
                        .columnByExpression("high_quality_id", "highQualityId")
                        .columnByExpression("is_deleted", "isDeleted")
                        .build());
        // 插入数据
        tEnv.executeSql("insert into face_profile " +
                "select `group`, " +
                "`center`," +
                "`group_count`," +
                "`personnel_name`," +
                "`personnel_id_number`," +
                "`personnel_photo_url`," +
                "`cosine_similarity`," +
                "`special_type`," +
                "`associated_time`," +
                "`centers`," +
                "`source_ids`," +
                "`household_code`," +
                "`household_address`," +
                "`birthday`," +
                "`gender`," +
                "`high_quality_id`," +
                "`is_deleted`" +
                " from outputTable");
    }
}
