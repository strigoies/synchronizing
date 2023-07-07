package com.yisa.sink;

import com.yisa.model.FullDocument;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.StringUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class LightningDBSink {
    private static final String DRIVER_NAME = "com.clickhouse.jdbc.ClickHouseDriver";
    public static String faceProfileInsertSql;

    static {
        faceProfileInsertSql = "INSERT INTO %s (group, center, new_id, group_count, personnel_name, personnel_id_number, personnel_photo_url, " +
                "cosine_similarity, associated_time, insert_time, centers, device_object_types, household_code, household_address, birthday, " +
                "gender, high_quality_id, is_deleted) " +
                "VALUES (" + StringUtils.generateMark(19) + ")";
    }

    /**
     * 写入雷霆表
     *
     * @param lightningDB
     * @return
     */
    public static SinkFunction<FullDocument> insertFaceProfileSinkFunction(ConfigEntity.LightningDB lightningDB) {
        String insertSql = String.format(faceProfileInsertSql, lightningDB.getActiveTable());
        // 随机获取一个节点
        List<List> lightningDBHostPorts = lightningDB.getHosts();
        int index = new Random().nextInt(lightningDBHostPorts.size());
        String url = String.format("jdbc:clickhouse://%s:%s/%s?use_binary_string=true",
                lightningDBHostPorts.get(index).get(0),
                lightningDBHostPorts.get(index).get(1),
                lightningDB.getDatabase()); // 默认数据库

        JdbcStatementBuilder<FullDocument> statementBuilder = getFaceProfileJdbcStatementBuilder();

        return JdbcSink.sink(
                insertSql,
                statementBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchSize(lightningDB.getBatchSize())
                        .withBatchIntervalMs(lightningDB.getFlushInterval())
                        .withMaxRetries(lightningDB.getMaxRetries())
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(DRIVER_NAME)
                        .withUsername(lightningDB.getUsername())
                        .withPassword(lightningDB.getPassword())
                        .build()
        );
    }

    private static JdbcStatementBuilder<FullDocument> getFaceProfileJdbcStatementBuilder() {

        return (statement, faceProfile) -> {
            statement.setLong(1, faceProfile.getGroup());
            statement.setBytes(2, faceProfile.getCenter());
            statement.setBytes(3, faceProfile.getNewId());
            statement.setInt(4, faceProfile.getGroupCount());
            statement.setString(5, faceProfile.getPersonnelName());
            statement.setString(6, faceProfile.getPersonnelIdNumber());
            statement.setString(7, faceProfile.getPersonnelPhotoUrl());
            statement.setFloat(8, faceProfile.getCosineSimilarity());
            statement.setInt(9, faceProfile.getAssociatedTime());
            statement.setInt(10, faceProfile.getInsertTime());

            Connection conn = statement.getConnection();
            statement.setArray(11, conn.createArrayOf("String", faceProfile.getCenters()));
            Object[] objectTuples = new Object[faceProfile.getSourceTypes().length];
            for (int i = 0; i < faceProfile.getSourceTypes().length; i++) {
                objectTuples[i] = new Object[] {
                        faceProfile.getSourceTypes()[i][0],
                        faceProfile.getSourceTypes()[i][1]
                };
            }
            statement.setArray(12, conn.createArrayOf("Tuple(UInt8, UInt8)", objectTuples));
            statement.setInt(13, faceProfile.getHouseholdCode());
            statement.setString(14, faceProfile.getHouseholdAddress());
            statement.setInt(15, faceProfile.getBirthday());
            statement.setShort(16, faceProfile.getGender());
            statement.setBytes(17, faceProfile.getHighQualityId());
            statement.setByte(18, faceProfile.getIsDeleted());
        };
    }
}
