package com.yisa.sink;

import com.yisa.model.BaseData;
import com.yisa.model.FaceProfile;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.StringUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.util.List;
import java.util.Random;

public class LightningDBSink {
    private static final String DRIVER_NAME = "com.clickhouse.jdbc.ClickHouseDriver";
    public static String faceProfileInsertSql;

    static {
        faceProfileInsertSql = "INSERT INTO %s (group, center, new_id, group_count, face_count, person_count, age_id,personnel_name, personnel_id_number, personnel_photo_url, " +
                "cosine_similarity, associated_time, insert_time, centers, device_object_types, household_code, household_address, birthday, " +
                "gender, high_quality_id, labels, is_deleted) " +
                "VALUES (" + StringUtils.generateMark(21) + ")";
    }

    /**
     * 写入雷霆表
     *
     * @param lightningDB
     * @return
     */
    public static SinkFunction<BaseData> insertFaceProfileSinkFunction(ConfigEntity.LightningDB lightningDB) {
        String insertSql = String.format(faceProfileInsertSql, lightningDB.getActiveTable());
        // 随机获取一个节点
        List<List> lightningDBHostPorts = lightningDB.getHosts();
        int index = new Random().nextInt(lightningDBHostPorts.size());
        String url = String.format("jdbc:clickhouse://%s:%s/%s?use_binary_string=true",
                lightningDBHostPorts.get(index).get(0),
                lightningDBHostPorts.get(index).get(1),
                lightningDB.getDatabase()); // 默认数据库

        JdbcStatementBuilder statementBuilder = getFaceProfileJdbcStatementBuilder();

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

    private static JdbcStatementBuilder<FaceProfile> getFaceProfileJdbcStatementBuilder() {

        return (statement, faceProfile) -> {
            int i = 0;
            statement.setLong(++i, faceProfile.getGroup());
            statement.setBytes(++i, faceProfile.getCenter());
            statement.setBytes(++i, faceProfile.getNewId());
            statement.setInt(++i, faceProfile.getGroupCount());
            statement.setInt(++i, faceProfile.getFaceCount());
            statement.setInt(++i, faceProfile.getPersonCount());
            statement.setInt(++i, faceProfile.getAgeId());
            statement.setString(++i, faceProfile.getPersonnelName());
            statement.setString(++i, faceProfile.getPersonnelIdNumber());
            statement.setString(++i, faceProfile.getPersonnelPhotoUrl());
            statement.setFloat(++i, faceProfile.getCosineSimilarity());
            statement.setInt(++i, faceProfile.getAssociatedTime());
            if (faceProfile.getInsertTime() == 0) /* 刪除的数据 设置最新时间戳 */ {
                faceProfile.setInsertTime(System.currentTimeMillis());
            }
            statement.setLong(++i, faceProfile.getInsertTime());

            Connection conn = statement.getConnection();
            statement.setArray(++i, conn.createArrayOf("String", faceProfile.getCenters()));
            Object[] objectTuples = new Object[faceProfile.getSourceTypes().length];
            for (int j = 0; j < faceProfile.getSourceTypes().length; j++) {
                objectTuples[j] = new Object[] {
                        faceProfile.getSourceTypes()[j][0],
                        faceProfile.getSourceTypes()[j][1]
                };
            }
            statement.setArray(++i, conn.createArrayOf("Tuple(UInt8, UInt8)", objectTuples));
            statement.setInt(++i, faceProfile.getHouseholdCode());
            statement.setString(++i, faceProfile.getHouseholdAddress());
            statement.setInt(++i, faceProfile.getBirthday());
            statement.setShort(++i, faceProfile.getGender());
            statement.setBytes(++i, faceProfile.getHighQualityId());
            statement.setArray(++i, conn.createArrayOf("UInt32", faceProfile.getLabels()));
            statement.setByte(++i, faceProfile.getIsDeleted());
        };
    }
}
