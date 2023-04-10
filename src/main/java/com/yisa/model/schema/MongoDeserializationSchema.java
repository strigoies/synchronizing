package com.yisa.model.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.yisa.model.FaceProfile;
import com.yisa.model.FullDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * mongoSource 的key反序列化schema
 */
@Slf4j
public class MongoDeserializationSchema implements DebeziumDeserializationSchema<FaceProfile> {

    private static final long serialVersionUID = -3168848963265670603L;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<FaceProfile> collector) throws Exception {
        Struct value = (Struct) sourceRecord.value();
        FaceProfile faceProfile = new FaceProfile();
        // 获取 documentKey 对应 雷霆的字段 group
        // documentKey={"_id": 68279}
        String idStruct = value.getString("documentKey");
        JSONObject idJson = JSON.parseObject(idStruct);
        Long group = idJson.getLong("_id");
        faceProfile.setGroup(group);

        // 获取操作类型
        String operationType = value.getString("operationType");
        faceProfile.setOperationType(operationType);

        // 获取数据
        String fullDocumentStr = value.getString("fullDocument");
        faceProfile.setFullDocument(JSON.parseObject(fullDocumentStr, FullDocument.class));

        if (fullDocumentStr!=null) {
            JSONObject fullDocumentData = JSONObject.parseObject(fullDocumentStr);
            CommonSchemaTransform commonSchemaTransform = new CommonSchemaTransform(fullDocumentData, faceProfile);
            commonSchemaTransform.parseBytesData();
        }

        collector.collect(faceProfile);
    }

    @Override
    public TypeInformation<FaceProfile> getProducedType() {
        return TypeInformation.of(FaceProfile.class);
    }
}
