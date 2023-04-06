package com.yisa.transform;

import com.yisa.model.FaceProfile;
import com.yisa.model.FullDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class Transform {

    /**
     * mongo的json字符串转化为javaBean
     */
    public static class ParseJsonFromMongo implements FlatMapFunction<FaceProfile, FullDocument> {

        @Override
        public void flatMap(FaceProfile faceProfile, Collector<FullDocument> collector) {
            try {
                // 设置删除标记
                if (faceProfile.getOperationType().equals("delete")) {
                    faceProfile.setDeleteTag(faceProfile.getGroup());
                }
                // 过滤数据
                if (faceProfile.getFullDocument() != null
                        && faceProfile.getGroup() > 0
                        // && faceProfile.getFullDocument().getNewId() != null
                ){

                    collector.collect(faceProfile.getFullDocument());
                }
                // collector.collect(faceProfile.getFullDocument());
            } catch (Exception e) {
                log.error("errorMessage: {}, data: {}", e.getMessage(), faceProfile);
            }
        }
    }
}
