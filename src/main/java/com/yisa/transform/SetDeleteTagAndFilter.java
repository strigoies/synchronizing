package com.yisa.transform;

import com.yisa.model.FaceProfile;
import com.yisa.model.FullDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 该删除的数据设置删除标记
 * 进一步过滤不符合规则的数据
 */
@Slf4j
public class SetDeleteTagAndFilter implements FlatMapFunction<FaceProfile, FullDocument> {
    @Override
    public void flatMap(FaceProfile faceProfile, Collector<FullDocument> collector) throws Exception {
        try {
            // 设置删除标记
            if (faceProfile.getOperationType().equals("delete") || faceProfile.getOperationType().equals("d")) {
                faceProfile.setDeleteTag(faceProfile.getGroup());
            }
            // 过滤数据
            if (faceProfile.getFullDocument() != null
                    && faceProfile.getGroup() > 0
                // && faceProfile.getFullDocument().getNewId() != null
            ){
                collector.collect(faceProfile.getFullDocument());
            }
        } catch (Exception e) {
            log.error("errorMessage: {}, data: {}", e.getMessage(), faceProfile);
        }
    }
}
