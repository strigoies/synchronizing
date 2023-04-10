package com.yisa;

import com.yisa.model.FaceProfile;
import com.yisa.model.FullDocument;
import com.yisa.sink.ClickhouseSink;
import com.yisa.source.KafkaSource;
import com.yisa.source.MongoSource;
import com.yisa.transform.SetDeleteTagAndFilter;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FaceProfileSynchronizing {
    public static String[] args;

    public static void main(String[] args) throws Exception {
        FaceProfileSynchronizing.args = args;

        // 获取配置文件配置
        ConfigEntity config = ReadConfig.getConfigEntity();

        /**
         * 本地环境调试配置
         *         Configuration conf = new Configuration();
         *         conf.setString(RestOptions.BIND_PORT, "8081-8099");
         *         StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 是否禁用算子操作链
        if (config.getParameter().isOperatorChain()) {
            env.disableOperatorChaining();
        }

        // 获取kafka数据
        SingleOutputStreamOperator<FaceProfile> kafkaStream = KafkaSource.getKafkaStream(env, config.getKafka());

        // 是否启用系统恢复
        SingleOutputStreamOperator<FullDocument> outputStream;
        if (config.getParameter().isSystemRecovery()) {
            // 获取mongo数据
            DataStreamSource<FaceProfile> mongoStream = MongoSource.getMongoStream(env, config.getMongodb());
            DataStream<FaceProfile> unionOutputStream = mongoStream.union(kafkaStream);
            outputStream = unionOutputStream.flatMap(new SetDeleteTagAndFilter());
        }else {
            outputStream = kafkaStream.flatMap(new SetDeleteTagAndFilter());
        }

        // 数据写入 clickhouse
        ClickhouseSink.getClickhouseSink(env, config.getLightningdb(), outputStream);
    }
}
