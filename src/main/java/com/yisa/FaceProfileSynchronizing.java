package com.yisa;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.yisa.model.FaceProfile;
import com.yisa.model.FaceProfileSchema;
import com.yisa.model.FullDocument;
import com.yisa.sink.ClickhouseSink;
import com.yisa.source.MongoSource;
import com.yisa.transform.Transform;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FaceProfileSynchronizing {
    public static String[] args;

    public static void main(String[] args) throws Exception {
        FaceProfileSynchronizing.args = args;

        // 获取配置文件配置
        ConfigEntity config = ReadConfig.getConfigEntity();

        Configuration conf = new Configuration();
        // conf.setInteger(RestOptions.PORT, 8081);
        conf.setString(RestOptions.BIND_PORT, "8081-8099");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取mongo数据
        MongoDBSource<FaceProfile> mongoSource = MongoSource.getMongoSource(config.getMongodb());

        // enable checkpoint
        env.enableCheckpointing(config.getParameter().getCheckpointingInterval());

        DataStreamSource<FaceProfile> mongoDBIncrementalStream = env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource");
        SingleOutputStreamOperator<FullDocument> outputStream = mongoDBIncrementalStream.flatMap(new Transform.ParseJsonFromMongo());

        // 数据写入 clickhouse
        ClickhouseSink.getClickhouseSink(env, config.getLightningdb(), outputStream);
        env.execute("faceProfileSynchronizing");
    }
}
