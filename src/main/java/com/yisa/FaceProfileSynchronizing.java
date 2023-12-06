package com.yisa;

import com.yisa.model.BaseData;
import com.yisa.model.FaceProfile;
import com.yisa.sink.LightningDBSink;
import com.yisa.source.KafkaSource;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FaceProfileSynchronizing {
    public static String[] args;
    private static final String FACE_PROFILE_JOB = "faceProfileSynchronizing";
    private static final String FACE_PROFILE_PLATE_JOB = "faceProfilePlateSynchronizing";

    public static void main(String[] args) throws Exception {
        FaceProfileSynchronizing.args = args;
        // 获取配置文件配置
        ConfigEntity config = ReadConfig.getConfigEntity();
        // 依据启动的任务 设置消费相应的topic和写入相应的table
        if (FACE_PROFILE_JOB.equals(config.getParameter().getJobName())) {
            config.getKafka().setActiveTopic(config.getKafka().getFaceProfileTopic());
            config.getLightningDB().setActiveTable(config.getLightningDB().getFaceProfileDistributedTable());
        } else if (FACE_PROFILE_PLATE_JOB.equals(config.getParameter().getJobName())) {
            config.getKafka().setActiveTopic(config.getKafka().getFaceProfilePlateTopic());
            config.getLightningDB().setActiveTable(config.getLightningDB().getFaceProfilePlateDistributedTable());
        } else {
            log.error("选择了错误的任务去运行!!!");
            throw new Exception("No change of task");
        }

        /*本地环境调试配置
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8099");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint
        env.enableCheckpointing(config.getParameter().getCheckPoint());
        // 设置两次 checkpoint 之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.getParameter().getCheckPoint()/2);
        // 开启task级别故障自动启动failover
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, config.getParameter().getDelayBetweenAttempts()));

        // 是否禁用算子操作链
        if (config.getParameter().isDisableOperatorChain()) {
            env.disableOperatorChaining();
        }

        // 获取kafka数据
        SingleOutputStreamOperator<BaseData> outputStream = KafkaSource.getKafkaStream(env, config.getKafka())
                .name("get data source");

        // 数据写入 clickhouse
        outputStream.keyBy((KeySelector<BaseData, Long>) value -> {
            FaceProfile val = (FaceProfile)value;
            return val.getGroup();
        })
                .addSink(LightningDBSink.insertFaceProfileSinkFunction(config.getLightningDB()))
                .name("insert into clickhouse");

        env.execute(config.getParameter().getJobName());
    }
}
