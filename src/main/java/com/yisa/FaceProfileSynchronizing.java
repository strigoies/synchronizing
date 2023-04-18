package com.yisa;

import com.yisa.model.FullDocument;
import com.yisa.sink.ClickhouseSink;
import com.yisa.source.KafkaSource;
import com.yisa.transform.SetDeleteTagAndFilter;
import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FaceProfileSynchronizing {
    public static String[] args;

    public static void main(String[] args) throws Exception {
        FaceProfileSynchronizing.args = args;

        // 获取配置文件配置
        ConfigEntity config = ReadConfig.getConfigEntity();

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
        SingleOutputStreamOperator<FullDocument> outputStream = KafkaSource.getKafkaStream(env, config.getKafka())
                .flatMap(new SetDeleteTagAndFilter());

        // 数据写入 clickhouse
        ClickhouseSink.getClickhouseSink(env, config.getLightningdb(), outputStream);
    }
}
