package com.yisa.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.yisa.FaceProfileSynchronizing;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;

@Slf4j
public class ReadConfig {

    public static ConfigEntity getConfigEntity() {
        String filePath = "config.yaml";
        ConfigEntity configEntity;
        String jobName;
        try {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            Options options = new Options();
            options.addOption("c", "config", true, "config file path");
            options.addOption("j", "jobName", true, "select job start");
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, FaceProfileSynchronizing.args);
            if (cmd.hasOption("c")) {
                // 通过命令行参数查找配置文件
                filePath = cmd.getOptionValue("c");
            }

            configEntity = objectMapper.readValue(new File(filePath), ConfigEntity.class);

            // 选择job启动
            if (cmd.hasOption("j")) {
                jobName = cmd.getOptionValue("j");
            } else {
                throw new ParseException("No job is specified to start.");
            }
            configEntity.getParameter().setJobName(jobName);
        } catch (ParseException | IOException e) {
            log.error("错误指定启动参数！");
            throw new RuntimeException(e);
        }
        return configEntity;
    }
}
