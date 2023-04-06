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
        ConfigEntity configEntity;
        try {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            Options options = new Options();
            options.addOption("c", "config", true, "config file path");
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, FaceProfileSynchronizing.args);
            if (cmd.hasOption("c")) {
                // 通过命令行参数查找配置文件
                String filePath = cmd.getOptionValue("c");
                configEntity = objectMapper.readValue(new File(filePath), ConfigEntity.class);
            } else {
                // 如果没有指定配置行文件则默认当前目录下的配置文件
                configEntity = objectMapper.readValue(new File("config.yaml"), ConfigEntity.class);
            }
        } catch (ParseException | IOException e) {
            log.error("没有指定配置文件！");
            throw new RuntimeException(e);
        }
        return configEntity;
    }
}
