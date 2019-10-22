package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

public class Checkpoint implements Serializable {
    protected static String constructCheckpoint(JsonNode config) throws Exception {
        String checkpoint = "";
        if(ConfigProvider.hasConfigValue(config, "checkpoint")) {
            checkpoint = ConfigProvider.retrieveConfigValue(config, "checkpoint");
        }
        else {
            checkpoint = constructKafkaTopic(config);
        }
        return checkpoint;
    }

    protected static String constructKafkaTopic(JsonNode kafkaConfig) throws Exception{
        String topic = "";
        if(ConfigProvider.hasConfigValue(kafkaConfig, "kafka.topic")){
            topic = ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic");
        } else {
            if(ConfigProvider.hasConfigValue(kafkaConfig, "output")) topic = ConfigProvider.retrieveConfigValue(kafkaConfig, "output");
        }

        if(topic.isEmpty()) throw new IllegalArgumentException("WriteToKafka: Can't find output topic in the config for writing data");
        return topic;
    }
}
