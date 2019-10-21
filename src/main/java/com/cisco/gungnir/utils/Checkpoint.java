package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

public class Checkpoint implements Serializable {
    protected static String constructCheckpoint(JsonNode config) throws Exception {
        String checkpoint = "";
        if(ConfigProvider.hasConfigValue(config, "checkpoint")) checkpoint = ConfigProvider.retrieveConfigValue(config, "checkpoint");
        else checkpoint = Kafka.constructKafkaTopic(config);
        return checkpoint;
    }
}
