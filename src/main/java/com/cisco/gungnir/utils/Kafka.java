package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class Kafka implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;


    public Kafka(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
    }

    private JsonNode getKafkaConfig(JsonNode providedConfig) throws Exception {
        JsonNode merged = ConfigProvider.merge(configProvider.getAppConfig().deepCopy(), providedConfig);
        return merged;
    }

    public Dataset readFromKafka(String processType, JsonNode providedConfig) throws Exception {
        JsonNode kafkaConfig = getKafkaConfig(providedConfig);

        switch (processType) {
            case "stream":
                if(ConfigProvider.hasConfigValue(kafkaConfig, "schemaName")){
                    return readKafkaStreamWithSchema(kafkaConfig);
                }
                return readKafkaStream(kafkaConfig);
            case "batch":
                if(ConfigProvider.hasConfigValue(kafkaConfig, "schemaName")){
                    return readKafkaBatchWithSchema(kafkaConfig);
                }
                return readKafkaBatch(kafkaConfig);
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for readFromKafka");
        }
    }

    public void writeToKafka(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        JsonNode kafkaConfig = getKafkaConfig(providedConfig);

        switch (processType) {
            case "batch":
                batchToKafka(dataset, kafkaConfig);
                break;
            case "stream":
                streamToKafka(dataset, kafkaConfig);
                break;
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for writeToKafka");
        }
    }

    public Dataset<Row> readKafkaStream(JsonNode kafkaConfig) throws Exception {
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                .option("subscribe", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic"))
                .option("maxOffsetsPerTrigger", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.maxOffsetsPerTrigger"))
                .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetNumRetries"))
                .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetRetryIntervalMs"))
                .option("failOnDataLoss", ConfigProvider.retrieveConfigValue(kafkaConfig,"spark.streamingKafkaFailOnDataLoss"))
                .option("startingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.startingOffsets")? kafkaConfig.get("kafka").get("startingOffsets").toString(): "earliest")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .selectExpr("CASE WHEN (key IS NOT NULL) THEN split(key, '^')[0] ELSE key END as key", "value");
    }

    public Dataset<Row> readKafkaStreamWithSchema(JsonNode kafkaConfig) throws Exception {
        return readKafkaStream(kafkaConfig)
                .filter(col("key").notEqual(Constants.BAD_DATA_LABLE))
                .select(from_json(col("value"), configProvider.readSchema(ConfigProvider.retrieveConfigValue(kafkaConfig, "schemaName"))).as("data")).select("data.*");
    }

    public Dataset<Row> readKafkaBatch(JsonNode kafkaConfig) throws Exception {
        return spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                .option("subscribe", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic"))
                .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetNumRetries"))
                .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetRetryIntervalMs"))
                .option("failOnDataLoss", ConfigProvider.retrieveConfigValue(kafkaConfig,"spark.streamingKafkaFailOnDataLoss"))
                .option("startingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.startingOffsets")? kafkaConfig.get("kafka").get("startingOffsets").toString(): "earliest")
                .option("endingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.endingOffsets")? kafkaConfig.get("kafka").get("endingOffsets").toString(): "latest")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .selectExpr("CASE WHEN (key IS NOT NULL) THEN split(key, '^')[0] ELSE key END as key", "value");
    }

    public Dataset<Row> readKafkaBatchWithSchema(JsonNode kafkaConfig) throws Exception {
        return readKafkaBatch(kafkaConfig)
                .filter(col("key").notEqual(Constants.BAD_DATA_LABLE))
                .select(from_json(col("value"), configProvider.readSchema(ConfigProvider.retrieveConfigValue(kafkaConfig, "schemaName"))).as("data")).select("data.*");
    }

    public StreamingQuery streamToKafka(Dataset<Row> dataset, JsonNode kafkaConfig) throws Exception {
        String topic = constructKafkaTopic(kafkaConfig);
        String output = ConfigProvider.hasConfigValue(kafkaConfig, "output")? ConfigProvider.retrieveConfigValue(kafkaConfig, "output"): "";

        return  constructKafkaKeyValue(dataset, kafkaConfig)
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                .option("topic", getKafkaTopicNames(topic))
                .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.retries"))
                .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.retryBackoffMs"))
                .option("kafka.metadata.fetch.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.metadataFetchTimeoutMs"))
                .option("kafka.linger.ms", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.lingerMs"))
                .option("kafka.batch.size", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.batchSize"))
                .option("kafka.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.timeoutMs"))
                .option("kafka.request.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.requestTimeoutMs"))
                .option("kafka.max.request.size", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.maxRequestSize"))
                .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetNumRetries"))
                .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetRetryIntervalMs"))
                .trigger(ProcessingTime(ConfigProvider.retrieveConfigValue(kafkaConfig,"spark.streamngTriggerWindow")))
                .queryName("streamToKafka_" + topic + output)
                .start();
    }

    public void batchToKafka(Dataset<Row> dataset, JsonNode kafkaConfig) throws Exception {
        constructKafkaKeyValue(dataset, kafkaConfig)
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                .option("topic", getKafkaTopicNames(constructKafkaTopic(kafkaConfig)))
                .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retries"))
                .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retryBackoffMs"))
                .save();
    }

    private String constructKafkaTopic(JsonNode kafkaConfig) throws Exception{
        String topic = "";
        if(ConfigProvider.hasConfigValue(kafkaConfig, "kafka.topic")){
            topic = ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic");
        } else {
            if(ConfigProvider.hasConfigValue(kafkaConfig, "output")) topic = ConfigProvider.retrieveConfigValue(kafkaConfig, "output");
        }

        if(topic.isEmpty()) throw new IllegalArgumentException("WriteToKafka: Can't find output topic in the config for writing data");
        return topic;
    }

    private Dataset constructKafkaKeyValue(Dataset dataset, JsonNode kafkaConfig) throws Exception{
        if(!ConfigProvider.hasConfigValue(kafkaConfig, "kafka.topicKey")) {
            if(ConfigProvider.hasConfigValue(kafkaConfig, "kafka.topicValue")){
                dataset = dataset.selectExpr(ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topicValue") + " as value");
            }else{
                dataset = dataset.selectExpr("to_json(struct(*)) as value");
            }
        }else{
            if(ConfigProvider.hasConfigValue(kafkaConfig, "kafka.topicValue")){
                dataset = dataset.selectExpr("CONCAT("+ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topicKey")+", '^', uuid("+ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topicKey")+")) as key", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topicValue") + " as value");
            }else{
                dataset = dataset.selectExpr("CONCAT("+ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topicKey")+", '^', uuid("+ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topicKey")+")) as key", "to_json(struct(*)) as value");
            }
        }
        return dataset;
    }

    private String getKafkaTopicNames(String topics) throws Exception{
        String[] topicNames = topics.split(",");
        for(int i=0; i < topicNames.length; i++){
            topicNames[i] = configProvider.retrieveAppConfigValue("kafka.topicPrefix") + topicNames[i] + configProvider.retrieveAppConfigValue("kafka.topicPostfix");
        }
        return String.join(",", topicNames);
    }

}

