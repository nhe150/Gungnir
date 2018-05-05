package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

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
                    return readKafkaStreamWithSchema(ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic"),
                            ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"), configProvider.readSchema(ConfigProvider.retrieveConfigValue(kafkaConfig, "schemaName")));
                }
                return readKafkaStream(ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic"), ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"));
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for readFromKafka");
        }
    }

    public void writeToKafka(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        JsonNode kafkaConfig = getKafkaConfig(providedConfig);
        String output = "";
        String topic = "";
        if(ConfigProvider.hasConfigValue(kafkaConfig, "output")) output = ConfigProvider.retrieveConfigValue(kafkaConfig, "output");
        if(ConfigProvider.hasConfigValue(kafkaConfig, "kafka.topic")) topic = ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic");

        String outputTopic = topic.isEmpty()? output : topic;

        switch (processType) {
            case "batch":
                batchToKafka(dataset, outputTopic, ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"));
                break;
            case "stream":
                streamToKafka(dataset, outputTopic, ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"), output);
                break;
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for writeToKafka");
        }
    }

    public Dataset<Row> readKafkaStream(String topics, String bootstrap_servers) throws Exception {
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("subscribe", topics)
                .option("maxOffsetsPerTrigger", configProvider.retrieveAppConfigValue("kafka.maxOffsetsPerTrigger"))
                .option("fetchOffset.numRetries", configProvider.retrieveAppConfigValue("kafka.fetchOffsetNumRetries"))
                .option("fetchOffset.retryIntervalMs", configProvider.retrieveAppConfigValue("kafka.fetchOffsetRetryIntervalMs"))
                .option("failOnDataLoss", configProvider.retrieveAppConfigValue("spark.streamingKafkaFailOnDataLoss"))
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    }

    public Dataset<Row> readKafkaStreamWithSchema(String topics, String bootstrap_servers, StructType schema) throws Exception {
        return readKafkaStream(getKafkaTopicNames(topics), bootstrap_servers)
                .filter(col("key").notEqual(CommonFunctions.PreProcess.BAD_DATA_LABLE))
                .select(from_json(col("value"), schema).as("data")).select("data.*");
    }

    public StreamingQuery streamToKafka(Dataset<Row> dataset, String topic, String bootstrap_servers, String output) throws Exception {
        if(topic.isEmpty()) throw new IllegalArgumentException("WriteToKafka: Can't find output topic in the config for writing data");
        return dataset
                .selectExpr("to_json(struct(*)) as value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("topic", getKafkaTopicNames(topic))
                .option("kafka.retries", configProvider.retrieveAppConfigValue("kafka.retries"))
                .option("kafka.retry.backoff.ms", configProvider.retrieveAppConfigValue("kafka.retryBackoffMs"))
                .option("kafka.metadata.fetch.timeout.ms", configProvider.retrieveAppConfigValue("kafka.metadataFetchTimeoutMs"))
                .option("kafka.linger.ms", configProvider.retrieveAppConfigValue("kafka.lingerMs"))
                .option("kafka.batch.size", configProvider.retrieveAppConfigValue("kafka.batchSize"))
                .option("kafka.timeout.ms", configProvider.retrieveAppConfigValue("kafka.timeoutMs"))
                .option("kafka.request.timeout.ms", configProvider.retrieveAppConfigValue("kafka.requestTimeoutMs"))
                .option("kafka.max.request.size", configProvider.retrieveAppConfigValue("kafka.maxRequestSize"))
                .option("fetchOffset.numRetries", configProvider.retrieveAppConfigValue("kafka.fetchOffsetNumRetries"))
                .option("fetchOffset.retryIntervalMs", configProvider.retrieveAppConfigValue("kafka.fetchOffsetRetryIntervalMs"))
                .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                .queryName("streamToKafka_" + topic + output)
                .start();
    }

    public void batchToKafka(Dataset<Row> dataset, String topic, String bootstrap_servers) throws Exception {
        if(topic.isEmpty()) throw new IllegalArgumentException("WriteToKafka: Can't find output topic in the config for writing data");
        dataset
                .selectExpr("to_json(struct(*)) AS value")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("topic", getKafkaTopicNames(topic))
                .option("kafka.retries", configProvider.retrieveAppConfigValue("kafka.retries"))
                .option("kafka.retry.backoff.ms", configProvider.retrieveAppConfigValue("kafka.retryBackoffMs"))
                .save();
    }

    private String getKafkaTopicNames(String topics) throws Exception{
        String[] topicNames = topics.split(",");
        for(int i=0; i < topicNames.length; i++){
            topicNames[i] = configProvider.retrieveAppConfigValue("kafka.topicPrefix") + topicNames[i] + configProvider.retrieveAppConfigValue("kafka.topicPostfix");
        }
        return String.join(",", topicNames);
    }

}

