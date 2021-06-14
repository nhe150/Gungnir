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

    //used for scala code. incase of type eraser, using extra parameter to distinguish --- 2019-07-23 Norman He@cisco
    public void writeToKafka(Dataset<Row> dataset, String processType, JsonNode providedConfig, boolean diff) throws Exception {
        if(dataset==null) throw new IllegalArgumentException("can't write to kafka: the input dataset is NULL, please check previous query");
        JsonNode kafkaConfig = getKafkaConfig(providedConfig);

        switch (processType) {
            case "batch":
                batchToKafka(dataset, kafkaConfig, diff);
                break;
            case "stream":
                streamToKafka(dataset, kafkaConfig);
                break;
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for writeToKafka");
        }
    }

    public void writeToKafka(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if(dataset==null) throw new IllegalArgumentException("can't write to kafka: the input dataset is NULL, please check previous query");
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



    public Dataset readKafkaStream(JsonNode kafkaConfig) throws Exception {

        String is_ssl = ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.ssl");

        if(is_ssl.equals("true")) {
            return spark
                    .readStream()
                    .format("kafka")
//                    .option("kafka.security.protocol", "SASL_SSL")
//                    .option("kafka.sasl.kerberos.service.name", "kafka")
//                    .option("kafka.ssl.truststore.location", "./rpbt1.truststore.keystore.jks")
//                    .option("kafka.ssl.truststore.password", "Test@1234")
//                    .option("kafka.ssl.keystore.location", "./rpbt1.server.keystore.jks")
//                    .option("kafka.ssl.keystore.password", "Test@1234")
//                    .option("kafka.ssl.key.password", "Test@1234")
                    .option("kafka.security.protocol", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.securityProtocol"))
                    .option("kafka.sasl.kerberos.service.name", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.kerberosServiceName"))
                    .option("kafka.ssl.truststore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.truststore"))
                    .option("kafka.ssl.truststore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.trustStorePassword"))
                    .option("kafka.ssl.keystore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keystore"))
                    .option("kafka.ssl.keystore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keyStorePassword"))
                    .option("kafka.ssl.key.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.sslPassword"))
                    .option("groupIdPrefix",ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.groupIdPrefix"))
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("subscribe", getKafkaTopicNames(ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic"), kafkaConfig))
                    .option("maxOffsetsPerTrigger", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.maxOffsetsPerTrigger"))
                    .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetNumRetries"))
                    .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetRetryIntervalMs"))
                    .option("failOnDataLoss", ConfigProvider.retrieveConfigValue(kafkaConfig, "spark.streamingKafkaFailOnDataLoss"))
                    .option("startingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.startingOffsets") ? kafkaConfig.get("kafka").get("startingOffsets").toString().replaceAll("^\"|\"$", "") : "latest")
                    .load()
                    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .selectExpr("CASE WHEN (key IS NOT NULL) THEN split(key, '^')[0] ELSE key END as key", "value");
        }
        else {
            return spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("subscribe", getKafkaTopicNames(ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.topic"), kafkaConfig))
                    .option("maxOffsetsPerTrigger", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.maxOffsetsPerTrigger"))
                    .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetNumRetries"))
                    .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.fetchOffsetRetryIntervalMs"))
                    .option("failOnDataLoss", ConfigProvider.retrieveConfigValue(kafkaConfig,"spark.streamingKafkaFailOnDataLoss"))
                    .option("startingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.startingOffsets")? kafkaConfig.get("kafka").get("startingOffsets").toString().replaceAll("^\"|\"$", ""): "latest")
                    .load()
                    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .selectExpr("CASE WHEN (key IS NOT NULL) THEN split(key, '^')[0] ELSE key END as key", "value");
        }
    }


    public Dataset readKafkaStreamWithSchema(JsonNode kafkaConfig) throws Exception {
        return readKafkaStream(kafkaConfig)
                .select(from_json(col("value"), configProvider.readSchema(ConfigProvider.retrieveConfigValue(kafkaConfig, "schemaName"))).as("data"), col("value").as("raw")).select("data.*", "raw");
    }

    public Dataset readKafkaBatch(JsonNode kafkaConfig) throws Exception {

        String is_ssl = ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.ssl");

        if(is_ssl.equals("true")) {
            return spark
                    .read()
                    .format("kafka")
                    .option("kafka.security.protocol", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.securityProtocol"))
                    .option("kafka.sasl.kerberos.service.name", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.kerberosServiceName"))
                    .option("kafka.ssl.truststore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.truststore"))
                    .option("kafka.ssl.truststore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.trustStorePassword"))
                    .option("kafka.ssl.keystore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keystore"))
                    .option("kafka.ssl.keystore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keyStorePassword"))
                    .option("kafka.ssl.key.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.sslPassword"))
                    .option("groupIdPrefix",ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.groupIdPrefix"))
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("subscribe", getKafkaTopicNames(ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic"), kafkaConfig))
                    .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetNumRetries"))
                    .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetRetryIntervalMs"))
                    .option("failOnDataLoss", ConfigProvider.retrieveConfigValue(kafkaConfig, "spark.streamingKafkaFailOnDataLoss"))
                    .option("startingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.startingOffsets") ? kafkaConfig.get("kafka").get("startingOffsets").toString().replaceAll("^\"|\"$", "") : "earliest")
                    .option("endingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.endingOffsets") ? kafkaConfig.get("kafka").get("endingOffsets").toString().replaceAll("^\"|\"$", "") : "latest")
                    .load()
                    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .selectExpr("CASE WHEN (key IS NOT NULL) THEN split(key, '^')[0] ELSE key END as key", "value");
        }
        else {
            return spark
                    .read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("subscribe", getKafkaTopicNames(ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.topic"), kafkaConfig))
                    .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetNumRetries"))
                    .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetRetryIntervalMs"))
                    .option("failOnDataLoss", ConfigProvider.retrieveConfigValue(kafkaConfig, "spark.streamingKafkaFailOnDataLoss"))
                    .option("startingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.startingOffsets") ? kafkaConfig.get("kafka").get("startingOffsets").toString().replaceAll("^\"|\"$", "") : "earliest")
                    .option("endingOffsets", ConfigProvider.hasConfigValue(kafkaConfig, "kafka.endingOffsets") ? kafkaConfig.get("kafka").get("endingOffsets").toString().replaceAll("^\"|\"$", "") : "latest")
                    .load()
                    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    .selectExpr("CASE WHEN (key IS NOT NULL) THEN split(key, '^')[0] ELSE key END as key", "value");
        }
    }

    public Dataset readKafkaBatchWithSchema(JsonNode kafkaConfig) throws Exception {
        return readKafkaBatch(kafkaConfig)
                .select(from_json(col("value"), configProvider.readSchema(ConfigProvider.retrieveConfigValue(kafkaConfig, "schemaName"))).as("data"), col("value").as("raw")).select("data.*", "raw");
    }

    public StreamingQuery streamToKafka(Dataset<Row> dataset, JsonNode kafkaConfig) throws Exception {
        String topic = Checkpoint.constructKafkaTopic(kafkaConfig);
        String checkpoint = Checkpoint.constructCheckpoint(kafkaConfig);

        String is_ssl = ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.ssl");

        if(is_ssl.equals("true")) {

            return constructKafkaKeyValue(dataset, kafkaConfig)
                    .writeStream()
                    .format("kafka")
                    .option("kafka.security.protocol", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.securityProtocol"))
                    .option("kafka.sasl.kerberos.service.name", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.kerberosServiceName"))
                    .option("kafka.ssl.truststore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.truststore"))
                    .option("kafka.ssl.truststore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.trustStorePassword"))
                    .option("kafka.ssl.keystore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keystore"))
                    .option("kafka.ssl.keystore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keyStorePassword"))
                    .option("kafka.ssl.key.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.sslPassword"))
                    .option("groupIdPrefix",ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.groupIdPrefix"))
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("topic", getKafkaTopicNames(topic, kafkaConfig))
                    .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retries"))
                    .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retryBackoffMs"))
                    .option("kafka.metadata.fetch.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.metadataFetchTimeoutMs"))
                    .option("kafka.linger.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.lingerMs"))
                    .option("kafka.batch.size", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.batchSize"))
                    .option("kafka.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.timeoutMs"))
                    .option("kafka.request.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.requestTimeoutMs"))
                    .option("kafka.max.request.size", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.maxRequestSize"))
                    .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetNumRetries"))
                    .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetRetryIntervalMs"))
                    .trigger(ProcessingTime(ConfigProvider.retrieveConfigValue(kafkaConfig, "spark.streamngTriggerWindow")))
                    .queryName("sinkToKafka_" + checkpoint)
                    .start();

        }

        else {

            return constructKafkaKeyValue(dataset, kafkaConfig)
                    .writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("topic", getKafkaTopicNames(topic, kafkaConfig))
                    .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retries"))
                    .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retryBackoffMs"))
                    .option("kafka.metadata.fetch.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.metadataFetchTimeoutMs"))
                    .option("kafka.linger.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.lingerMs"))
                    .option("kafka.batch.size", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.batchSize"))
                    .option("kafka.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.timeoutMs"))
                    .option("kafka.request.timeout.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.requestTimeoutMs"))
                    .option("kafka.max.request.size", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.maxRequestSize"))
                    .option("fetchOffset.numRetries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetNumRetries"))
                    .option("fetchOffset.retryIntervalMs", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.fetchOffsetRetryIntervalMs"))
                    .trigger(ProcessingTime(ConfigProvider.retrieveConfigValue(kafkaConfig, "spark.streamngTriggerWindow")))
                    .queryName("sinkToKafka_" + checkpoint)
                    .start();
        }
    }

    //used for scala code. incase of type eraser, using extra parameter to distinguish --- 2019-07-23 Norman He@cisco
    public void batchToKafka(Dataset<Row> dataset, JsonNode kafkaConfig, boolean diff) throws Exception {
        String is_ssl = ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.ssl");

        if(is_ssl.equals("true")) {
            System.out.println("Inserting the data into the kafka topic -- SASL");
            constructKafkaKeyValue(dataset, kafkaConfig)
                    .write()
                    .format("kafka")
                    .option("kafka.security.protocol", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.securityProtocol"))
                    .option("kafka.sasl.kerberos.service.name", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.kerberosServiceName"))
                    .option("kafka.ssl.truststore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.truststore"))
                    .option("kafka.ssl.truststore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.trustStorePassword"))
                    .option("kafka.ssl.keystore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keystore"))
                    .option("kafka.ssl.keystore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keyStorePassword"))
                    .option("kafka.ssl.key.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.sslPassword"))
                    .option("groupIdPrefix",ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.groupIdPrefix"))
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("topic", getKafkaTopicNames(Checkpoint.constructKafkaTopic(kafkaConfig), kafkaConfig))
                    .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retries"))
                    .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retryBackoffMs"))
                    .save();
        }

         else {
            constructKafkaKeyValue(dataset, kafkaConfig)
                    .write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("topic", getKafkaTopicNames(Checkpoint.constructKafkaTopic(kafkaConfig), kafkaConfig))
                    .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retries"))
                    .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retryBackoffMs"))
                    .save();
        }
    }

    public void batchToKafka(Dataset dataset, JsonNode kafkaConfig) throws Exception {

        String is_ssl = ConfigProvider.retrieveConfigValue(kafkaConfig,"kafka.ssl");

        if(is_ssl.equals("true")) {
            System.out.println("Inserting the data into the kafka topic -- SASL");
            constructKafkaKeyValue(dataset, kafkaConfig)
                    .write()
                    .format("kafka")
                    .option("kafka.security.protocol", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.securityProtocol"))
                    .option("kafka.sasl.kerberos.service.name", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.kerberosServiceName"))
                    .option("kafka.ssl.truststore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.truststore"))
                    .option("kafka.ssl.truststore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.trustStorePassword"))
                    .option("kafka.ssl.keystore.location", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keystore"))
                    .option("kafka.ssl.keystore.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.keyStorePassword"))
                    .option("groupIdPrefix",ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.groupIdPrefix"))
                    .option("kafka.ssl.key.password", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.sslPassword"))
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("topic", getKafkaTopicNames(Checkpoint.constructKafkaTopic(kafkaConfig), kafkaConfig))
                    .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retries"))
                    .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retryBackoffMs"))
                    .save();
        }

        else {
            constructKafkaKeyValue(dataset, kafkaConfig)
                    .write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.broker"))
                    .option("topic", getKafkaTopicNames(Checkpoint.constructKafkaTopic(kafkaConfig), kafkaConfig))
                    .option("kafka.retries", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retries"))
                    .option("kafka.retry.backoff.ms", ConfigProvider.retrieveConfigValue(kafkaConfig, "kafka.retryBackoffMs"))
                    .save();
        }
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

    private String getKafkaTopicNames(String topics, JsonNode kafkaConfig) throws Exception{
        boolean useTopicPrefix = !ConfigProvider.hasConfigValue(kafkaConfig, "kafka.useTopicPrefix") || kafkaConfig.get("kafka").get("useTopicPrefix").asBoolean();

        if(!useTopicPrefix) return topics;
        topics = topics.replaceAll("\\s","");
        String[] topicNames = topics.split(",");
        for(int i=0; i < topicNames.length; i++){
            topicNames[i] = configProvider.retrieveConfigValue(kafkaConfig, "kafka.env") + configProvider.retrieveConfigValue(kafkaConfig,"kafka.topicPrefix") + topicNames[i] + configProvider.retrieveConfigValue(kafkaConfig,"kafka.topicPostfix");
        }
        return String.join(",", topicNames);
    }

}

