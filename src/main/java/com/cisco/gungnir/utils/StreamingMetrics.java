package com.cisco.gungnir.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StreamingMetrics extends StreamingQueryListener implements Serializable {
    private KafkaProducer<String, String> kafkaProducer;
    private String topic;
    private Map<UUID, String> queryIdMap;
    public StreamingMetrics(String servers, String topic){
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", servers);
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties);
        this.topic = topic;
        this.queryIdMap = new HashMap<>();
    }

    @Override
    public void onQueryStarted(QueryStartedEvent queryStarted) {
        queryIdMap.put(queryStarted.id(), queryStarted.name());
        String message = "{\"id\":\"" + queryStarted.id() + "\"," + "\"name\":\"" + queryStarted.name() + "\",\"timestamp\":\"" + Instant.now().toString() + "\"}";
        String fullMessage = "{\"started\":" + message + "}";
        kafkaProducer.send(new ProducerRecord(topic, fullMessage));
    }
    @Override
    public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
        String message = "{\"id\":\"" + queryTerminated.id() + "\"," + "\"name\":\"" + queryIdMap.get(queryTerminated.id()) + "\",\"timestamp\":\"" + Instant.now().toString() + "\"}";
        String fullMessage = "{\"terminated\":" + message + "}";
        kafkaProducer.send(new ProducerRecord(topic, fullMessage));
    }
    @Override
    public void onQueryProgress(StreamingQueryListener.QueryProgressEvent queryProgress) {
        String message = "{\"progress\":" + queryProgress.progress().json() + "}";
        kafkaProducer.send(new ProducerRecord(topic, message));
    }
}
