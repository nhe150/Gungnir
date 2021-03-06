package com.cisco.gungnir.util.kafka;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig$;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance, 1 Kafka broker, and 1
 * Confluent Schema Registry instance.
 */
public class EmbeddedSingleNodeKafkaCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);
    private static final int DEFAULT_BROKER_PORT = 9092; // 0 results in a random port being selected

    private ZooKeeperEmbedded zookeeper;
    private KafkaEmbedded broker;
    private final Properties brokerConfig;

    /**
     * Creates and starts the cluster.
     */
    public EmbeddedSingleNodeKafkaCluster() {
        this(new Properties());
    }

    /**
     * Creates and starts the cluster.
     *
     * @param brokerConfig Additional broker configuration settings.
     */
    public EmbeddedSingleNodeKafkaCluster(Properties brokerConfig) {
        this.brokerConfig = new Properties();
        this.brokerConfig.putAll(brokerConfig);
    }

    /**
     * Creates and starts the cluster.
     */
    public void start() throws Exception {
        log.debug("Initiating embedded Kafka cluster startup");
        log.debug("Starting a ZooKeeper instance...");
        zookeeper = new ZooKeeperEmbedded();
        log.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

        Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
        log.debug("Starting a Kafka instance on port {} ...",
                effectiveBrokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
        broker = new KafkaEmbedded(effectiveBrokerConfig);
        log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
                broker.brokerList(), broker.zookeeperConnect());
    }

    private Properties effectiveBrokerConfigFrom(Properties brokerConfig, ZooKeeperEmbedded zookeeper) {
        Properties effectiveConfig = new Properties();
        effectiveConfig.putAll(brokerConfig);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
        effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);

        effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
        effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        return effectiveConfig;
    }

    @Override
    protected void before() throws Exception {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    /**
     * Stops the cluster.
     */
    public void stop() {
        if (broker != null) {
            broker.stop();
        }
        try {
            if (zookeeper != null) {
                zookeeper.stop();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
     *
     * You can use this to tell Kafka Streams applications, Kafka producers, and Kafka consumers (new
     * consumer API) how to connect to this cluster.
     */
    public String bootstrapServers() {
        return broker.brokerList();
    }

    /**
     * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
     * Example: `127.0.0.1:2181`.
     *
     * You can use this to e.g. tell Kafka consumers (old consumer API) how to connect to this
     * cluster.
     */
    public String zookeeperConnect() {
        return zookeeper.connectString();
    }

    /**
     * Creates a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(String topic) {
        createTopic(topic, 1, 1, new Properties());
    }

    /**
     * Creates a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(String topic, int partitions, int replication) {
        createTopic(topic, partitions, replication, new Properties());
    }

    /**
     * Creates a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    public void createTopic(String topic,
                            int partitions,
                            int replication,
                            Properties topicConfig) {
        broker.createTopic(topic, partitions, replication, topicConfig);
    }

}