package util

import java.io.File

import com.typesafe.config.ConfigFactory
class Constants(confFile: String) extends Serializable {
  val myConfigFile = new File(confFile)

  val conf =  ConfigFactory.parseFile(myConfigFile)

  def CassandraHosts    = conf.getString("cassandra.host")
  def CassandraKeySpace = conf.getString("cassandra.keyspace")
  def CassandraCluster  = conf.getString("cassandra.cluster")
  def CassandraUsername = conf.getString("cassandra.username")
  def CassandraPassword = conf.getString("cassandra.password")
  def CassandraTableData = conf.getString("cassandra.detailsTableName")
  def CassandraTableAgg = conf.getString("cassandra.aggregatesTableName")

  def kafkaInputTopic = conf.getString("kafka.input.topic")
  def kafkaInputBroker = conf.getString("kafka.input.broker")
  def kafkaOutputBroker = conf.getString("kafka.output.broker")
  def kafkaMonitorBroker = conf.getString("kafka.monitor.broker")
  def kafkaMonitorTopic = conf.getString("kafka.monitor.topic")

  def streamngKafkaMaxRatePerPartition = conf.getString("spark.streamngKafkaMaxRatePerPartition")
  def streamingBackpressureInitialRate = conf.getString("spark.streamingBackpressureInitialRate")
  def streamingBackpressureEnabled = conf.getString("spark.streamingBackpressureEnabled")
  def streamingStopGracefullyOnShutdown = conf.getString("spark.streamingStopGracefullyOnShutdown")
  def streamngTriggerWindow = conf.getString("spark.streamngTriggerWindow")
  def checkpointLocation = conf.getString("spark.checkpointLocation")
  def outputLocation = conf.getString("spark.outputLocation")
  def cassandraOutputConsistencyLevel = conf.getString("spark.cassandraOutputConsistencyLevel")
  def logLevel = conf.getString("spark.logLevel")
  def kafkaMaxOffsetsPerTrigger = conf.getString("kafka.maxOffsetsPerTrigger")
  def kafkaProducerRetries = conf.getString("kafka.retries")
  def kafkaRetryBackoffMs = conf.getString("kafka.retryBackoffMs")
  def kafkaFetchOffsetNumRetries = conf.getString("kafka.fetchOffsetNumRetries")
  def kafkaFetchOffsetRetryIntervalMs = conf.getString("kafka.fetchOffsetRetryIntervalMs")
  def kafkaMetadataFetchTimeoutMs = conf.getString("kafka.metadataFetchTimeoutMs")
}
