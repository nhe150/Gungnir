import com.datastax.driver.core.Session;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.col;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.commons.cli.*;
import util.Constants;

public class SparkDataStreaming implements Serializable {
    private Constants constants;
    private SparkSession spark;
    private TableProcessor tableProcessor;
    private CassandraConnector connector;

    public SparkDataStreaming(String appName, Constants constants){
        this.constants = constants;
        this.spark = createSparkSession(appName, constants);
        this.tableProcessor = new TableProcessor(spark);
        this.connector = CassandraConnector.apply(spark.sparkContext().getConf());
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option config = new Option("c", "config", true, "config file");
        config.setRequired(true);
        options.addOption(config);

        Option job = new Option("j", "job", true, "job name");
        job.setRequired(true);
        options.addOption(job);

        Option period = new Option("p", "period", true, "aggregation period");
        period.setRequired(false);
        options.addOption(period);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("SparkDataStreaming", options);

            System.exit(1);
            return;
        }


        String jobName = cmd.getOptionValue("job");
        String aggPeriod = cmd.getOptionValue("period");
        String configFile = cmd.getOptionValue("config");

        SparkDataStreaming streamingApp = new SparkDataStreaming(jobName, new Constants(configFile));
        streamingApp.tableProcessor.setAggregatePeriod(aggPeriod);

        streamingApp.spark.streams().addListener(new KafkaMetrics(streamingApp.constants.kafkaMonitorBroker(), streamingApp.constants.kafkaMonitorTopic()));

        streamingApp.run(jobName);

        streamingApp.spark.streams().awaitAnyTermination();
    }

    private SparkSession createSparkSession(String appName, Constants constants){
        SparkSession spark = SparkSession.builder()
//                .config("spark.cores.max", "1")
//                .config("spark.deploy.defaultCores", "1")
                .config("spark.cassandra.connection.host", constants.CassandraHosts())
                .config("spark.cassandra.auth.username", constants.CassandraUsername())
                .config("spark.cassandra.auth.password", constants.CassandraPassword())
                .config("spark.cassandra.output.consistency.level", constants.cassandraOutputConsistencyLevel())
                .config("spark.sql.streaming.checkpointLocation", constants.checkpointLocation())
                .config("spark.streaming.stopGracefullyOnShutdown", constants.streamingStopGracefullyOnShutdown())
                .config("spark.streaming.backpressure.enabled", constants.streamingBackpressureEnabled())
                .appName(appName).getOrCreate();

        spark.sparkContext().setLogLevel(constants.logLevel());
        return spark;
    }

    private void run(String jobName) throws Exception {
        switch (jobName){
            case "splitData":
                splitData(constants.kafkaInputTopic(), "conv,metrics,locus");
                break;
            case "saveToFile":
                sinkTopicsToFile("conv,metrics,locus,fileUsed,activeUser,registeredEndpoint,callQuality,callVolume,callDuration");
                break;
            case "saveToCassandra":
                sinkDetailsToCassandra("fileUsed,registeredEndpoint,callQuality,callVolume,callDuration");
                break;
            case "details":
                fileUsed("conv");
                activeUser("conv,locus");
                registeredEndpoint("metrics");
                callQuality("metrics");
                callVolume("metrics");
                callDuration("locus");
                activeUserRollUp("activeUser");
                rtUser("activeUser");
                break;
            case "aggregates":
                fileUsedCount("fileUsed");
                activeUserCount("activeUser");
                registeredEndpointCount("registeredEndpoint");
                callQualityCount("callQuality");
                callVolumeCount("callVolume");
                callDurationCount("callDuration");
                break;
            case "activeUserRollUp":
                activeUserRollUp("activeUser");
                break;
            case "rtUser":
                rtUser("activeUser");
                break;
            case "fileUsed":
                fileUsed("conv");
                break;
            case "activeUser":
                activeUser("conv,locus");
                break;
            case "registeredEndpoint":
                registeredEndpoint("metrics");
                break;
            case "callQuality":
                callQuality("metrics");
                break;
            case "callVolume":
                callVolume("metrics");
                break;
            case "callDuration":
                callDuration("locus");
                break;
            case "fileUsedCount":
                fileUsedCount("fileUsed");
                break;
            case "activeUserCount":
                activeUserCount("activeUser");
                break;
            case "registeredEndpointCount":
                registeredEndpointCount("registeredEndpoint");
                break;
            case "callQualityCount":
                callQualityCount("callQuality");
                break;
            case "callVolumeCount":
                callVolumeCount("callVolume");
                break;
            case "callDurationCount":
                callDurationCount("callDuration");
                break;
            default:
                System.out.println("Invalid input for job name");
                System.exit(0);
        }
    }

    private void splitData(String input, String applist) throws Exception{
        Dataset<String> inputStream = readFromKafka(input, constants.kafkaInputBroker()).select("value").as(Encoders.STRING());

        for (String s : applist.split(",")) {
            Dataset<Tuple2<String, String>> splitedData = inputStream
                    .filter(new Functions.AppFilter(s))
                    .flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
            sinkToKafka(splitedData.toDF("key", "value"), s);
        }
    }

    private void sinkTopicsToFile(String topicList) {
        for (String s : topicList.split(",")) {
            Dataset<Row> inputStream = readFromKafka(s);
            sinkToFileByKey(inputStream.toDF("key", "value"), "parquet", s);
        }
    }

    private void sinkDetailsToCassandra(String topicList) throws Exception{
        for (String s : topicList.split(",")) {
            Dataset<Row> inputStream = readFromKafkaWithSchema(s);
            sinkToCassandra(inputStream, constants.CassandraTableData(), "append", s);
        }
    }

    private void callQuality(String input) throws Exception{
        Dataset<Row> raw = readFromKafkaWithSchema(input, tableProcessor.getSchema("/metrics.json"));
        Dataset<Row> callQuality = tableProcessor.callQuality(raw);
        sinkToKafka(callQuality.selectExpr("pdate as key","to_json(struct(*)) AS value"), "callQuality");
    }

    private void callVolume(String input) throws Exception{
        Dataset<Row> raw = readFromKafkaWithSchema(input, tableProcessor.getSchema("/metrics.json"));
        Dataset<Row> callVolume = tableProcessor.callVolume(raw);
        sinkToKafka(callVolume.selectExpr("pdate as key","to_json(struct(*)) AS value"), "callVolume");
    }

    private void callDuration(String input) throws Exception{
        Dataset<Row> raw = readFromKafkaWithSchema(input, tableProcessor.getSchema("/locus.json"));
        Dataset<Row> callDuration = tableProcessor.callDuration(raw);
        sinkToKafka(callDuration.selectExpr("pdate as key","to_json(struct(*)) AS value"), "callDuration");
    }

    private void fileUsed(String input) throws Exception{
        Dataset<Row> raw = readFromKafkaWithSchema(input, tableProcessor.getSchema("/conv.json"));
        Dataset<Row> fileUsed = tableProcessor.fileUsed(raw);
        sinkToKafka(fileUsed.selectExpr("pdate as key","to_json(struct(*)) AS value"), "fileUsed");
    }

    private void registeredEndpoint(String input) throws Exception{
        Dataset<Row> raw = readFromKafkaWithSchema(input, tableProcessor.getSchema("/metrics.json"));
        Dataset<Row> registeredEndpoint = tableProcessor.registeredEndpoint(raw);
        sinkToKafka(registeredEndpoint.selectExpr("pdate as key","to_json(struct(*)) AS value"),"registeredEndpoint");
    }

    private void activeUser(String input) throws Exception{
        Dataset<Row> raw = readFromKafkaWithSchema(input, tableProcessor.getSchema("/conv.json"));
        Dataset<Row> activeUser = tableProcessor.activeUser(raw);
        sinkToKafka(activeUser.selectExpr("pdate as key","to_json(struct(*)) AS value"),"activeUser");
    }

    private void callQualityCount(String input) throws Exception{
        Dataset<Row> callQuality = readFromKafkaWithSchema(input);
        Dataset<Row> callQualityTotalCount = tableProcessor.callQualityTotalCount(callQuality);
//        callQualityTotalCount.writeStream().format("console").outputMode("update").start();
        sinkToCassandra(callQualityTotalCount, constants.CassandraTableAgg(), "update",   "callQualityTotalCount_" + tableProcessor.getAggregatePeriod());

        Dataset<Row> callQualityBadCount = tableProcessor.callQualityBadCount(callQuality);
//        callQualityBadCount.writeStream().format("console").outputMode("update").start();
        sinkToCassandra(callQualityBadCount, constants.CassandraTableAgg(), "update",   "callQualityBadCount_" + tableProcessor.getAggregatePeriod());
    }

    private void callVolumeCount(String input) throws Exception{
        Dataset<Row> callVolume = readFromKafkaWithSchema(input);
        Dataset<Row> callVolumeCount = tableProcessor.callVolumeCount(callVolume);
//        callVolumeCount.writeStream().format("console").outputMode("update").start();
        sinkToCassandra(callVolumeCount, constants.CassandraTableAgg(), "update", "callVolumeCount_" + tableProcessor.getAggregatePeriod());
    }

    private void callDurationCount(String input) throws Exception{
        Dataset<Row> callDuration = readFromKafkaWithSchema(input);
        Dataset<Row> callDurationCount = tableProcessor.callDurationCount(callDuration);
//        callDurationCount.writeStream().format("console").outputMode("update").start();
        sinkToCassandra(callDurationCount, constants.CassandraTableAgg(), "update", "callDurationCount_" + tableProcessor.getAggregatePeriod());
    }

    private void fileUsedCount(String input) throws Exception{
        Dataset<Row> fileUsed = readFromKafkaWithSchema(input);
        Dataset<Row> fileUsedCount = tableProcessor.fileUsedCount(fileUsed);
//        fileUsedCount.writeStream().format("console").outputMode("update").start();
        sinkToCassandra(fileUsedCount, constants.CassandraTableAgg(), "update", "fileUsedCount_" + tableProcessor.getAggregatePeriod());
    }

    private void registeredEndpointCount(String input) throws Exception{
        Dataset<Row> registeredEndpoint = readFromKafkaWithSchema(input);
        Dataset<Row> registeredEndpointCount = tableProcessor.registeredEndpointCount(registeredEndpoint);
//        registeredEndpointCount.writeStream().format("console").outputMode("update").start();
        sinkToCassandra(registeredEndpointCount, constants.CassandraTableAgg(), "update", "registeredEndpointCount_" + tableProcessor.getAggregatePeriod());
    }

    private void activeUserCount(String input) throws Exception{
        Dataset<Row> activeUser = readFromKafkaWithSchema(input);
        List<Dataset> activeUserCounts = tableProcessor.activeUserCounts(activeUser);
        for(Dataset activeUserCount: activeUserCounts){
//            activeUserCount.writeStream().format("console").outputMode("update").start();
            sinkToCassandra(activeUserCount, constants.CassandraTableAgg(), "update", activeUserCount.columns()[3] + "_" + tableProcessor.getAggregatePeriod());
        }
    }

    private void activeUserRollUp(String input) throws Exception{
        Dataset<Row> activeUser = readFromKafkaWithSchema(input);
        Dataset<Row> activeUserRollUp = tableProcessor.activeUserRollUp(activeUser);
//      activeUserRollUp.writeStream().format("console").outputMode("update").start();
        sinkToCassandra(activeUserRollUp, constants.CassandraTableData(), "update", "activeUserRollUp");
    }

    private void rtUser(String input) throws Exception{
        Dataset<Row> activeUser = readFromKafkaWithSchema(input);
        Dataset<Row> rtUser = tableProcessor.rtUser(activeUser);
//      rtUser.writeStream().format("console").outputMode("append").start();
        sinkToCassandra(rtUser, constants.CassandraTableData(), "append", "rtUser");
    }

    private StreamingQuery sinkToKafka(Dataset<Row> dataset, String topic){
        return dataset
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", constants.kafkaOutputBroker())
                .option("topic", topic)
                .option("kafka.retries", constants.kafkaProducerRetries())
                .option("kafka.retry.backoff.ms", constants.kafkaRetryBackoffMs())
                .queryName("sinkToKafka_" + topic)
                .start();
    }

    private Dataset<Row> readFromKafka(String topics){
        return readFromKafka(topics, constants.kafkaOutputBroker());
    }

    private Dataset<Row> readFromKafka(String topics, String bootstrap_ervers){
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_ervers)
                .option("subscribe", topics)
                .option("maxOffsetsPerTrigger", constants.kafkaMaxOffsetsPerTrigger())
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    }

    private Dataset<Row> readFromKafkaWithSchema(String topics){
        return readFromKafkaWithSchema(topics, Schemas.schema.get(topics));
    }

    private Dataset<Row> readFromKafkaWithSchema(String topics, StructType schema){
        return readFromKafka(topics)
                .select(from_json(col("value"), schema).as("data")).select("data.*");
    }

    private StreamingQuery sinkToFileByKey(Dataset<Row> dataset, String format, String queryName){
        return dataset
                .repartition(col("key"))
                .writeStream()
                .queryName("sinkToFile_" + queryName)
                .partitionBy("key")
                .outputMode("append")
                .format(format)
                .option("path", constants.outputLocation() + queryName).start();
    }

    private StreamingQuery sinkToCassandra(Dataset<Row> dataset, String tablename, String outputMode, String queryName){
        return sinkToCassandra(dataset, constants.CassandraKeySpace(), tablename, outputMode, queryName);
    }

    private StreamingQuery sinkToCassandra(Dataset<Row> dataset, String keySpace, String tablename, String outputMode, String queryName){
        return dataset
                .writeStream()
                .outputMode(outputMode)
                .foreach((ForeachWriter) new CassandraForeachWriter(connector, keySpace, tablename, dataset.schema()))
                .queryName("sinkToCassandra_" + queryName)
                .start();
    }

    public class CassandraForeachWriter extends ForeachWriter<GenericRowWithSchema> {
        private CassandraConnector connector;
        private String keySpace;
        private String tablename;
        private StructType schema;
        public CassandraForeachWriter(CassandraConnector connector, String keySpace, String tablename, StructType schema){
            this.connector = connector;
            this.keySpace = keySpace;
            this.tablename = tablename;
            this.schema = schema;
        }
        @Override
        public boolean open(long partitionId, long version) {
            // open connection
            return true;
        }

        @Override
        public void process(GenericRowWithSchema value) {
            // write string to connection
            StructField[] structField =schema.fields();
            String[] values = new String[structField.length];
            for(int i=0; i<structField.length; i++){
                DataType type = structField[i].dataType();
                values[i] = type.sameType(DataTypes.StringType)
                        || type.sameType(DataTypes.TimestampType) ? "'" + value.get(i) + "'" : value.get(i) + "";
            }

            String fields = "(" + String.join(", ", schema.fieldNames()).toLowerCase() + ")";
            String fieldValues =  "(" + String.join(", ", values) + ")";

            String statement = "insert into " + keySpace + "." + tablename + " " + fields + " values" + fieldValues;
            try (Session session = connector.openSession()) {
                session.execute(statement);
            }
        }

        @Override
        public void close(Throwable errorOrNull) {
            // close the connection
        }
    }

    private static class KafkaMetrics extends StreamingQueryListener {
        private KafkaProducer<String, String> kafkaProducer;
        private String topic;
        private Map<UUID, String> queryIdMap;
        public KafkaMetrics(String servers, String topic){
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
}
