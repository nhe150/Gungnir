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
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.commons.cli.*;
import sql.Queries;
import util.Constants;

public class SparkDataStreaming implements Serializable {
    private Constants constants;
    private SparkSession spark;
    private TableProcessor tableProcessor;
    private CassandraConnector connector;
    private String dataFile;

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
    }

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

        Option dataFileOpt = new Option("f", "file", true, "datafile path");
        dataFileOpt.setRequired(false);
        options.addOption(dataFileOpt);

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
        String dataFile = cmd.getOptionValue("file");

        SparkDataStreaming streamingApp = new SparkDataStreaming(jobName, new Constants(configFile));
        streamingApp.tableProcessor.setAggregatePeriod(aggPeriod);
        streamingApp.setDataFile(dataFile);

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
//                .config("spark.cassandra.output.consistency.level", constants.cassandraOutputConsistencyLevel())
                .config("spark.sql.streaming.checkpointLocation", constants.checkpointLocation())
                .config("spark.streaming.stopGracefullyOnShutdown", constants.streamingStopGracefullyOnShutdown())
                .config("spark.streaming.backpressure.enabled", constants.streamingBackpressureEnabled())
                .appName(appName)
                .getOrCreate();

        spark.sparkContext().setLogLevel(constants.logLevel());
        return spark;
    }

    private void run(String jobName) throws Exception {
        switch (jobName){
            case "splitData":
                splitData(constants.kafkaInputTopic(), "conv,metrics,locus");
                break;
            case "saveToFile":
                sinkTopicsToFile("conv,metrics,locus,fileUsed,activeUser,registeredEndpoint,callQuality,callDuration", "parquet");
                break;
            case "saveToCassandra":
                sinkDetailsToCassandra("fileUsed,registeredEndpoint,callQuality,callDuration");
                break;
            case "details":
                fileUsed("conv");
                activeUser("conv,locus");
                registeredEndpoint("metrics");
                callQuality("metrics");
                callDuration("locus");
                rtUser("activeUser");
                break;
            case "aggregates":
                fileUsedCount("fileUsed");
                activeUserCount("activeUser");
                registeredEndpointCount("registeredEndpoint");
                callQualityCount("callQuality");
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
            case "activeUserData":
                activeUserData();
                break;
            case "activeUser":
                activeUser("conv,locus");
                break;
            case "registeredEndpoint":
                registeredEndpoint("metrics");
                break;
            case "registeredEndpointData":
                registeredEndpointData();
                break;
            case "callQuality":
                callQuality("metrics");
                break;
            case "callDuration":
                callDuration("locus");
                break;
            case "fileUsedCount":
                fileUsedCount("fileUsed");
                break;
            case "fileUsedData":
                fileUsedData();
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
            case "callDurationCount":
                callDurationCount("callDuration");
                break;
            case "rawDataCount":
                convCount("conv");
                metricsCount("metrics");
                locusCount("locus");
                break;
            case "convCount":
                convCount("conv");
                break;
            case "metricsCount":
                metricsCount("metrics");
                break;
            case "locusCount":
                locusCount("locus");
                break;
            default:
                throw new IllegalArgumentException("Invalid input for job name");
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

    /**
     *
     * @param topicList
     * @param format "csv" , "parquet" etc
     */
    private void sinkTopicsToFile(String topicList, String format) {
        for (String s : topicList.split(",")) {
            Dataset<Row> inputStream = readFromKafka(s);
            sinkToFileByKey(inputStream.selectExpr("split(key, '_')[0] as key", "value"), format, s);
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

    private void registeredEndpointData() throws Exception{
         syncDataInfoKafka(Queries.registeredEndpointData(), "re", "registeredEndpoint");
    }

    private void fileUsedData() throws Exception {
        syncDataInfoKafka(Queries.fileUsedData(), "fu", "fileUsed");
    }

    private void activeUserData() {
        syncDataInfoKafka(Queries.activeUserData(), "au", "activeUser");
    }

    private void syncDataInfoKafka(String query, String viewTable, String topic) {
        Dataset<Row> df0 = spark.read().option("header","true").csv(dataFile);
        df0.printSchema();

        Dataset<Row> df = spark.readStream().schema(df0.schema()).csv(dataFile);

        df.createOrReplaceTempView(viewTable);
        Dataset<Row> au = spark.sql(query);


        au.printSchema();
        sinkToKafka(au.selectExpr("pdate as key","to_json(struct(*)) AS value"), topic);
    }

    private void activeUser(String input) throws Exception{
        Dataset<Row> raw = readFromKafkaWithSchema(input, tableProcessor.getSchema("/conv.json"));
        Dataset<Row> activeUser = tableProcessor.activeUser(raw);
        sinkToKafka(activeUser.selectExpr("pdate as key","to_json(struct(*)) AS value"),"activeUser");
    }

    private void callQualityCount(String input) throws Exception{
        Dataset<Row> callQuality = readFromKafkaWithSchema(input);
        Dataset<Row> callQualityTotalCount = tableProcessor.callQualityTotalCount(callQuality);
        sinkToCassandra(callQualityTotalCount, constants.CassandraTableAgg(), "update",   "callQualityTotalCount_" + tableProcessor.getAggregatePeriod());

        Dataset<Row> callQualityBadCount = tableProcessor.callQualityBadCount(callQuality);
        sinkToCassandra(callQualityBadCount, constants.CassandraTableAgg(), "update",   "callQualityBadCount_" + tableProcessor.getAggregatePeriod());
    }

    private void callDurationCount(String input) throws Exception{
        Dataset<Row> callDuration = readFromKafkaWithSchema(input);
        Dataset<Row> callDurationCount = tableProcessor.callDurationCount(callDuration);
        sinkToCassandra(callDurationCount, constants.CassandraTableAgg(), "update", "callDurationCount_" + tableProcessor.getAggregatePeriod());
        Dataset<Row> totalCallCount = tableProcessor.totalCallCount(callDuration);
        sinkToCassandra(totalCallCount, constants.CassandraTableAgg(), "update", "totalCallCount_" + tableProcessor.getAggregatePeriod());
    }

    private void fileUsedCount(String input) throws Exception{
        Dataset<Row> fileUsed = readFromKafkaWithSchema(input);
        Dataset<Row> fileUsedCount = tableProcessor.fileUsedCount(fileUsed);
        sinkToCassandra(fileUsedCount, constants.CassandraTableAgg(), "update", "fileUsedCount_" + tableProcessor.getAggregatePeriod());
    }

    private void registeredEndpointCount(String input) throws Exception{
        Dataset<Row> registeredEndpoint = readFromKafkaWithSchema(input);
        Dataset<Row> registeredEndpointCount = tableProcessor.registeredEndpointCount(registeredEndpoint);
        sinkToCassandra(registeredEndpointCount, constants.CassandraTableAgg(), "update", "registeredEndpointCount_" + tableProcessor.getAggregatePeriod());
    }

    private void activeUserCount(String input) throws Exception{
        Dataset<Row> activeUser = readFromKafkaWithSchema(input);
        List<Dataset> activeUserCounts = tableProcessor.activeUserCounts(activeUser);
        for(Dataset activeUserCount: activeUserCounts){
            sinkToCassandra(activeUserCount, constants.CassandraTableAgg(), "update", activeUserCount.columns()[3] + "_" + tableProcessor.getAggregatePeriod());
        }
    }

    private void activeUserRollUp(String input) throws Exception{
        Dataset<Row> activeUser = readFromKafkaWithSchema(input);
        Dataset<Row> activeUserRollUp = tableProcessor.activeUserRollUp(activeUser);
        sinkToCassandra(activeUserRollUp, constants.CassandraTableData(), "update", "activeUserRollUp");
    }

    private void rtUser(String input) throws Exception{
        Dataset<Row> activeUser = readFromKafkaWithSchema(input);
        Dataset<Row> rtUser = tableProcessor.rtUser(activeUser);
        sinkToCassandra(rtUser, constants.CassandraTableData(), "append", "rtUser");
    }

    private void convCount(String input) throws Exception{
        Dataset<Row> conv = readFromKafkaWithSchema(input, tableProcessor.getSchema("/conv.json"));
        Dataset<Row> convCount = tableProcessor.convCount(conv);
        sinkToCassandra(convCount, constants.CassandraTableAgg(), "update", "convCount_" + tableProcessor.getAggregatePeriod());
    }

    private void metricsCount(String input) throws Exception{
        Dataset<Row> metrics = readFromKafkaWithSchema(input, tableProcessor.getSchema("/metrics.json"));
        Dataset<Row> metricsCount = tableProcessor.metricsCount(metrics);
        sinkToCassandra(metricsCount, constants.CassandraTableAgg(), "update", "metricsCount_" + tableProcessor.getAggregatePeriod());
    }

    private void locusCount(String input) throws Exception{
        Dataset<Row> locus = readFromKafkaWithSchema(input, tableProcessor.getSchema("/locus.json"));
        Dataset<Row> locusCount = tableProcessor.locusCount(locus);
        sinkToCassandra(locusCount, constants.CassandraTableAgg(), "update", "locusCount_" + tableProcessor.getAggregatePeriod());
    }

    private StreamingQuery sinkToKafka(Dataset<Row> dataset, String topic){
        return dataset
                .selectExpr("CONCAT(key, '_', uuid(key)) as key", "value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", constants.kafkaOutputBroker())
                .option("topic", getKafkaTopicNames(topic))
                .option("kafka.retries", constants.kafkaProducerRetries())
                .option("kafka.retry.backoff.ms", constants.kafkaRetryBackoffMs())
                .option("kafka.metadata.fetch.timeout.ms", constants.kafkaMetadataFetchTimeoutMs())
                .option("kafka.linger.ms", constants.kafkaLingerMs())
                .option("kafka.batch.size", constants.kafkaBatchSize())
                .option("kafka.timeout.ms", constants.kafkaTimeoutMs())
                .option("kafka.request.timeout.ms", constants.kafkaRequestTimeoutMs())
                .option("kafka.max.request.size", constants.kafkaMaxRequestSize())
                .option("fetchOffset.numRetries", constants.kafkaFetchOffsetNumRetries())
                .option("fetchOffset.retryIntervalMs", constants.kafkaFetchOffsetRetryIntervalMs())
                .trigger(ProcessingTime(constants.streamngTriggerWindow()))
                .queryName("sinkToKafka_" + topic)
                .start();
    }

    private Dataset<Row> readFromKafka(String topics){
        return readFromKafka(getKafkaTopicNames(topics), constants.kafkaOutputBroker());
    }

    private Dataset<Row> readFromKafka(String topics, String bootstrap_ervers){
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_ervers)
                .option("subscribe", topics)
                .option("maxOffsetsPerTrigger", constants.kafkaMaxOffsetsPerTrigger())
                .option("fetchOffset.numRetries", constants.kafkaFetchOffsetNumRetries())
                .option("fetchOffset.retryIntervalMs", constants.kafkaFetchOffsetRetryIntervalMs())
                .option("failOnDataLoss", constants.streamingKafkaFailOnDataLoss())
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    }

    private Dataset<Row> readFromKafkaWithSchema(String topics){
        return readFromKafkaWithSchema(topics, Schemas.schema.get(topics));
    }

    private Dataset<Row> readFromKafkaWithSchema(String topics, StructType schema){
        return readFromKafka(topics)
                .filter(col("key").notEqual(Functions.PreProcess.BAD_DATA_LABLE))
                .select(from_json(col("value"), schema).as("data")).select("data.*");
    }

    private String getKafkaTopicNames(String topics) {
        String[] topicNames = topics.split(",");
        for(int i=0; i < topicNames.length; i++){
            topicNames[i] = constants.kafkaTopicPrefix() + topicNames[i] + constants.kafkaTopicPostfix();
        }
        return String.join(",", topicNames);
    }

    private StreamingQuery sinkToFileByKey(Dataset<Row> dataset, String format, String queryName){
        return dataset
                .writeStream()
                .queryName("sinkToFile_" + queryName)
                .partitionBy("key")
                .outputMode("append")
                .trigger(ProcessingTime(constants.saveToFileTriggerWindow()))
                .format(format)
                .option("path", constants.outputLocation() + queryName).start();
    }

    private StreamingQuery sinkToCassandra(Dataset<Row> dataset, String tablename, String outputMode, String queryName){
        return sinkToCassandra(dataset, constants.CassandraKeySpace(), tablename, outputMode, queryName);
    }

    private StreamingQuery sinkToCassandra(Dataset<Row> dataset, String keySpace, String tablename, String outputMode, String queryName){
        dataset.printSchema();
        return dataset
                .writeStream()
                .outputMode(outputMode)
                .foreach((ForeachWriter) new CassandraForeachWriter(connector, keySpace, tablename, dataset.schema()))
                .trigger(ProcessingTime(constants.streamngTriggerWindow()))
                .queryName("sinkToCassandra_" + queryName)
                .start();
    }

    public class CassandraForeachWriter extends ForeachWriter<GenericRowWithSchema> {
        private CassandraConnector connector;
        private String keySpace;
        private String tablename;
        private StructType schema;
        /**
         * ToDo: better resource sharing
         */
        private Session session;
        public CassandraForeachWriter(CassandraConnector connector, String keySpace, String tablename, StructType schema){
            this.connector = connector;
            this.keySpace = keySpace;
            this.tablename = tablename;
            this.schema = schema;

        }
        @Override
        public boolean open(long partitionId, long version) {
            session = connector.openSession();
            return true;
        }

        /**
         * handle wrong schema case so the system don't blow up
         * @param values
         * @return
         */
        private boolean isAllNull(String[] values)
        {
            for( String so : values){
                if (so == null || so.equals("'null'") || so.equals("null"))
                {
                   continue;

                }else {
                    return false;
                }
            }

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

            if( isAllNull(values)){
                return;
            }

            String fields = "(" + String.join(", ", schema.fieldNames()).toLowerCase() + ")";

            String fieldValues =  "(" + String.join(", ", values) + ")";

            String statement = "insert into " + keySpace + "." + tablename + " " + fields + " values" + fieldValues;

            //System.out.println(statement);
            session.execute(statement);
        }

        @Override
        public void close(Throwable errorOrNull) {
            if( session != null ){
                session.close();
            }
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
