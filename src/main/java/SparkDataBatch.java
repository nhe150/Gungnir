import org.apache.commons.cli.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import util.Constants;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class SparkDataBatch implements Serializable{
    private SparkSession spark;
    private TableProcessor tableProcessor;
    private String intputPath;
    private String startDate;
    private String writeMode;
    private String period;
    private Constants constants;

    public SparkDataBatch(String appName, Constants constants){
        this.constants = constants;
        this.spark = createSparkSession(appName, constants);
        this.tableProcessor = new TableProcessor(spark);
        this.writeMode = "append";
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option config = new Option("c", "config", true, "config file");
        config.setRequired(true);
        options.addOption(config);

        Option job = new Option("j", "job", true, "job name");
        job.setRequired(true);
        options.addOption(job);

        Option startDate = new Option("s", "startDate", true, "start date of data to be processed");
        startDate.setRequired(false);
        options.addOption(startDate);

        Option input = new Option("i", "input", true, "input data path");
        input.setRequired(false);
        options.addOption(input);

        Option writeMode = new Option("w", "writeMode", true, "save mode for data output");
        input.setRequired(false);
        options.addOption(writeMode);

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
            formatter.printHelp("SparkDataBatch", options);

            System.exit(1);
            return;
        }

        String jobName = cmd.getOptionValue("job");
        String aggPeriod = cmd.getOptionValue("period");
        String configFile = cmd.getOptionValue("config");

        SparkDataBatch app = new SparkDataBatch(jobName, new Constants(configFile));
        app.tableProcessor.setAggregatePeriod(aggPeriod);
        app.startDate = cmd.getOptionValue("startDate");
        app.intputPath = cmd.getOptionValue("input");
        app.period = aggPeriod;
        if(cmd.getOptionValue("writeMode")!= null) app.writeMode = cmd.getOptionValue("writeMode");

        app.run(jobName);
    }

    private SparkSession createSparkSession(String appName, Constants constants){
        SparkSession spark = SparkSession.builder()
//                .config("spark.cores.max", "6")
                .config("spark.cassandra.connection.host", constants.CassandraHosts())
                .config("spark.cassandra.auth.username", constants.CassandraUsername())
                .config("spark.cassandra.auth.password", constants.CassandraPassword())
//                .config("spark.cassandra.output.consistency.level", constants.cassandraOutputConsistencyLevel())
                .config("spark.hadoop.io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec")
                .appName(appName).getOrCreate();

        spark.sparkContext().setLogLevel(constants.logLevel());
        return spark;
    }

    private void run(String jobName) throws Exception {
        switch (jobName){
            case "sparkData":
                sparkData();
            case "splitData":
                if(startDate != null) {
                    if(writeMode.equals("overwrite")){
                        splitDataWithStartDate(intputPath, "conv,metrics,locus", startDate, SaveMode.Overwrite);
                    } else{
                        splitDataWithStartDate(intputPath, "conv,metrics,locus", startDate, SaveMode.Append);
                    }
                } else {
                    splitData(intputPath, "conv,metrics,locus");
                }
                break;
            case "splitDataText":
                splitDataText(intputPath, "conv,metrics,locus");
            case "details":
                details();
                break;
            case "aggregates":
                aggregates();
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
                activeUser("conv","locus");
                break;
            case "registeredEndpoint":
                registeredEndpoint("metrics");
                break;
            case "callQuality":
                callQuality("metrics");
                break;
//            case "callVolume":
//                callVolume("metrics");
//                break;
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
//            case "callVolumeCount":
//                callVolumeCount("callVolume");
//                break;
            case "callDurationCount":
                callDurationCount("callDuration");
                break;
            case "activeUserTopCount":
                activeUserTopCount("activeUser");
                break;
            case "topPoorQuality":
                topPoorQuality("callQuality");
                break;
            case "kohlsRawData":
                kohlsRawData("conv");
                break;
            case "test":
                test(intputPath);
                break;
            default:
                System.out.println("Invalid input for job name");
                System.exit(0);
        }
    }

    private void sparkData() throws Exception{
        splitData(intputPath, "conv,metrics,locus");
        details();
        aggregates();
    }

    private void details() throws Exception{
        fileUsed("conv");
        activeUser("conv","locus");
        registeredEndpoint("metrics");
        callQuality("metrics");
        callVolume("metrics");
        callDuration("locus");
        activeUserRollUp("activeUser");
        rtUser("activeUser");
    }

    private void aggregates() throws Exception{
        fileUsedCount("fileUsed");
        activeUserCount("activeUser");
        registeredEndpointCount("registeredEndpoint");
        callQualityCount("callQuality");
        callVolumeCount("callVolume");
        callDurationCount("callDuration");
        activeUserTopCount("activeUser");
        topPoorQuality("callQuality");
    }

    private void test(String input) {
        Dataset<String> inputData = spark.read().textFile(input);
        Dataset test = inputData.filter(new Functions.TestFilter());
        test.repartition(1).write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(constants.outputLocation() + "test_1143");
    }


    private void splitData(String input, String applist) throws Exception{
        Dataset<String> inputData = spark.read().textFile(input).cache();
        for (String s : applist.split(",")) {
            Dataset<Tuple2<String, String>> splitedData = inputData
                    .filter(new Functions.AppFilter(s))
                    .flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
            sinkToFileByKey(splitedData.toDF("key", "value"), "parquet", s, SaveMode.Append);
        }
    }


    private void splitDataText(String input, String applist) throws Exception{
        Dataset<String> inputData = spark.read().textFile(input).cache();
        for (String s : applist.split(",")) {
            Dataset<Tuple2<String, String>> splitedData = inputData
                    .filter(new Functions.AppFilter(s))
                    .flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
            splitedData.coalesce(1).write()
                    .mode(SaveMode.Append)
                    .format("text")
                    .save(constants.outputLocation() + s + "_textfile");
        }
    }

    private void splitDataWithStartDate(String input, String applist, String startDate, SaveMode saveMode) throws Exception{
        Dataset<String> inputData = spark.read().textFile(input).repartition(500).cache();
        for (String s : applist.split(",")) {
            Dataset<Tuple2<String, String>> splitedData = inputData
                    .filter(new Functions.AppFilter(s))
                    .flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
            sinkToFile(splitedData.toDF("key", "value").filter(col("key").equalTo(startDate)), "parquet", s, saveMode);
        }
    }

    private void callQuality(String input) throws Exception{
        Dataset<Row> raw = readRaw(input, tableProcessor.getSchema("/metrics.json"));
        Dataset<Row> callQuality = tableProcessor.callQuality(raw);

        sinkToFile(callQuality.selectExpr("pdate as key","to_json(struct(*)) AS value"), "parquet", "callQuality", SaveMode.Overwrite);

        writeToCassandra(callQuality, constants.CassandraTableData());
    }

    private void callVolume(String input) throws Exception{
        Dataset<Row> raw = readRaw(input, tableProcessor.getSchema("/metrics.json"));
        Dataset<Row> callVolume = tableProcessor.callVolume(raw);

        sinkToFile(callVolume.selectExpr("pdate as key","to_json(struct(*)) AS value"), "parquet", "callVolume", SaveMode.Overwrite);

        writeToCassandra(callVolume, constants.CassandraTableData());
    }

    private void callDuration(String input) throws Exception{
        Dataset<Row> raw = readRaw(input, tableProcessor.getSchema("/locus.json"));
        Dataset<Row> callDuration = tableProcessor.callDuration(raw);

        sinkToFile(callDuration.selectExpr("pdate as key","to_json(struct(*)) AS value"), "parquet", "callDuration", SaveMode.Overwrite);

        writeToCassandra(callDuration, constants.CassandraTableData());

    }

    private void fileUsed(String input) throws Exception{
        Dataset<Row> raw = readRaw(input, tableProcessor.getSchema("/conv.json"));
        Dataset<Row> fileUsed = tableProcessor.fileUsed(raw);

        sinkToFile(fileUsed.selectExpr("pdate as key","to_json(struct(*)) AS value"), "parquet", "fileUsed", SaveMode.Overwrite);

        writeToCassandra(fileUsed, constants.CassandraTableData());
    }

    //temp throw away job
    private void kohlsRawData(String input) throws Exception{
        Dataset<Row> raw = readRaw(input, tableProcessor.getSchema("/conv.json"));
//        Dataset test = spark.sql("select coalesce(orgId, SM.actor.orgId, SM.participant.orgId, SM.orgId, 'unknown') AS orgId from raw")
        Dataset test = raw.filter(col("SM.actor.orgId").equalTo("7cc93e58-2470-45e3-9812-856fed12c26e")).selectExpr("to_json(struct(*)) AS value");

        test.select("value").repartition(1).write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(constants.outputLocation() + "test");
    }

    private void activeUserRollUp(String input) throws Exception{
        Dataset<Row> activeUser = readDetails(input);
        Dataset<Row> activeUserRollUp = tableProcessor.activeUserRollUp(activeUser);
//        sinkToFile(activeUserRollUp.selectExpr("pdate as key","to_json(struct(*)) AS value"), "csv", "activeUserRollUp");

        writeToCassandra(activeUserRollUp, constants.CassandraTableData());
    }

    private void rtUser(String input) throws Exception{
        Dataset<Row> activeUser = readDetails(input);
        Dataset<Row> rtUser = tableProcessor.rtUser(activeUser);
//        sinkToFile(rtUser.selectExpr("pdate as key","to_json(struct(*)) AS value"), "csv", "rtUser");

        writeToCassandra(rtUser, constants.CassandraTableData());
    }

    private void activeUser(String conv, String locus) throws Exception{
        Dataset<Row> convRaw = readRaw(conv, tableProcessor.getSchema("/conv.json"));
        Dataset<Row> locusRaw = readRaw(locus, tableProcessor.getSchema("/conv.json"));
        Dataset<Row> raw = convRaw.union(locusRaw);
        Dataset<Row> activeUser = tableProcessor.activeUser(raw);

        sinkToFile(activeUser.selectExpr("pdate as key","to_json(struct(*)) AS value"), "parquet", "activeUser", SaveMode.Overwrite);
    }

    private void registeredEndpoint(String input) throws Exception{
        Dataset<Row> raw = readRaw(input, tableProcessor.getSchema("/metrics.json"));
        Dataset<Row> registeredEndpoint = tableProcessor.registeredEndpoint(raw);

        sinkToFile(registeredEndpoint.selectExpr("pdate as key","to_json(struct(*)) AS value"), "parquet", "registeredEndpoint", SaveMode.Overwrite);

        writeToCassandra(registeredEndpoint, constants.CassandraTableData());
    }

    private void callQualityCount(String input) throws Exception{
        Dataset<Row> callQuality = readDetails(input);
        Dataset<Row> callQualityTotalCount = tableProcessor.callQualityTotalCount(callQuality);
//        callQualityTotalCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "callQualityTotalCount");

        writeToCassandra(callQualityTotalCount,  constants.CassandraTableAgg());

        Dataset<Row> callQualityBadCount = tableProcessor.callQualityBadCount(callQuality);
//        callQualityBadCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "callQualityBadCount");

        writeToCassandra(callQualityBadCount,  constants.CassandraTableAgg());
    }

    private void callVolumeCount(String input) throws Exception{
        Dataset<Row> callVolume = readDetails(input);
        Dataset<Row> callVolumeCount = tableProcessor.callVolumeCount(callVolume);
//        callVolumeCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "callVolumeCount");

        writeToCassandra(callVolumeCount, constants.CassandraTableAgg());
    }

    private void callDurationCount(String input) throws Exception{
        Dataset<Row> callDuration = readDetails(input);
        Dataset<Row> callDurationCount = tableProcessor.callDurationCount(callDuration);
//        callDurationCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "callDurationCount");

        writeToCassandra(callDurationCount, constants.CassandraTableAgg());

        Dataset<Row> totalCallCount = tableProcessor.totalCallCount(callDuration);

        writeToCassandra(totalCallCount, constants.CassandraTableAgg());

    }

    private void fileUsedCount(String input) throws Exception{
        Dataset<Row> fileUsed = readDetails(input);
        Dataset<Row> fileUsedCount = tableProcessor.fileUsedCount(fileUsed);
//        fileUsedCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "fileUsedCount");

        writeToCassandra(fileUsedCount, constants.CassandraTableAgg());
    }

    private void registeredEndpointCount(String input) throws Exception{
        Dataset<Row> registeredEndpoint = readDetails(input);
        Dataset<Row> registeredEndpointCount = tableProcessor.registeredEndpointCount(registeredEndpoint);
//        registeredEndpointCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "registeredEndpointCount");

        writeToCassandra(registeredEndpointCount, constants.CassandraTableAgg());
    }

    private void activeUserCount(String input) throws Exception{
        Dataset<Row> activeUser = readDetails(input).cache();
        List<Dataset> activeUserCounts = tableProcessor.activeUserCounts(activeUser);
        for(Dataset activeUserCount: activeUserCounts){
//            activeUserCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                    .save(constants.outputLocation() + activeUserCount.columns()[3]);

            writeToCassandra(activeUserCount, constants.CassandraTableAgg());
        }
    }

    private void activeUserTopCount(String input) throws Exception{
        Dataset<Row> activeUser = readDetails(input);
        Dataset<Row> activeUserTopCount = tableProcessor.activeUserTopCount(activeUser);
//        activeUserTopCount.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "activeUserTopCount");

        writeToCassandra(activeUserTopCount, constants.CassandraTableAgg());
    }

    private void topPoorQuality(String input) throws Exception{
        Dataset<Row> callQuality = readDetails(input);
        Dataset<Row> topPoorQuality = tableProcessor.topPoorQuality(callQuality);
//        topPoorQuality.repartition(1).write().format("csv").mode(SaveMode.Overwrite)
//                .save(constants.outputLocation() + "topPoorQuality");

        writeToCassandra(topPoorQuality,  constants.CassandraTableAgg());
    }

    private void sinkToFileByKey(Dataset<Row> dataset, String format, String datasetName, SaveMode saveMode){
        dataset.write()
                .mode(saveMode)
                .partitionBy("key")
                .format(format)
                .save(constants.outputLocation() + datasetName);
    }

    private void sinkToFile(Dataset<Row> dataset, String format, String datasetName, SaveMode saveMode){
        dataset.write()
                .mode(saveMode)
                .format(format)
                .save(constants.outputLocation() + datasetName + "/key=" + startDate);
    }

    private Dataset<Row> readRaw(String source, StructType schema) throws Exception {
        return readFromParquet(constants.outputLocation() + source + "/key=" + startDate, schema);
    }

    private Dataset<Row> readDetails(String source) throws Exception {
        Dataset<Row> details = spark.createDataFrame(new ArrayList<>(), Schemas.schema.get(source));

        for(String date: aggregateDates(startDate, period)){
            details = details.union(readFromParquet(constants.outputLocation() + source + "/key=" + date, Schemas.schema.get(source)));
        }
        return details;
    }

    private Dataset<Row> readFromParquet(String input, StructType schema) throws Exception {
        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        if(!(fs.exists(new org.apache.hadoop.fs.Path(input)) || Files.exists(Paths.get(input)))) return spark.createDataFrame(new ArrayList<>(), schema);
        return spark
                .read()
                .parquet(input)
                .select(from_json(col("value"), schema).as("data"))
                .select("data.*");
    }

    private void writeToCassandra(Dataset<Row> dataset, String table){
        writeToCassandra(dataset, constants.CassandraKeySpace() ,table);
    }

    private void writeToCassandra(Dataset<Row> dataset, String keyspace, String table){
        columnNameToLowerCase(dataset).write()
                .mode(SaveMode.Append)
                .format("org.apache.spark.sql.cassandra")
                .options(cassandraConfig(keyspace, table))
                .save();
    }

    private Dataset<Row> columnNameToLowerCase(Dataset dataset){
        for (String col: dataset.columns()){
            dataset = dataset.withColumnRenamed(col, col.toLowerCase());
        }
        return dataset;
    }

    private Map<String, String> cassandraConfig(String keyspace, String table){
        Map<String, String> cassandraConfig = new HashMap<>();
        cassandraConfig.put("table", table);
        cassandraConfig.put("keyspace", keyspace);
        return cassandraConfig;
    }

    private List<String> aggregateDates(String  startDate, String period) throws Exception {
        List<String> dates = new ArrayList<>();
        dates.add(startDate);
        if(period == null) return dates;
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final Date date = format.parse(startDate);
        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, date.getYear());
        calendar.set(Calendar.MONTH, date.getMonth());

        int aggregateDuration=1;
        if(period.equals("weekly")) aggregateDuration = 7;
        if(period.equals("monthly")) aggregateDuration = calendar.getActualMaximum(Calendar.DATE);

        calendar.setTime(date);

        for(int i=2; i<=aggregateDuration; i++){
            calendar.add(Calendar.DAY_OF_YEAR, 1);
            dates.add(format.format(calendar.getTime()));
        }

        return dates;
    }

}
