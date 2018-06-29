import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.apache.spark.sql.functions.lit;

public class PipelineTest extends JavaDatasetSuiteBase implements Serializable {
    private SparkSession spark;
    private TableProcessor tableProcessor;
    private Dataset expected;
    private Dataset result;

    @Before
    public void createTableProcessor(){
        this.spark = spark();
        this.tableProcessor = new TableProcessor(spark);
        spark.sparkContext().setLogLevel("WARN");
    }

    @Test
    public void testPreProcess() throws Exception {
        Dataset input = spark.read().textFile("src/test/data/beforePreprocess.json");

        expected = spark.read().text("src/test/data/afterPreprocess.json");

        result = input.filter(new Functions.AppFilter("conv")).flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("key", "value").select("value");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testPreProcessMetrics() throws Exception {
        Dataset input = spark.read().textFile("src/test/data/beforePreprocess.json");

        expected = spark.read().text("src/test/data/afterPreprocessMetrics.json");

        result = input.filter(new Functions.AppFilter("metrics")).flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("key", "value").select("value");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testProcessAtlasData() throws Exception {
        Dataset input = read("src/test/data/atlas.json", tableProcessor.getSchema("/atlas.json"));
        expected = readExpected("src/test/data/atlasOutput.json", Schemas.autoLicenseSchema).drop("dataid");
        result = tableProcessor.autoLicense(input).drop("dataid");
        assertDatasetEquals(expected, result);
    }


    @Test
    public void testCallQuality() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = readExpected("src/test/data/callQuality.json", Schemas.callQualitySchema).drop("dataid");

        result = tableProcessor.callQuality(input).drop("dataid").drop("raw");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallDuration() throws Exception {
        Dataset input = read("src/test/data/locus.json", tableProcessor.getSchema("/locus.json"));

        expected = readExpected("src/test/data/callDuration.json", Schemas.callDurationSchema).drop("dataid");

        result = tableProcessor.callDuration(input).drop("dataid").drop("raw");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testRegisteredEndpoint() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = readExpected("src/test/data/registeredEndpoint.json", Schemas.registeredEndpointSchema).drop("dataid");

        result = tableProcessor.registeredEndpoint(input).drop("dataid").drop("raw");

        assertDatasetEquals(expected, result);
    }

//    @Test
//    public void testActiveUser() throws Exception {
//        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));
//
//        expected = readExpected("src/test/data/activeUser.json", Schemas.activeUserSchema).drop("dataid");
//
//        result = tableProcessor.activeUser(input).drop("dataid").drop("raw");
//
//        assertDatasetEquals(expected, result);
//    }

    @Test
    public void testFileUsed() throws Exception {
        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));

        expected = readExpected("src/test/data/fileUsed.json", Schemas.fileUsedSchema).drop("dataid");

        result = tableProcessor.fileUsed(input).drop("dataid").drop("raw");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallQualityBadCount() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = readExpected("src/test/data/callQualityBadCount.json", Schemas.callQualityBadCountSchema);

        result = tableProcessor.callQualityBadCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallQualityCount() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = readExpected("src/test/data/callQualityCount.json", Schemas.callQualityCountSchema);

        result = tableProcessor.callQualityTotalCount(input);

        assertDatasetEquals(expected, result);
    }

//    @Test
//    public void testCallDurationCount() throws Exception {
//        Dataset input = read("src/test/data/callDuration.json", Schemas.callDurationSchema);
//
//        expected = readExpected("src/test/data/callDurationCount.json", Schemas.callDurationCountSchema);
//
//        result = tableProcessor.callDurationCount(input);
//        result.show(false);
//
//        assertDatasetEquals(expected, result);
//    }

    @Test
    public void testRegisteredEndpointCount() throws Exception {
        Dataset input = read("src/test/data/registeredEndpoint.json", Schemas.registeredEndpointSchema);

        expected = readExpected("src/test/data/registeredEndpointCount.json", Schemas.registeredEndpointCountSchema);

        result = tableProcessor.registeredEndpointCount(input);

        assertDatasetEquals(expected, result);
    }

//    @Test
//    public void testFileUsedCount() throws Exception {
//        Dataset input = read("src/test/data/fileUsed.json", Schemas.fileUsedSchema);
//
//        expected = readExpected("src/test/data/fileUsedCount.json", Schemas.fileUsedCountSchema);
//
//        result = tableProcessor.fileUsedCount(input);
//        result.show(false);
//
//
//        assertDatasetEquals(expected, result);
//    }

//    @Test
//    public void testTotalCallCount() throws Exception {
//        Dataset input = read("src/test/data/callDuration.json", Schemas.callDurationSchema);
//
//        expected = readExpected("src/test/data/totalCallCount.json", Schemas.totalCallCountSchema);
//
//        result = tableProcessor.totalCallCount(input);
//        result.show(false);
//
//
//        assertDatasetEquals(expected, result);
//    }

//    @Test
//    public void testActiveUserCount() throws Exception {
//        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);
//
//        expected = readExpected("src/test/data/activeUserCount.json", Schemas.activeUserCountSchema);
//
//        result = tableProcessor.activeUserCounts(input).get(0);
//
//        assertDatasetEquals(expected, result);
//    }

//    @Test
//    public void testConvCount() throws Exception {
//        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));
//
//        expected = readExpected("src/test/data/convCount.json", Schemas.activeUserCountSchema);
//
//        result = tableProcessor.convCount(input);
//        result.show(false);
//
//        assertDatasetEquals(expected, result);
//    }

    @Test
    public void testMetricsCount() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = readExpected("src/test/data/metricsCount.json", Schemas.activeUserCountSchema);

        result = tableProcessor.metricsCount(input);

        assertDatasetEquals(expected, result);
    }

//    @Test
//    public void testLocusCount() throws Exception {
//        Dataset input = read("src/test/data/locus.json", tableProcessor.getSchema("/locus.json"));
//
//        expected = readExpected("src/test/data/locusCount.json", Schemas.activeUserCountSchema);
//
//        result = tableProcessor.locusCount(input);
//        result.show(false);
//
//        assertDatasetEquals(expected, result);
//    }

    @Test
    public void testTopUser() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = readExpected("src/test/data/topUser.json", Schemas.topUserSchema);

        result = tableProcessor.activeUserTopCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testTopPoorQuality() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = readExpected("src/test/data/topPoorQuality.json", Schemas.topPoorQualitySchema);

        result = tableProcessor.topPoorQuality(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testActiveUserRollUp() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = readExpected("src/test/data/activeUserRollUp.json", Schemas.activeUserRollUpSchema);

        result = tableProcessor.activeUserRollUp(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testRtUser() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = readExpected("src/test/data/rtUser.json", Schemas.rtUserSchema);

        result = tableProcessor.rtUser(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testSetAggregatePeriodToDaily() throws Exception {
        tableProcessor.setAggregatePeriod("daily");
        assertEquals("daily", tableProcessor.getAggregatePeriod());
    }

    @Test
    public void testSetAggregatePeriodToWeekly() throws Exception {
        tableProcessor.setAggregatePeriod("weekly");
        assertEquals("weekly", tableProcessor.getAggregatePeriod());
    }

    @Test
    public void testSetAggregatePeriodToMonthly() throws Exception {
        tableProcessor.setAggregatePeriod("monthly");
        assertEquals("monthly", tableProcessor.getAggregatePeriod());
    }

    @Test
    public void testSetAggregatePeriodToNull() throws Exception {
        tableProcessor.setAggregatePeriod(null);
        assertEquals("daily", tableProcessor.getAggregatePeriod());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetAggregatePeriodToSomethingelse() throws Exception {
        tableProcessor.setAggregatePeriod("something else");
    }

    private Dataset read(String input, StructType schema){
        return spark
                .read()
                //PERMISSIVE mode -> save malformed cells in a row as "null" to be filtered out later.
                //DROPMALFORMED mode -> toss the bad rows.
                .option("multiline", true).option("mode", "PERMISSIVE")
                .schema(schema)
                .json(input).withColumn("raw", lit(""));
    }

    private Dataset readExpected(String input, StructType schema){
        return TableProcessor.setTimestampField(spark
                .read()
                //PERMISSIVE mode -> save malformed cells in a row as "null" to be filtered out later.
                //DROPMALFORMED mode -> toss the bad rows.
                .option("multiline", true).option("mode", "PERMISSIVE")
                .schema(schema)
                .json(input), "time_stamp");
    }
}