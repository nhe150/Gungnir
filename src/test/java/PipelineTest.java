import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class PipelineTest extends JavaDatasetSuiteBase implements Serializable {
    private SparkSession spark;
    private TableProcessor tableProcessor;
    private Dataset expected;
    private Dataset result;

    @Before
    public void createTableProcessor(){
        this.spark = spark();
        spark.sqlContext().setConf("spark.sql.session.timeZone", "GMT");
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
    public void testPreProcessAtlasData() throws Exception {
        Dataset input = spark.read().textFile("src/test/data/rawAtlas.json");
        expected = spark.read().text("src/test/data/atlas.json");
        result = input.filter(new Functions.AppFilter("atlas")).flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("key", "value").select("value");
        assertDatasetEquals(expected, result);
    }


    @Test
    public void testProcessAtlasData() throws Exception {
        Dataset input = read("src/test/data/atlas.json", tableProcessor.getSchema("/atlas.json"));
        expected = read("src/test/data/atlasOutput.json", Schemas.autoLicenseSchema).drop("dataid");
        result = tableProcessor.autoLicense(input).drop("dataid");
        assertDatasetEquals(expected, result);
    }


    @Test
    public void testCallQuality() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = read("src/test/data/callQuality.json", Schemas.callQualitySchema).drop("dataid");

        result = tableProcessor.callQuality(input).drop("dataid");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallVolume() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = read("src/test/data/callVolume.json", Schemas.callVolumeSchema).drop("dataid");

        result = tableProcessor.callVolume(input).drop("dataid");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallDuration() throws Exception {
        Dataset input = read("src/test/data/locus.json", tableProcessor.getSchema("/locus.json"));

        expected = read("src/test/data/callDuration.json", Schemas.callDurationSchema).drop("dataid");

        result = tableProcessor.callDuration(input).drop("dataid");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testRegisteredEndpoint() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = read("src/test/data/registeredEndpoint.json", Schemas.registeredEndpointSchema).drop("dataid");

        result = tableProcessor.registeredEndpoint(input).drop("dataid");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testActiveUser() throws Exception {
        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));

        expected = read("src/test/data/activeUser.json", Schemas.activeUserSchema).drop("dataid");

        result = tableProcessor.activeUser(input).drop("dataid");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testFileUsed() throws Exception {
        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));

        expected = read("src/test/data/fileUsed.json", Schemas.fileUsedSchema).drop("dataid");

        result = tableProcessor.fileUsed(input).drop("dataid");

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallVolumeCount() throws Exception {
        Dataset input = read("src/test/data/callVolume.json", Schemas.callVolumeSchema);

        expected = read("src/test/data/callVolumeCount.json", Schemas.callVolumeCountSchema);

        result = tableProcessor.callVolumeCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallQualityBadCount() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = read("src/test/data/callQualityBadCount.json", Schemas.callQualityBadCountSchema);

        result = tableProcessor.callQualityBadCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallQualityCount() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = read("src/test/data/callQualityCount.json", Schemas.callQualityCountSchema);

        result = tableProcessor.callQualityTotalCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testCallDurationCount() throws Exception {
        Dataset input = read("src/test/data/callDuration.json", Schemas.callDurationSchema);

        expected = read("src/test/data/callDurationCount.json", Schemas.callDurationCountSchema);

        result = tableProcessor.callDurationCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testRegisteredEndpointCount() throws Exception {
        Dataset input = read("src/test/data/registeredEndpoint.json", Schemas.registeredEndpointSchema);

        expected = read("src/test/data/registeredEndpointCount.json", Schemas.registeredEndpointCountSchema);

        result = tableProcessor.registeredEndpointCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testFileUsedCount() throws Exception {
        Dataset input = read("src/test/data/fileUsed.json", Schemas.fileUsedSchema);

        expected = read("src/test/data/fileUsedCount.json", Schemas.fileUsedCountSchema);

        result = tableProcessor.fileUsedCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testTotalCallCount() throws Exception {
        Dataset input = read("src/test/data/callDuration.json", Schemas.callDurationSchema);

        expected = read("src/test/data/totalCallCount.json", Schemas.totalCallCountSchema);

        result = tableProcessor.totalCallCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testActiveUserCount() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/activeUserCount.json", Schemas.activeUserCountSchema);

        result = tableProcessor.activeUserCounts(input).get(0);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testConvCount() throws Exception {
        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));

        expected = read("src/test/data/convCount.json", Schemas.activeUserCountSchema);

        result = tableProcessor.convCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testMetricsCount() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = read("src/test/data/metricsCount.json", Schemas.activeUserCountSchema);

        result = tableProcessor.metricsCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testLocusCount() throws Exception {
        Dataset input = read("src/test/data/locus.json", tableProcessor.getSchema("/locus.json"));

        expected = read("src/test/data/locusCount.json", Schemas.activeUserCountSchema);

        result = tableProcessor.locusCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testTopUser() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/topUser.json", Schemas.topUserSchema);

        result = tableProcessor.activeUserTopCount(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testTopPoorQuality() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = read("src/test/data/topPoorQuality.json", Schemas.topPoorQualitySchema);

        result = tableProcessor.topPoorQuality(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testActiveUserRollUp() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/activeUserRollUp.json", Schemas.activeUserRollUpSchema);

        result = tableProcessor.activeUserRollUp(input);

        assertDatasetEquals(expected, result);
    }

    @Test
    public void testRtUser() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/rtUser.json", Schemas.rtUserSchema);

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
                .json(input);
    }
}