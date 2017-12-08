import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

public class PipelineTest extends JavaDatasetSuiteBase implements Serializable {
    private SparkSession spark;
    private TableProcessor tableProcessor;
    private Dataset expected;
    private Dataset result;

    @Before
    public void createTableProcessor(){
        this.spark = spark();
        this.tableProcessor = new TableProcessor(spark);
    }

    @Test
    public void testPreProcess() throws Exception {
        Dataset input = spark.read().textFile("src/test/data/raw.json");

        expected = spark.read().text("src/test/data/conv.json");

        result = input.filter(new Functions.AppFilter("conv")).flatMap(new Functions.PreProcess(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("key", "value").select("value");
    }

    @Test
    public void testCallQuality() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = read("src/test/data/callQuality.json", Schemas.callQualitySchema).drop("dataid");

        result = tableProcessor.callQuality(input).drop("dataid");
    }

    @Test
    public void testCallVolume() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = read("src/test/data/callVolume.json", Schemas.callVolumeSchema).drop("dataid");

        result = tableProcessor.callVolume(input).drop("dataid");
    }

    @Test
    public void testCallDuration() throws Exception {
        Dataset input = read("src/test/data/locus.json", tableProcessor.getSchema("/locus.json"));

        expected = read("src/test/data/callDuration.json", Schemas.callDurationSchema).drop("dataid");

        result = tableProcessor.callDuration(input).drop("dataid");
    }

    @Test
    public void testRegisteredEndpoint() throws Exception {
        Dataset input = read("src/test/data/metrics.json", tableProcessor.getSchema("/metrics.json"));

        expected = read("src/test/data/registeredEndpoint.json", Schemas.registeredEndpointSchema).drop("dataid");

        result = tableProcessor.registeredEndpoint(input).drop("dataid");
    }

    @Test
    public void testActiveUser() throws Exception {
        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));

        expected = read("src/test/data/activeUser.json", Schemas.activeUserSchema).drop("dataid");

        result = tableProcessor.activeUser(input).drop("dataid");
    }

    @Test
    public void testFileUsed() throws Exception {
        Dataset input = read("src/test/data/conv.json", tableProcessor.getSchema("/conv.json"));

        expected = read("src/test/data/fileUsed.json", Schemas.fileUsedSchema).drop("dataid");

        result = tableProcessor.fileUsed(input).drop("dataid");
    }

    @Test
    public void testCallQualityCount() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = read("src/test/data/callQualityCount.json", Schemas.callQualityCountSchema);

        result = tableProcessor.callQualityTotalCount(input);

    }

    @Test
    public void testCallVolumeCount() throws Exception {
        Dataset input = read("src/test/data/callVolume.json", Schemas.callVolumeSchema);

        expected = read("src/test/data/callVolumeCount.json", Schemas.callVolumeCountSchema);

        result = tableProcessor.callVolumeCount(input);
    }

    @Test
    public void testCallDurationCount() throws Exception {
        Dataset input = read("src/test/data/callDuration.json", Schemas.callDurationSchema);

        expected = read("src/test/data/callDurationCount.json", Schemas.callDurationCountSchema);

        result = tableProcessor.callDurationCount(input);
    }

    @Test
    public void testRegisteredEndpointCount() throws Exception {
        Dataset input = read("src/test/data/registeredEndpoint.json", Schemas.registeredEndpointSchema);

        expected = read("src/test/data/registeredEndpointCount.json", Schemas.registeredEndpointCountSchema);

        result = tableProcessor.registeredEndpointCount(input);
    }

    @Test
    public void testFileUsedCount() throws Exception {
        Dataset input = read("src/test/data/fileUsed.json", Schemas.fileUsedSchema);

        expected = read("src/test/data/fileUsedCount.json", Schemas.fileUsedCountSchema);

        result = tableProcessor.fileUsedCount(input);
    }

    @Test
    public void testActiveUserCount() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/activeUserCount.json", Schemas.activeUserCountSchema);

        result = tableProcessor.activeUserCounts(input).get(1);
    }

    @Test
    public void testTopUser() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/topUser.json", Schemas.topUserSchema);

        result = tableProcessor.activeUserTopCount(input);
    }

    @Test
    public void testTopPoorQuality() throws Exception {
        Dataset input = read("src/test/data/callQuality.json", Schemas.callQualitySchema);

        expected = read("src/test/data/topPoorQuality.json", Schemas.topPoorQualitySchema);

        result = tableProcessor.topPoorQuality(input);
    }

    @Test
    public void testActiveUserRollUp() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/activeUserRollUp.json", Schemas.activeUserRollUpSchema);

        result = tableProcessor.activeUserRollUp(input);
    }

    @Test
    public void testRtUser() throws Exception {
        Dataset input = read("src/test/data/activeUser.json", Schemas.activeUserSchema);

        expected = read("src/test/data/rtUser.json", Schemas.rtUserSchema);

        result = tableProcessor.rtUser(input);
    }

    @After
    public void verify() {
//        System.out.println(result.selectExpr("to_json(struct(*)) AS value").first());
        assertDatasetEquals(expected, result);
    }

    private Dataset read(String input, StructType schema){
        return spark
                .read()
                .schema(schema)
                .json(input);
    }
}