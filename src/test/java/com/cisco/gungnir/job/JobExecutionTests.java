package com.cisco.gungnir.job;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.udf.Udfs;
import com.cisco.gungnir.util.kafka.EmbeddedSingleNodeKafkaCluster;
import com.datastax.driver.core.ResultSet;
import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.*;

import java.io.File;
import java.io.Serializable;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JobExecutionTests extends JavaDatasetSuiteBase implements Serializable {
    private SparkSession spark;
    private JobExecutor jobExecutor;

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster KafkaCLUSTER = new EmbeddedSingleNodeKafkaCluster();

    @ClassRule
    public static CassandraCQLUnit cassandra = new CassandraCQLUnit(new ClassPathCQLDataSet("create_cassandra_tables.cql","ks_global_pda"));

    @Before
    public void  before() throws Exception {
        this.spark = spark();
        ConfigProvider configProvider = new ConfigProvider(spark, "src/test/gungnir_job_repo/application.conf");
        this.jobExecutor = new JobExecutor(spark, configProvider);
        Udfs udfs = new Udfs(spark);
        udfs.registerFunctions();
    }

    @Test
    public void testSchemaForTransform() throws Exception {
        jobExecutor.execute("schemaForTransform", "batch");
    }

    @Test
    public void testSplitDataBatch() throws Exception {
        jobExecutor.execute("splitData", "batch");
    }

    @Test
    public void testSplitDataStream() throws Exception {
        jobExecutor.execute("splitData", "stream");
    }

    @Test
    public void testCallQualityBatch() throws Exception {
        jobExecutor.execute("callQuality", "batch");
    }

    @Test
    public void testCallDurationBatch() throws Exception {
        jobExecutor.execute("callDuration", "batch");
    }

    @Test
    public void testFileUsedBatch() throws Exception {
        jobExecutor.execute("fileUsed", "batch");
    }

    @Test
    public void testActiveUserBatch() throws Exception {
        jobExecutor.execute("activeUser", "batch");
    }

    @Test
    public void testRegisteredEndpointBatch() throws Exception {
        jobExecutor.execute("registeredEndpoint", "batch");
    }

    @Test
    public void testAutoLicenseBatch() throws Exception {
        jobExecutor.execute("autoLicense", "batch");
    }

    @Test
    public void testTopPoorQualityBatch() throws Exception {
        jobExecutor.execute("topPoorQuality", "batch");
    }

    @Test
    public void testTopUserBatch() throws Exception {
        jobExecutor.execute("topUser", "batch");
    }

    @Test
    public void testCallQualityStream() throws Exception {
        jobExecutor.execute("callQuality", "stream");
    }

    @Test
    public void testCallDurationStream() throws Exception {
        jobExecutor.execute("callDuration", "stream");
    }

    @Test
    public void testFileUsedStream() throws Exception {
        jobExecutor.execute("fileUsed", "stream");
    }

    @Test
    public void testActiveUserStream() throws Exception {
        jobExecutor.execute("activeUser", "stream");
    }

    @Test
    public void testRegisteredEndpointStream() throws Exception {
        jobExecutor.execute("registeredEndpoint", "stream");
    }

    @Test
    public void testAutoLicenseStream() throws Exception {
        jobExecutor.execute("autoLicense", "stream");
    }

    @Test
    public void testActiveUserFileBatch() throws Exception {
        jobExecutor.execute("activeUserFile", "batch");
    }

    @Test
    public void testAutoLicenseFileBatch() throws Exception {
        jobExecutor.execute("autoLicenseFile", "batch");
    }

    @Test
    public void testCallDurationFileBatch() throws Exception {
        jobExecutor.execute("callDurationFile", "batch");
    }

    @Test
    public void testCallQualityFileBatch() throws Exception {
        jobExecutor.execute("callQualityFile", "batch");
    }

    @Test
    public void testFileUsedFileBatch() throws Exception {
        jobExecutor.execute("fileUsedFile", "batch");
    }

    @Test
    public void testRegisteredEndpointFileBatch() throws Exception {
        jobExecutor.execute("registeredEndpointFile", "batch");
    }

    @Test
    public void testSplitDataFileBatch() throws Exception {
        jobExecutor.execute("splitDataFile", "batch");
    }

    @Test
    public void testWriteDataToKafkaBatch() throws Exception {
        jobExecutor.execute("writeDataToKafka", "batch");
    }

    @Test
    public void testWriteDataToKafkaStream() throws Exception {
        jobExecutor.execute("writeDataToKafka", "stream");
    }

    @Test
    public void testWriteDataToCassandraBatch() throws Exception {
        jobExecutor.execute("writeDataToCassandra", "batch");
    }

    @Test
    public void testWriteDataToCassandraStream() throws Exception {
        jobExecutor.execute("writeDataToCassandra", "stream");
    }

    @Test
    public void testCassandraDataMigrationBatch() throws Exception {
        jobExecutor.execute("cassandraDataMigration", "batch");
    }

    @Test
    public void testDeleteDataFromCassandra() throws Exception {
        jobExecutor.execute("writeDataToCassandra", "batch");
        ResultSet resultBeforeDelete = cassandra.session.execute("select * from spark_data");
        assertNotNull(resultBeforeDelete.iterator().next().getString("pdate"));
        jobExecutor.execute("deleteDataFromCassandra", "batch");
        ResultSet resultAfterDelete = cassandra.session.execute("select * from spark_data");
        assertNull(resultAfterDelete.iterator().next());
    }

    @Test
    public void testSparkDataMonitor() throws Exception {
        ConfigProvider configProvider = new ConfigProvider(spark, "src/test/gungnir_job_repo/sparkDataMonitor.conf");
        SparkDataMonitor sparkDataMonitor = new SparkDataMonitor(spark, configProvider);
        sparkDataMonitor.run(null,"0.3", "*");
    }

    @After
    public void after()throws Exception{

    }

    @AfterClass
    public static void afterClass() throws Exception {
        FileUtils.deleteDirectory(new File("src/test/checkpoint"));
    }

}