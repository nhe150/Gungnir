package com.cisco.gungnir.jobExecution;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.job.SparkDataMonitor;
import com.cisco.gungnir.util.kafka.EmbeddedSingleNodeKafkaCluster;
import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.SparkSession;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.Serializable;


public class SparkDataMonitorTests extends JavaDatasetSuiteBase implements Serializable {
    private SparkSession spark;
    private SparkDataMonitor sparkDataMonitor;

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster KafkaCLUSTER = new EmbeddedSingleNodeKafkaCluster();

    @ClassRule
    public static CassandraCQLUnit cassandra = new CassandraCQLUnit(new ClassPathCQLDataSet("create_cassandra_tables.cql", "ks_global_pda"));

    @Before
    public void before() throws Exception {
        this.spark = spark();
        ConfigProvider configProvider = new ConfigProvider(spark, "src/test/gungnir_job_repo/sparkDataMonitor.conf");
        this.sparkDataMonitor = new SparkDataMonitor(spark, configProvider);
    }

    @Test
    public void testSplitDataBatch() throws Exception {
        sparkDataMonitor.run("2018-05-11", "0.3", "*");
    }

}
