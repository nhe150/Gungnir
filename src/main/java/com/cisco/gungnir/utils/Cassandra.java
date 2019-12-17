package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.DataFrameFunctions;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Some;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class Cassandra implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;
    private Map<String, String> cassandraConfig;

    public Cassandra(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
    }

    private JsonNode getCassandraConfig(JsonNode providedConfig) throws Exception {
        JsonNode merged = ConfigProvider.merge(configProvider.getAppConfig().deepCopy(), providedConfig);

        Map<String, String> cassandraConfigMap = new HashMap<>();
        cassandraConfigMap.put("table", ConfigProvider.retrieveConfigValue(merged, "cassandra.table"));
        cassandraConfigMap.put("keyspace", ConfigProvider.retrieveConfigValue(merged, "cassandra.keyspace"));
        cassandraConfigMap.put("spark.cassandra.connection.host", ConfigProvider.retrieveConfigValue(merged, "cassandra.host"));
        cassandraConfigMap.put("spark.cassandra.connection.port", ConfigProvider.retrieveConfigValue(merged, "cassandra.port"));
        cassandraConfigMap.put("spark.cassandra.auth.username", ConfigProvider.retrieveConfigValue(merged, "cassandra.username"));
        cassandraConfigMap.put("spark.cassandra.auth.password", ConfigProvider.retrieveConfigValue(merged, "cassandra.password"));
        cassandraConfigMap.put("spark.cassandra.output.consistency.level", ConfigProvider.retrieveConfigValue(merged, "cassandra.consistencyLevel"));
        cassandraConfigMap.put("spark.cassandra.input.consistency.level", ConfigProvider.retrieveConfigValue(merged,"cassandra.consistencyLevel"));
        cassandraConfigMap.put("spark.cassandra.read.timeout_ms", ConfigProvider.retrieveConfigValue(merged, "cassandra.readTimeout"));
        cassandraConfigMap.put("spark.cassandra.output.batch.grouping.key", ConfigProvider.retrieveConfigValue(merged, "cassandra.output_batch_grouping_key"));
        cassandraConfigMap.put("spark.cassandra.output.batch.grouping.buffer.size", ConfigProvider.retrieveConfigValue(merged, "cassandra.output_batch_grouping_buffer_size"));
        cassandraConfigMap.put("spark.cassandra.output.concurrent.writes", ConfigProvider.retrieveConfigValue(merged, "cassandra.output_concurrent_writes"));
        cassandraConfigMap.put("spark.cassandra.output.throughput_mb_per_sec", ConfigProvider.retrieveConfigValue(merged, "cassandra.output_throughput_mb_per_sec"));
        //configs for SSL
        cassandraConfigMap.put("spark.cassandra.connection.ssl.enabled", ConfigProvider.retrieveConfigValue(merged,
                "cassandra.ssl_enabled"));
        cassandraConfigMap.put("spark.cassandra.connection.ssl.trustStore.password",
                ConfigProvider.retrieveConfigValue(merged, "cassandra.ssl_trustStore_password"));
        cassandraConfigMap.put("spark.cassandra.connection.ssl.trustStore.path",
                ConfigProvider.retrieveConfigValue(merged, "cassandra.ssl_trustStore_path"));
        this.cassandraConfig = cassandraConfigMap;
        return merged;
    }

    public Dataset readFromCassandra(String processType, JsonNode providedConfig) throws Exception {
        getCassandraConfig(providedConfig);
        switch (processType) {
            case "batch":
                return readCassandraBatch(providedConfig);
            case "stream":
                throw new IllegalArgumentException("Data source org.apache.spark.sql.cassandra does not support streamed reading");
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for readFromCassandra");
        }
    }

    public void writeToCassandra(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if (dataset == null)
            throw new IllegalArgumentException("can't write to cassandra: the input dataset is NULL, please check previous query");
        dataset = dataset.drop("raw");
        JsonNode mergedConfig = getCassandraConfig(providedConfig);
        switch (processType) {
            case "batch":
                batchToCassandra(dataset, ConfigProvider.retrieveConfigValue(mergedConfig, "cassandra.saveMode"));
                break;
            case "stream":
                streamToCassandra(dataset, ConfigProvider.retrieveConfigValue(mergedConfig, "output"), ConfigProvider.retrieveConfigValue(mergedConfig, "cassandra.saveMode"));
                break;
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for writeToCassandra");
        }
    }

    public void deleteFromCassandra(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if (dataset == null) {
            throw new IllegalArgumentException("can't delete from cassandra: the input dataset is NULL, please check previous query");
        }

        getCassandraConfig(providedConfig);

        switch (processType) {
            case "batch":
                deleteFromCassandra(dataset, cassandraConfig.get("keyspace"), cassandraConfig.get("table"), providedConfig);
                break;
            case "stream":
                throw new IllegalArgumentException("Data source org.apache.spark.sql.cassandra does not support streamed delete");
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for readFromCassandra");
        }
    }

    /**
     * Need refract this part
     * @param conf
     * @return
     * @throws Exception
     */
    public Dataset readCassandraBatch(JsonNode conf) throws Exception {
        Dataset ds = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .options(cassandraConfig)
                .load();
        if (!ConfigProvider.hasConfigValue(conf, "date") && !ConfigProvider.hasConfigValue(conf, "month")) {
            return ds;
        }

        if (ConfigProvider.hasConfigValue(conf, "month")) {
            ds = ds.where(String.format("month = '%s'", ConfigProvider.retrieveConfigValue(conf, "month")));
        }

        if (!ConfigProvider.hasConfigValue(conf, "date")) {
            return ds;
        }

        Dataset result = null;
        String date = DateUtil.getDate(ConfigProvider.retrieveConfigValue(conf, "date"));
        System.out.println("cassconfig: " + conf.toString());
        System.out.println("date to workon: " + date);

        if (date != null) {
            String relation = ConfigProvider.retrieveConfigValue(conf, "relation");
            if (ConfigProvider.hasConfigValue(conf, "monthPartition")) {
                //seperate table
                String month = date.substring(0, 7);
                System.out.println("month = " + month);
                String sql = String.format("month = '%s' and pdate = '%s'", month, date);
                System.out.println("sqlstat: " + sql);
                result = ds.where(sql);
            } else {
                if (relation.contains(",")) {
                    String[] whereClauses = Util.buildWhereClauses(date, relation);

                    for (String whereC : whereClauses) {
                        System.out.println("build where clause:" + whereC);
                        if (result == null) {
                            result = ds.where(whereC);
                        } else {
                            result = result.union(ds.where(whereC));
                        }
                    }

                } else {
                    if( ConfigProvider.retrieveConfigValue(conf, "cassandra.table").equals("spark_agg_v2")){
                        result = ds.where(String.format("time_stamp = '%s' and relation_name = '%s'", date, relation));
                    }
                    else {
                        result = ds.where(String.format("pdate = '%s' and relation_name = '%s'", date, relation));
                    }
                }
            }
        }

        return result;
    }

    public void batchToCassandra(Dataset dataset, String saveMode) {
        columnNameToLowerCase(dataset).write()
                .mode(File.getSaveMode(saveMode))
                .format("org.apache.spark.sql.cassandra")
                .options(cassandraConfig)
                .save();
    }

    public StreamingQuery streamToCassandra(Dataset<Row> dataset, String queryName, String saveMode) throws Exception {
        return dataset
                .coalesce(100)
                .writeStream()
                .outputMode(saveMode)
                .foreach((ForeachWriter) new CassandraForeachWriter(getCassandraConnector(), cassandraConfig.get("keyspace"), cassandraConfig.get("table"), dataset.schema()))
                .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                .queryName("sinkToCassandra_" + queryName)
                .start();
    }

    private SparkConf getCassandraSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.cassandra.connection.host", cassandraConfig.get("spark.cassandra.connection.host"));
        sparkConf.set("spark.cassandra.connection.port", cassandraConfig.get("spark.cassandra.connection.port"));
        sparkConf.set("spark.cassandra.auth.username", cassandraConfig.get("spark.cassandra.auth.username"));
        sparkConf.set("spark.cassandra.auth.password", cassandraConfig.get("spark.cassandra.auth.password"));
        sparkConf.set("spark.cassandra.output.consistency.level", cassandraConfig.get("spark.cassandra.output.consistency.level"));
        sparkConf.set("spark.cassandra.input.consistency.level", cassandraConfig.get("spark.cassandra.input.consistency.level"));
        return sparkConf;
    }

    private CassandraConnector getCassandraConnector() {
        CassandraConnector connector = CassandraConnector.apply(getCassandraSparkConf());
        return connector;
    }

    private Dataset<Row> columnNameToLowerCase(Dataset dataset) {
        for (String col : dataset.columns()) {
            dataset = dataset.withColumnRenamed(col, col.toLowerCase());
        }
        return dataset;
    }

    public void createCassandraTable(Dataset dataset, String partitionKeyColumns, String clusteringKeyColumns) {
        DataFrameFunctions dataFrameFunctions = new DataFrameFunctions(dataset);

        partitionKeyColumns = partitionKeyColumns.replaceAll("\\s", "");
        String[] partitionKeyList = partitionKeyColumns.split(",");

        clusteringKeyColumns = clusteringKeyColumns.replaceAll("\\s", "");
        String[] clusteringKeyList = clusteringKeyColumns.split(",");

        Seq<String> partitionKeysSeq = JavaConversions.asScalaBuffer(Arrays.asList(partitionKeyList)).seq();
        Option<Seq<String>> partitionKeys = new Some<>(partitionKeysSeq);
        Seq<String> clusteringKeysSeq = JavaConversions.asScalaBuffer(Arrays.asList(clusteringKeyList)).seq();
        Option<Seq<String>> clusteringKeys = new Some<>(clusteringKeysSeq);

        dataFrameFunctions.createCassandraTable(cassandraConfig.get("keyspace"), cassandraConfig.get("table"), partitionKeys, clusteringKeys, getCassandraConnector());
    }

    public void deleteFromCassandra(Dataset dataset, String keySpace, String tablename, JsonNode conf) throws Exception {
        util.Cassandra.deleteRecords(dataset, keySpace, tablename, getCassandraConnector());
    }

    private static String whereOrgId(String[] orgList) {

        StringBuilder sb = new StringBuilder();

        for (String a : orgList) {
            sb.append("'").append(a).append("',");
        }
        sb.deleteCharAt(sb.length() - 1);  // remove last "'"
        return sb.toString();
    }


    public class CassandraForeachWriter extends ForeachWriter<GenericRowWithSchema> {
        private CassandraConnector connector;
        private String keySpace;
        private String tablename;
        private StructType schema;
        private boolean emptySchema;
        /**
         * ToDo: better resource sharing
         */
        private Session session;

        public CassandraForeachWriter(CassandraConnector connector, String keySpace, String tablename, StructType schema) {
            this.connector = connector;
            this.keySpace = keySpace;
            this.tablename = tablename;
            this.schema = schema;
            emptySchema = (schema == null) || schema.isEmpty();


        }

        @Override
        public boolean open(long partitionId, long version) {
            if (emptySchema) {
                return false;
            }

            session = connector.openSession();
            return true;
        }

        /**
         * handle wrong schema case so the system don't blow up
         *
         * @param values
         * @return
         */
        private boolean isAllNull(String[] values) {
            for (String so : values) {
                if (so == null || so.equals("'null'") || so.equals("null")) {
                    continue;

                } else {
                    return false;
                }
            }

            return true;
        }

        @Override
        public void process(GenericRowWithSchema value) {
            if (emptySchema) return;

            // write string to connection
            StructField[] structField = schema.fields();
            String[] values = new String[structField.length];
            for (int i = 0; i < structField.length; i++) {
                DataType type = structField[i].dataType();
                values[i] = type.sameType(DataTypes.StringType)
                        || type.sameType(DataTypes.TimestampType) ? "'" + value.get(i) + "'" : value.get(i) + "";
            }

            if (isAllNull(values)) {
                return;
            }

            String fields = "(" + String.join(", ", schema.fieldNames()).toLowerCase() + ")";
            String fieldValues = "(" + String.join(", ", values) + ")";
            String statement = "insert into " + keySpace + "." + tablename + " " + fields + " values" + fieldValues;

            try {
                session.execute(statement);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("sqlstat: " + statement);
                throw e;
            }
        }

        @Override
        public void close(Throwable errorOrNull) {
            if (errorOrNull != null) {
                errorOrNull.printStackTrace();
            }

            if (session != null) {
                session.close();
            }
        }
    }
}