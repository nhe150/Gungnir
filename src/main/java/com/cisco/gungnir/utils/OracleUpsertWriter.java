package com.cisco.gungnir.utils;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

//Just define JDBCSink in a separated file rather than defining it as an inner class which may capture the outer reference.

public class OracleUpsertWriter  implements ForeachPartitionFunction<Row> {
    //        private CassandraConnector connector;
    private String pk;
    private StructType schema;
    private boolean emptySchema;

    private Connection connection;
    private Statement session;
    private Map<String, String> oracleConfig;

    public OracleUpsertWriter(Map<String, String> oracleConfig, StructType schema, String pk) {
        this.schema=schema;
        this.pk=pk;
        this.oracleConfig=oracleConfig;
    }

    @Override
    public void call(Iterator<Row> t) throws Exception {

        Class.forName("oracle.jdbc.driver.OracleDriver");
        Properties info = new java.util.Properties();
        info.put ("user", oracleConfig.get("user"));
        info.put ("password",oracleConfig.get("password"));
        info.put ("oracle.jdbc.timezoneAsRegion","false");
        this.connection = DriverManager.getConnection(oracleConfig.get("url"),info);

        this.session = connection.createStatement();

        String tableName = oracleConfig.get("dbtable");
        String[] names = schema.fieldNames();

        Set<String> pkSet = new HashSet<>(Arrays.asList(pk.replace(" ","").toLowerCase().split(",")));
        Set<String> pk_KeyValue = new HashSet<>();
        Set<String> nonPk_KeyValue = new HashSet<>();
        // write string to connection
        StructField[] structField = schema.fields();

        while (t.hasNext()){
            Row row = t.next();
            pk_KeyValue.clear();
            nonPk_KeyValue.clear();
            String[] values = new String[structField.length];


            for (int i = 0; i < structField.length; i++) {

                DataType type = structField[i].dataType();
                if(type.sameType(DataTypes.StringType)){
                    values[i] = "'" + row.get(i) + "'";
                }else if(type.sameType(DataTypes.TimestampType)){
                    //if format is '2019-07-16 17:53:58.606'
                    values[i] = "to_date( substr('" + row.get(i) + "' , 1, 19), 'YYYY-MM-DD HH24:MI:SS')";
                }else{
                    values[i] = row.get(i)+"";
                }

                if(pkSet.contains(names[i])){
                    pk_KeyValue.add(names[i] +" = "+values[i]);
                }else{
                    nonPk_KeyValue.add(names[i] +" = "+values[i]);
                }
            }

            if (isAllNull(values)) {
                return;
            }

            String fields = "(" + String.join(", ", schema.fieldNames()).toLowerCase() + ")";
            String fieldValues = "(" + String.join(", ", values) + ")";
            String insertStatement = "insert into " + tableName + " " + fields + " values " + fieldValues +" ;";

            String updateStatement =
                    "update " + tableName + " SET " + String.join(",", nonPk_KeyValue) + " where " + String.join(" and ", pk_KeyValue) +" ;";


        /**
         * Idea:
         * UPDATE tablename SET val1 = in_val1, val2 = in_val2
         *     WHERE val3 = in_val3;
         * IF ( sql%notfound ) THEN
         *     INSERT INTO tablename
         *         VALUES (in_val1, in_val2, in_val3);
         * END IF;
         */

            String sqlStr = "BEGIN " + updateStatement + " IF sql%notfound THEN " +  insertStatement + " END IF; END;";

            session.execute(sqlStr);
        }

        session.close();
        connection.close();
    }


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

}