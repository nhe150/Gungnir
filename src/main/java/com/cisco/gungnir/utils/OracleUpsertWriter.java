package com.cisco.gungnir.utils;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
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
    private String driver;

    private Connection connection;
    private Statement session;
    private Map<String, String> oracleConfig;

    public OracleUpsertWriter(Map<String, String> oracleConfig, String driver, StructType schema, String pk) {
        this.schema=schema;
        this.pk=pk;
        this.oracleConfig=oracleConfig;
        this.driver = driver;
    }

    @Override
    public void call(Iterator<Row> t) throws Exception {

        Class.forName(driver);
        Properties info = new java.util.Properties();
        info.put ("user", oracleConfig.get("user"));
        info.put ("password",oracleConfig.get("password"));
        if( driver.contains("oracle")) {
            info.put("oracle.jdbc.timezoneAsRegion", "false");
        }
        this.connection = DriverManager.getConnection(oracleConfig.get("url"),info);

        this.session = connection.createStatement();

        String tableName = oracleConfig.get("dbtable");
        String[] names = schema.fieldNames();

        Set<String> pkSet = new HashSet<>(Arrays.asList(pk.replace(" ","").split(",")));
        Set<String> pk_KeyValue = new HashSet<>();
        Set<String> nonPk_KeyValue = new HashSet<>();
        // write string to connection
        StructField[] structField = schema.fields();

        while (t.hasNext()){
            Row row = t.next();
            pk_KeyValue.clear();
            nonPk_KeyValue.clear();
            String[] values = new String[structField.length];


            String sqlStr = Util.getInsertSQLStr(schema,tableName,row,values,pkSet,pk_KeyValue,nonPk_KeyValue, driver.contains("oracle"));
            System.out.println("sqlStr:" + sqlStr);

            //check execution boolean value
            session.execute(sqlStr);
        }

        session.close();
        connection.close();
    }




}