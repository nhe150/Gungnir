package com.cisco.gungnir.utils;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

//Just define JDBCSink in a separated file rather than defining it as an inner class which may capture the outer reference.

public class OracleStreamWriter extends ForeachWriter<GenericRowWithSchema> {
    //        private CassandraConnector connector;
    private String tableName;
    private StructType schema;
    private boolean emptySchema;
    private String pk;

    private Connection connection;
    private Statement session;
    private Map<String, String> oracleConfig;

    public OracleStreamWriter(Map<String, String> oracleConfig, String tableName, StructType schema, String pk) {
        this.oracleConfig = oracleConfig;
        this.tableName = tableName;
        this.schema = schema;
        this.pk=pk;
        emptySchema = (schema == null) || schema.isEmpty();
    }

    @Override
    public boolean open(long partitionId, long version) {
        try{

            Class.forName("oracle.jdbc.driver.OracleDriver");
            Properties info = new java.util.Properties();
            info.put ("user", oracleConfig.get("user"));
            info.put ("password",oracleConfig.get("password"));
            info.put ("oracle.jdbc.timezoneAsRegion","false");
            connection = DriverManager.getConnection(oracleConfig.get("url"),info);

            session = connection.createStatement();
            return true;
        } catch (SQLException e){
            return false;
        }
        catch (ClassNotFoundException ex){
            return false;
        }

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
    public void process(GenericRowWithSchema row) {
        if (emptySchema) return;

        // write string to connection
        StructField[] structField = schema.fields();
        String[] names = schema.fieldNames();
        String[] values = new String[structField.length];
        Set<String> pkSet = new HashSet<>(Arrays.asList(pk.replace(" ","").split(",")));
        Set<String> pk_KeyValue = new HashSet<>();
        Set<String> nonPk_KeyValue = new HashSet<>();

        String sqlStr = Util.getInsertSQLStr(schema,tableName,row,values,pkSet,pk_KeyValue,nonPk_KeyValue);

        try {
            session.execute(sqlStr);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("sqlstat: " + sqlStr);
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            if (errorOrNull != null) {
                errorOrNull.printStackTrace();
            }

            if (connection != null) {
                connection.close();
            }
        }catch (SQLException e){
        }
    }
}