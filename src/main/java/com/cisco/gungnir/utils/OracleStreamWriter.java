package com.cisco.gungnir.utils;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
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
        Set<String> pkSet = new HashSet<>(Arrays.asList(pk.replace(" ","").toLowerCase().split(",")));
        Set<String> pk_KeyValue = new HashSet<>();
        Set<String> nonPk_KeyValue = new HashSet<>();


//        for (int i = 0; i < structField.length; i++) {
//            DataType type = structField[i].dataType();
//            if(type.sameType(DataTypes.StringType)){
//                values[i] = "'" + value.get(i) + "'";
//            }else if(type.sameType(DataTypes.TimestampType)){
//                //if format is '2019-07-16 17:53:58.606'
//                values[i] = "to_date( substr('" + value.get(i) + "' , 1, 19), 'YYYY-MM-DD HH24:MI:SS')";
//            }else{
//                values[i] = value.get(i)+"";
//            }
//
//            if(pkSet.contains(names[i])){
//                pk_KeyValue.add(names[i] +" = "+values[i]);
//            }else{
//                nonPk_KeyValue.add(names[i] +" = "+values[i]);
//            }
//        }
//
//        if (isAllNull(values)) {
//            return;
//        }
//
//        String fields = "(" + String.join(", ", schema.fieldNames()).toLowerCase() + ")";
//        String fieldValues = "(" + String.join(", ", values) + ")";
//        String statement = "insert into " + tableName + " " + fields + " values" + fieldValues;
//
//        String insertStatement = "insert into " + tableName + " " + fields + " values " + fieldValues +" ;";
//
//        String updateStatement =
//                "update " + tableName + " SET " + String.join(",", nonPk_KeyValue) + " where " + String.join(" and ", pk_KeyValue) +" ;";
//
//
//        /**
//         * Idea:
//         * UPDATE tablename SET val1 = in_val1, val2 = in_val2
//         *     WHERE val3 = in_val3;
//         * IF ( sql%notfound ) THEN
//         *     INSERT INTO tablename
//         *         VALUES (in_val1, in_val2, in_val3);
//         * END IF;
//         */
//
//        String sqlStr = "BEGIN " + updateStatement + " IF sql%notfound THEN " +  insertStatement + " END IF; END;";

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