package com.cisco.gungnir.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Util implements Serializable {
    public static String[] buildWhereClauses(String date, String relation) {
        String[] names = relation.split(",");
        String[] results = new String[names.length];
        for (int i = 0; i < names.length; i++) {

            String one = String.format("pdate = '%s' and relation_name = '%s'", date, names[i].trim());
            results[i] = one;
        }
        return results;
    }

    /**
     * will deprecate
     * @param date
     * @param relation
     * @return
     */
    @Deprecated
    public static String buildWhereClauseSS(String date, String relation) {
        String[] names = relation.split(",");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < names.length; i++) {

            String one = String.format(" ( pdate = '%s' and relation_name = '%s' )", date, names[i].trim());
            sb.append(one);
            if (i != names.length - 1)
                sb.append(" OR ");
        }
        return sb.toString();
    }

    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static String getInsertSQLStr(StructType schema, String tableName, Row row, String pk, boolean isOracle) {
        StructField[] structField = schema.fields();
        String[] names = schema.fieldNames();

        Set<String> pkSet = new HashSet<>(Arrays.asList(pk.replace(" ","").split(",")));
        String[] values = new String[structField.length];


        Set<String> pk_KeyValue = new HashSet<String>();
        Set<String> nonPk_KeyValue = new HashSet<String>();

        for (int i = 0; i < structField.length; i++) {
            DataType type = structField[i].dataType();
            //need DateType to be based on sink instead of source
            if (type.sameType(DataTypes.StringType) || type.sameType(DataTypes.DateType) ) {
                values[i] = "'" + row.get(i) + "'";
            } else if (type.sameType(DataTypes.TimestampType)) {
                //if format is '2019-07-16 17:53:58.606'
                values[i] = "to_date( substr('" + row.get(i) + "' , 1, 19), 'YYYY-MM-DD HH24:MI:SS')";
            } else {
                values[i] = row.get(i) + "";
            }

            if (pkSet.contains(names[i])) {
                pk_KeyValue.add(escapeOraclePreserve(names[i]) + " = " + values[i]);
            } else {
                nonPk_KeyValue.add( escapeOraclePreserve(names[i]) + " = " + values[i]);
            }
        }

        if (isAllNull(values)) {
            return "";
        }

        String fields = "(" + String.join(", ", escapeOraclePreserve(schema.fieldNames())) + ")";
        String fieldValues = "(" + String.join(", ", values) + ")";
        String insertStatement = "insert into " + tableName + " " + fields + " values " + fieldValues + " ;";

        String updateStatement =
                "update " + tableName + " SET " + String.join(",", nonPk_KeyValue) + " where " + String.join(" and ", pk_KeyValue) + " ;";


        /**
         * Idea:
         * UPDATE tablename SET val1 = in_val1, val2 = in_val2
         *     WHERE val3 = in_val3;
         * IF ( sql%notfound ) THEN
         *     INSERT INTO tablename
         *         VALUES (in_val1, in_val2, in_val3);
         * END IF;
         */

        String sqlStr = "BEGIN " + updateStatement + " IF sql%notfound THEN " + insertStatement + " END IF; END;";

        if ( isOracle) {
            return sqlStr;
        }

        //for postgress upsert
        String upsertPostgres = "";
        if( nonPk_KeyValue.isEmpty()) {
            upsertPostgres = "insert into " + tableName + " " + fields + " values " + fieldValues + "  ON CONFLICT (" + String.join(",", pkSet)
                    + " )  DO Nothing;";
        }else
        {
            upsertPostgres = "insert into " + tableName + " " + fields + " values " + fieldValues + "  ON CONFLICT (" + String.join(",", pkSet)
                    + " )  DO UPDATE SET " + String.join(",", nonPk_KeyValue) + " ;";
        }

        return upsertPostgres;
    }

    static String QUOTE = "\"";
    static Set<String> oracleSpecials = new HashSet<String>();

    static {
        oracleSpecials.add("DATE");
    }

    private static String escapeOraclePreserve(String name) {
        if (oracleSpecials.contains(name.toUpperCase())) {
            return QUOTE + name + QUOTE;
        }

        return name;
    }

    private static String[] escapeOraclePreserve(String[] names) {
        for (int i = 0; i < names.length; i++) {
            names[i] = escapeOraclePreserve(names[i]);
        }

        return names;
    }


    public static boolean isAllNull(String[] values) {
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
