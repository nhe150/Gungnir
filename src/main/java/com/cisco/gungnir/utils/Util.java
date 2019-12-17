package com.cisco.gungnir.utils;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.List;

public class Util implements Serializable  {
    public static String[] buildWhereClauses(String date, String relation){
        String[] names = relation.split(",");
        String[] results = new String[names.length];
        for( int i= 0; i < names.length; i++) {

            String one = String.format("pdate = '%s' and relation_name = '%s'", date, names[i].trim());
            results[i] = one;
        }
        return results;
    }


    public static String buildWhereClause(String date, String relation){
        String[] names = relation.split(",");
        StringBuilder sb = new StringBuilder();
        for( int i= 0; i < names.length; i++) {

            String one = String.format(" ( pdate = '%s' and relation_name = '%s' )", date, names[i].trim());
            sb.append(one);
            if( i != names.length - 1)
                sb.append(" OR ");
        }
        return sb.toString();
    }

    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }


    public static void main(String[] args){
        System.out.println(buildWhereClause("2019-10-02", "callQuality, callDuration, rtUser, activeUserRollUp, fileUsed"));
    }
}
