package com.cisco.gungnir.utils;

import org.apache.spark.sql.Dataset;

import java.io.Serializable;

public class DatasetFunctions implements Serializable {
    public static boolean hasColumn(Dataset dataset, String columnName){
        for(String column: dataset.columns()){
            if(columnName.equals(column)){
                return true;
            }
        }
        return false;
    }
}
