package com.cisco.gungnir.pipelines;

import com.cisco.gungnir.job.CallAnalyzerDataMonitor;
import org.apache.spark.sql.*;
import java.io.Serializable;




public class CallAnalyzerMonitor implements Serializable {

    public static void main(String[] args) throws Exception {

        // Default parameter:
        int orgNum = 50;
        //String threshold = "0.33"; // Real alert
        String threshold = "1.1"; // All fail
        boolean ifInitialize = false;
        int historyDuration = 30;

        boolean isTest = true;
        
        SparkSession spark;

        if(isTest){
            spark = SparkSession.builder()
                .master("local[4]")
                .appName("CallAnalyzerMonitor")
                .getOrCreate();
        }else{
            spark = SparkSession.builder()
                .appName("CallAnalyzerMonitor")
                .getOrCreate();
        }

        CallAnalyzerDataMonitor app = new CallAnalyzerDataMonitor(spark);

        app.run(orgNum, threshold, ifInitialize,historyDuration,isTest );

    }

}

