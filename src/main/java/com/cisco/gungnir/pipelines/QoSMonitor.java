package com.cisco.gungnir.pipelines;

import com.cisco.gungnir.job.QoSDataMonitor;
import org.apache.spark.sql.*;
import java.io.Serializable;

// CallAnalyzer
public class QoSMonitor implements Serializable {

    public static void main(String[] args) throws Exception {

        // Default parameter values
        int orgNum = 50;
        String threshold = "0.33";
        boolean ifInitialize = false;
        boolean isTest = false;
        int historyDuration = 30;

        // Override default values with args[] from CMP or from configuration setting
        if(args.length == 5){
            orgNum = Integer.parseInt(args[0]);
            threshold = args[1];
            ifInitialize = Boolean.parseBoolean(args[2]);
            isTest = Boolean.parseBoolean(args[3]);
            historyDuration = Integer.parseInt(args[4]);
        }

        SparkSession spark = SparkSession.builder()
            .master("local[4]") // Use this for local testing
            .appName("QoSMonitor")
            .getOrCreate();

        QoSDataMonitor app = new QoSDataMonitor(spark);

        app.run(orgNum, threshold, ifInitialize,historyDuration,isTest);

    }

}

