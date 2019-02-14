package com.cisco.gungnir.pipelines;

import com.cisco.gungnir.job.QoSDataMonitor;
import org.apache.spark.sql.*;
import java.io.Serializable;

public class QoSMonitor implements Serializable { // CallAnalyzer

    public static void main(String[] args) throws Exception {

        // Default parameter. Set args[] in configuration when testing to override this.
        int orgNum = 50;
        String threshold = "0.33";
        boolean ifInitialize = false;
        boolean isTest = false;
        int historyDuration = 30;

        // Override with args from CMP
        if(args.length>0){
            if(args.length > 0){
                orgNum = Integer.parseInt(args[0]);
            }
            if(args.length > 1){
                threshold = args[1];
            }
            if(args.length > 2){
                ifInitialize = Boolean.parseBoolean(args[2]);
            }
            if(args.length > 3){
                isTest = Boolean.parseBoolean(args[3]);
            }
            if(args.length > 4){
                historyDuration = Integer.parseInt(args[4]);
            }
        }

        SparkSession spark = SparkSession.builder()
            .master("local[4]") // Use this for local testing.
            .appName("CallAnalyzerMonitor")
            .getOrCreate();

        QoSDataMonitor app = new QoSDataMonitor(spark);

        app.run(orgNum, threshold, ifInitialize,historyDuration,isTest);

    }

}

