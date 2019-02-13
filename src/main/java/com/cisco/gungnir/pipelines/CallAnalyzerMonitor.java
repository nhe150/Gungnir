package com.cisco.gungnir.pipelines;

import com.cisco.gungnir.job.CallAnalyzerDataMonitor;
import org.apache.spark.sql.*;
import java.io.Serializable;




public class CallAnalyzerMonitor implements Serializable {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
            .master("local[4]")
            .appName("CallAnalyzerMonitor")
            .getOrCreate();

        CallAnalyzerDataMonitor app = new CallAnalyzerDataMonitor(spark);

        app.run();

    }

}

