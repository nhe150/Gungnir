package com.cisco.gungnir.pipelines;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.job.SparkDataMonitor;
import org.apache.commons.cli.*;
import org.apache.spark.sql.*;
import java.io.Serializable;

public class DataVolumeMonitor implements Serializable {

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option config = new Option("c", "config", true, "system config file");
        config.setRequired(true);
        options.addOption(config);

        Option date = new Option("d", "date", true, "date of data to be monitored");
        date.setRequired(true);
        options.addOption(date);

        Option thresholdOpt = new Option("t", "threshold", true, "threshold of data volume off to be consider abnormal");
        thresholdOpt.setRequired(false);
        options.addOption(thresholdOpt);

        Option orgIdOpt = new Option("o", "orgId", true, "orgId of data to be monitored");
        orgIdOpt.setRequired(false);
        options.addOption(orgIdOpt);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("sparkDataMonitor", options);

            System.exit(1);
            return;
        }

        String configFile = cmd.getOptionValue("config");
        String currentDate  = cmd.getOptionValue("date");
        String threshold = cmd.getOptionValue("threshold")== null ? "0.3" : cmd.getOptionValue("threshold");
        String orgId = cmd.getOptionValue("orgId")== null ? "*" : "'" + cmd.getOptionValue("orgId") + "'";

        SparkSession spark = SparkSession.builder()
                .appName("sparkDataMonitor").getOrCreate();

        ConfigProvider appConfigProvider = new ConfigProvider(spark, configFile);

        SparkDataMonitor app = new SparkDataMonitor(spark, appConfigProvider);

        app.run(currentDate, threshold, orgId);
    }
}