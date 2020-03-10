package com.cisco.gungnir.pipelines;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.job.DataMonitor;
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
        date.setRequired(false);
        options.addOption(date);

        Option thresholdOpt = new Option("t", "threshold", true, "threshold of data volume off to be consider abnormal");
        thresholdOpt.setRequired(false);
        options.addOption(thresholdOpt);

        Option orgIdOpt = new Option("o", "orgId", true, "orgId of data to be monitored");
        orgIdOpt.setRequired(false);
        options.addOption(orgIdOpt);

        Option clOpt = new Option("cl", "class", true, "Job class name");
        clOpt.setRequired(false);
        options.addOption(clOpt);
        
        Option regionOpt = new Option("region", "region", true, "Region Name");
        regionOpt.setRequired(false);
        options.addOption(regionOpt);

        Option relationNamesOpt = new Option("rn", "relation", true, "Relation names to be read from cassandra");
        relationNamesOpt.setRequired(false);
        options.addOption(relationNamesOpt);

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
        String cl = cmd.getOptionValue("cl") == null ? "SparkDataMonitor" : cmd.getOptionValue("cl");
        String region  = cmd.getOptionValue("region");
        String relationNames  = cmd.getOptionValue("rn");

        SparkSession spark = SparkSession.builder()
                .appName(cl).getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        ConfigProvider appConfigProvider = new ConfigProvider(spark, configFile);

        String classS = "com.cisco.gungnir.job." + cl;

        DataMonitor monitor = (DataMonitor) Class.forName(classS).newInstance();
        monitor.set(spark, appConfigProvider);

        monitor.run(currentDate, threshold, orgId, region, relationNames);
    }
}