package com.cisco.gungnir.pipelines;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.job.JobExecutor;
import org.apache.commons.cli.*;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class PipelineRunner implements Serializable {
    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option config = new Option("c", "config", true, "config file");
        config.setRequired(true);
        options.addOption(config);

        Option job = new Option("j", "job", true, "job name");
        job.setRequired(true);
        options.addOption(job);

        Option type = new Option("t", "type", true, "job type(batch or stream)");
        type.setRequired(true);
        options.addOption(type);

        Option codecs = new Option("co", "codecs", true, "spark compression codecs");
        codecs.setRequired(false);
        options.addOption(codecs);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("PipelineRunner", options);

            System.exit(1);
            return;
        }

        String jobName = cmd.getOptionValue("job");
        String configFile = cmd.getOptionValue("config");
        String jobType = cmd.getOptionValue("type");

        SparkSession.Builder builder = SparkSession.builder().enableHiveSupport().appName(jobName);
        if(cmd.getOptionValue("codecs")!=null && cmd.getOptionValue("codecs").equals("lzo")){
            builder = builder.config("spark.hadoop.io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
        }
        SparkSession spark = builder.getOrCreate();

        ConfigProvider configProvider = new ConfigProvider(spark, configFile);
        JobExecutor jobExecutor = new JobExecutor(spark, configProvider);

        jobName = jobName.replaceAll("\\s","");
        String[] jobNames = jobName.split(",");
        for(String jobname: jobNames){
            jobExecutor.execute(jobname, jobType);
        }
        if("stream".equals(jobType)){
            spark.streams().awaitAnyTermination();
        }
    }
}
