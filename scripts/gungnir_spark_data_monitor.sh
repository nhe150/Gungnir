export SPARK_KAFKA_VERSION=0.10

DATE=`date --date="-1 days" +%Y-%m-%d`

spark2-submit --name gungnir_spark_data_monitor --master yarn --deploy-mode cluster --driver-memory 2g --executor-memory 2g --num-executors 4 --queue pda --files application.conf,sparkDataMonitor.orgList --jars jar_files/kafka-clients-0.10.0-kafka-2.1.0.jar,jar_files/slf4j-api-1.7.21.jar,jar_files/spark-tags_2.11-2.2.0.jar,jar_files/lz4-1.3.0.jar,jar_files/snappy-java-1.1.2.6.jar,jar_files/unused-1.0.0.jar,jar_files/scala-library-2.11.8.jar,jar_files/spark-sql-kafka-0-10_2.11-2.2.0.jar --class SparkDataMonitor Gungnir-assembly-0.1.jar --config application.conf --date $DATE --orgIdList sparkDataMonitor.orgList

