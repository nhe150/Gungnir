DATE=`date --date="-1 days" +%Y-%m-%d`
#delete hdfs files from streaming

spark2-submit  --name batch_splitData_$DATE --files $1 --master yarn --deploy-mode cluster --driver-memory 6g --num-executors 4 --executor-cores 2 --executor-memory 8g --queue pda --jars hadoop-lzo-0.4.21-SNAPSHOT.jar --class SparkDataBatch Gungnir-assembly-0.1.jar --job splitData --input /kafka/aggr_splunk/$DATE/ --config $1

