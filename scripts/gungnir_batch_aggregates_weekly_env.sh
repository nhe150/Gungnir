DATE=`date --date="-8 days" +%Y-%m-%d`
spark2-submit --name batch_aggregates_$DATE --files $1 --master yarn --deploy-mode cluster --num-executors 6 --driver-memory 6g --executor-memory 6g --jars hadoop-lzo-0.4.21-SNAPSHOT.jar --class SparkDataBatch Gungnir-assembly-0.1.jar --job aggregates --startDate $DATE --period weekly --input /kafka/aggr_splunk/$DATE/ --config $1
