echo $1

nohup spark2-submit --name batch_splitData_$1 --files application.conf --master yarn --deploy-mode cluster --num-executors 6 --driver-memory 6g --executor-memory 6g --jars hadoop-lzo-0.4.21-SNAPSHOT.jar --class SparkDataBatch Gungnir-assembly-0.1.jar --job splitData --input /kafka/aggr_splunk/2017-$1/ --config application.conf > log/batch_splitData_$1 &

