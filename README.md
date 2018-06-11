# Gungnir

Gungnir is a framework for constructing both batch and streaming data processing pipelines and executing them on Apache Spark.

## Overview
Abstractly, data processing can be expressed in the form of a directed acyclic graph (DAG), which starts at the root vertices 
of the DAG and continues down the directed edges till it reaches the leaf vertices. Gungnir provides a config driven 
approach for expressing such data processing DAG, thus remove code duplication and provides flexible execution of code.

## How to define a data processing job
User can define a data processing pipeline with a JSON file that contains a list representation of a DAG. Within the concept of 
Gungnir, each vertex of the DAG defines a query which can be input query (read data from a specific source), output 
query (write data to a specific storage) or processing query (user logic that expressed as spark SQL).

Below is an example of job definition in Gungnir
that reads data from kafka then processes it with the 'autoLicense' query and write the result to cassandra:

```json
{
  "name": "autoLicense",
  "queryPlan":[
    [
      {
        "queryName":"readFromKafka",
        "parameters":{
          "kafka":{
            "topic": "atlas"
          },
          "timeStampField": "timeRcvd",
          "schemaName": "atlas"
        }
      }
    ],
    [
      {
        "queryName":"autoLicense",
        "parameters":{
          "output": "autoLicense"
        }
      }
    ],
    [
      {
        "queryName":"writeToCassandra",
        "parameters":{
          "cassandra":{
            "table": "license",
            "saveMode": "append"
          }
        }
      }
    ]
  ]
}
```

Gungnir does not support data processing DAG with cycles.

For more examples see the [Gungnir-Job](https://sqbu-github.cisco.com/SAP/Gungnir-Job) repo.

## What kind of queries are supported

#### Kafka
* readFromKafka
```json
{
  "queryName":"readFromKafka",
  "parameters":{
    "kafka":{
      "broker": "localhost:9092",
      "topic": "topic1, topic2",
      "startingOffsets": {"topic1":{"0":-2},"topic2":{"0":-2}},
      "endingOffsets": {"topic1":{"0":-1},"topic2":{"0":-1}},
      "useTopicPrefix": false      
    },
    "timeStampField": "timeRcvd",
    "schemaName": "schemaName"
  }
}
```

| property  | description | required? |
| ------------- | ------------- | --- |
| kafka.broker  | Kafka broker (overrides the property at application.conf) |  no   |
| kafka.topic   | Kafka topic(s) to consume (comma separated list) | yes |
| kafka.startingOffsets | startingOffsets (see [spark documentation](https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html)) |  no   |
| kafka.endingOffsets   | endingOffsets (see [spark documentation](https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html)) |  no   |
| kafka.useTopicPrefix  | whether to use topicPrefix defined at application.conf (default true) |  no   |
| timeStampField| The name of the field that is timeStamp type  |  no   |
| schemaName    | The name of schema that used to parse the messages from kafka |  no     |

* writeToKafka
```json
{
  "queryName":"writeToKafka",
  "parameters":{
    "kafka":{
      "broker": "localhost:9092",
      "topic": "topic1",
      "topicKey": "field1",
      "topicValue": "field2",
      "useTopicPrefix": false      
    }
  }  
}
```
| property  | description | required? |
| ------------- | ------------- | --- |
| kafka.broker  | Kafka broker (overrides the property at application.conf) |  no   |
| kafka.topic   | Kafka topic to save the result data | no (yes if there is not 'output' property set in previous query) |
| kafka.topicKey  | The field to be store as message key in the topic |  no   |
| kafka.topicValue  | The field to be store as message value in the topic |  no   |
| kafka.useTopicPrefix  | whether to use topicPrefix defined at application.conf (default true) |  no   |

#### Cassandra
* readFromCassandra 
```json
{
  "queryName":"readFromCassandra",
  "parameters":{
    "cassandra":{
      "host": "localhost",
      "port": "9142",
      "username": "",
      "password": "",
      "table": "tableName",
      "keyspace": "ks_global_pda"
    }
  }
}
```
| property  | description | required? |
| ------------- | ------------- | --- |
| cassandra.host  | cassandra host (overrides the property at application.conf) |  no   |
| cassandra.port  | cassandra port (overrides the property at application.conf) |  no   |
| cassandra.username  | cassandra username (overrides the property at application.conf) |  no   |
| cassandra.password  | cassandra password (overrides the property at application.conf) |  no   |
| cassandra.keyspace  | cassandra keyspace (overrides the property at application.conf) |  no   |
| cassandra.table  | cassandra table to read data from |  yes   |

Note: readFromCassandra dose not support streaming mode

* writeToCassandra
```json
{
  "queryName":"writeToCassandra",
  "parameters":{
    "cassandra":{
      "host": "localhost",
      "port": "9142",
      "username": "",
      "password": "",
      "table": "spark_agg",
      "keyspace": "ks_global_pda",
      "saveMode": "update",
      "consistencyLevel": "LOCAL_ONE"
    }
  }
}
```
| property  | description | required? |
| ------------- | ------------- | --- |
| cassandra.host  | cassandra host (overrides the property at application.conf) |  no   |
| cassandra.port  | cassandra port (overrides the property at application.conf) |  no   |
| cassandra.username  | cassandra username (overrides the property at application.conf) |  no   |
| cassandra.password  | cassandra password (overrides the property at application.conf) |  no   |
| cassandra.keyspace  | cassandra keyspace (overrides the property at application.conf) |  no   |
| cassandra.table  | cassandra table to write data to |  yes   |
| cassandra.saveMode  | The saveMode for cassandra write (see [spark documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes))  |  yes   |
| cassandra.consistencyLevel  | cassandra write consistencyLevel |  no   |

* deleteFromCassandra
```json
{
  "queryName":"deleteFromCassandra",
  "parameters":{
    "cassandra":{
      "host": "localhost",
      "port": "9142",
      "username": "",
      "password": "",
      "table": "spark_data",
      "keyspace": "ks_global_pda"
    }
  }
}
```
| property  | description | required? |
| ------------- | ------------- | --- |
| cassandra.host  | cassandra host (overrides the property at application.conf) |  no   |
| cassandra.port  | cassandra port (overrides the property at application.conf) |  no   |
| cassandra.username  | cassandra username (overrides the property at application.conf) |  no   |
| cassandra.password  | cassandra password (overrides the property at application.conf) |  no   |
| cassandra.keyspace  | cassandra keyspace (overrides the property at application.conf) |  no   |
| cassandra.table  | cassandra table to write data to |  yes   |

#### File

* readFromFile
```json
{
  "queryName":"readFromFile",
  "parameters":{
    "input": "folder1, folder2",
    "dataLocation": "src/test/gungnir_job_repo/testData/",
    "timeStampField": "time_stamp",
    "schemaName": "activeUser",
    "format": "json",
    "multiline": true,
    "date": "2018-04-21",
    "period": "weekly",
    "partitionKey": "pdate" 
  },
  "dateRange": {
    "startDate": "2018-05-03",
    "endDate": "2018-05-04"
  }
}
```
| property  | description | required? |
| ------------- | ------------- | --- |
| input  | input folder(s) or file(s) that contains data |  yes   |
| dataLocation  | The path in which the input folder/file exists (overrides the property at application.conf)  |  no   |
| timeStampField| The name of the field that is timeStamp type  |  no   |
| schemaName    | The name of schema that used to parse messages from file |  no     |
| format  | The format of input data (json, csv, text, parquet, etc.) |  yes   |
| multiline  | Whether it is multiline json format (default false) |  no   |
| date  | Select data for specific date (when data is partitioned with date)  |  no   |
| period  | Select data based on certain period (the period range the selected 'date' is in)  |  no   |
| partitionKey  | The partitionKey of the data |  no   |
| dateRange.startDate  | Start date of a date range for selecting the data |  no   |
| dateRange.endDate  | End date of a date range for selecting the data |  no   |

* writeToFile
```json
{
  "queryName":"writeToFile",
  "parameters":{
    "format": "json",
    "saveMode": "append",
    "partitionKey": "pdate",
    "output": "folderName"
  }
}
```
| property  | description | required? |
| ------------- | ------------- | --- |
| format  | The format of output data (json, csv, text, parquet, etc.) |  yes   |
| saveMode  | The file saveMode (see [spark documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes))  |  yes   |
| partitionKey  | The partitionKey used to save the data |  no   |
| output  | Output folder to save the data  |  no (yes if there is not 'output' property set in previous query)   |

#### SQL Queries
Gungnir relies on Spark SQL to express the user logic for data processing. All the SQL query files need to be located in the 'queryLocation' which is set in the Gungnir config.
Please reference [Gungnir-Job](https://sqbu-github.cisco.com/SAP/Gungnir-Job) repo to see how the SQL query file looks like.

* Use SQL query in job definition file
```json
{
  "queryName":"activeUserCount",
  "parameters":{
    "aggregatePeriod": "daily",
    "timeStampField": "time_stamp",
    "output": "activeUserCount"
  }
}
```
| property  | description | required? |
| ------------- | ------------- | --- |
| queryName  | The name of the sql query (need to match the query file name stored in the 'queryLocation') |  yes   |
| output  | The output target for storing the query result (output folder/file name for file storage and output topic for kafka) |  no   |
| timeStampField| The name of the field that is timeStamp type  |  no   |
| aggregatePeriod  | For time based aggregation only (daily/weekly/monthly)  |  no |


## Configuration
User can provides a json file (application.conf) that contains default values for various config properties that used in Gungnir:

```json
{
  "kafka": {
    "broker": "localhost:9092",
    "streamingMetricsTopic": "bt1_spark_streaming_monitor",
    "topicPrefix": "bt1_spark_",
    "topicPostfix": "_hdfs",
    "maxOffsetsPerTrigger": 5000000,
    "retries": 1,
    "retryBackoffMs": 500,
    "fetchOffsetNumRetries": 1000,
    "fetchOffsetRetryIntervalMs": 1000,
    "metadataFetchTimeoutMs": 600000,
    "lingerMs": 3000,
    "batchSize": 163840,
    "timeoutMs": 300000,
    "requestTimeoutMs": 300000,
    "maxRequestSize": 104857600
  },

  "cassandra": {
    "host": "127.0.0.1",
    "port": "9142",
    "keyspace": "ks_global_pda",
    "username": "",
    "password": "",
    "consistencyLevel": "LOCAL_ONE"
  },

  "spark": {
    "streamingCheckpointLocation": "src/test/checkpoint/",
    "streamingStopGracefullyOnShutdown": true,
    "streamingBackpressureEnabled": true,
    "streamingBackpressureInitialRate": 5000000,
    "streamngKafkaMaxRatePerPartition": 5000000,
    "streamingKafkaFailOnDataLoss": false,
    "streamngTriggerWindow": "10 seconds",
    "logLevel": "WARN"
  },

  "dataLocation": "src/test/output/",
  "queryLocation": "src/test/gungnir_job_repo/sqlQueries/",
  "schemaLocation": "src/test/gungnir_job_repo/schemas/",
  "jobLocation": "src/test/gungnir_job_repo/jobs/",
  "configLocation": "src/test/gungnir_job_repo/config/"
}
```

Most of the config properties can be overridden in the job definition file under 'parameters' block in a per
query basis.

## Building Gungnir
* sbt compile. To compile this project
* sbt test. To run tests of this project
* sbt "set test in assembly := {}" clean assembly. To build fat jar, and then run with spark-submit


## Run Gungnir
```
bin/spark-submit --master spark://DAYJIANG-M-21HY:7077 --class com.cisco.gungnir.pipelines.PipelineRunner /Users/dayjiang/work/Gungnir/target/scala-2.11/Gungnir-assembly-0.2-SNAPSHOT.jar --config application.conf --job writeDataToKafka,writeDataToCassandra --type batch

```
Required parameters:
* --config: the Gungnir configuration mentioned above
* --job: the name(s) of the job(s) to be executed(comma separated)
* --type: the type of spark job(Batch/Streaming)

Optional parameters:
* --codecs: the compression codecs to use (eg. lzo for reading lzo files)

