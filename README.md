# OfflineESIndexGenerator
Offline Elasticsearch(ES) index generator.

Generates ES index using Apache Spark and stores snapshots of indices in HDFS. From there indices can be restored using [ES HDFS repository plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs.html)

## Building
To build an jar file with dependencies:

    sbt assembly

## Usage
Please see [run_on_spark.sh](run_on_spark.sh) for an example how to run on Spark YARN.
