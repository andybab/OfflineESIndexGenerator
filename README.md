# OfflineESIndexGenerator
Offline Elasticsearch(ES) index generator.

Based on [MyPureCloud/elasticsearch-lambda](https://github.com/MyPureCloud/elasticsearch-lambda). Generates ES index using Apache Spark and stores snapshots of indices in HDFS. From there indices can be restored using [ES HDFS repository plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs.html)

##Dependencies
This project depends on shaded ES client libraries. A sample Maven project to build them can be found [here](https://github.com/andybab/es-shaded)

## Building
To build an jar file with dependencies:

    sbt assembly

## Usage
Please see [run_on_spark.sh](run_on_spark.sh) for an example how to run on Spark YARN.
