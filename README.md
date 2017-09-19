# OfflineESIndexGenerator
Offline Elasticsearch(ES) index generator for Elasticsearch 5. Tested on v5.4 and CDH 5.11

Based on [MyPureCloud/elasticsearch-lambda](https://github.com/MyPureCloud/elasticsearch-lambda). Generates ES index using Apache Spark and stores snapshots of indices in HDFS. From there indices can be restored using [ES HDFS repository plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs.html).

Changes made in ESv5 to the snapshot directory structure require to execute an aditional [script](arange_snapshosts.py) to move the generated snapshots to the final HDFS snapshot directory.

##Dependencies
This project depends on shaded ES client libraries. A sample Maven project to build them can be found [here](https://github.com/andybab/es-shaded)

## Building
To build an jar file with dependencies:

    sbt assembly

## Usage
Please see [run_on_spark.sh](run_on_spark.sh) for an example how to run on Spark YARN.

After the script finishes please run [arange_snapshosts.py](arange_snapshosts.py) to 
