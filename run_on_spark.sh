#!bash

spark-submit --conf spark.yarn.executor.memoryOverhead=5120 --class sk.eset.dbsystems.OfflineIndexer --deploy-mode client --total-executor-cores 15 --executor-memory 3g --master yarn --jars \
./lib_managed/jars/org.apache.lucene/lucene-core/lucene-core-5.5.2.jar  \
./target/scala-2.10/OfflineESIndex-2.3.2.jar \
-w WORK_DIR \
-M src/test/resources/mtcars-mapping.json \
-S src/test/resources/mtcars-settings.json \
--indexShardCnt 2 \
--indexName mtcars \
-d mtcar \
--tableSchema name=string,mpg=int,cyl=int,disp=int,hp=int,drat=float,wt=float,qsec=float,vs=int,am=int,gear=int,carb=int \
--fieldsToExtractMapping carmake=name,mpg=mpg,cyl=cyl,disp=disp,carb=carb,qsec=qsec \
-r SnapshotRepo \
--snapshotName mtcars \
--destDFSDir WORK_DIR/SNAPSHOT_REPO
<path to input data>/$1 &> log_$1.file

