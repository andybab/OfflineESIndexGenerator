package sk.eset.dbsystems

import org.apache.spark.rdd.RDD

/**
  * Created by andrej.babolcai on 22. 1. 2016.
  *
  */

package object spark {

  implicit class DBSysSparkRDDFunctions[T <: Map[String,Object]](val rdd: RDD[T]) extends AnyVal {
    def saveToESSnapshot(workdir:String,
                         indexName: String,
                         documentType: String,
                         mapping: String,
                         indexSettings: String,
                         snapshotrepoName: String,
                         snapshotName: String,
                         destDFSDir: String,
                         hadoopConfResources: Seq[String]
                        ):Unit = {

      Product1
      //Configure job
      val numPartitions = rdd.partitions.length //For some reason we have to use this variable instead of directly passing ...length (NullPointerException)
      lazy val esWriter = new ESWriter(
        numPartitions,
        workdir,
        indexName,
        documentType,
        mapping,
        indexSettings,
        snapshotrepoName,
        snapshotName,
        destDFSDir,
        hadoopConfResources
      )
      //Run job
      rdd.sparkContext.runJob(rdd, esWriter.processPartition _)
    }
  }
}
