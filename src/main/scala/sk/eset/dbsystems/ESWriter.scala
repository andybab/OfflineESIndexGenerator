package sk.eset.dbsystems

import java.io.File
import java.{lang, util}

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.DirectoryFileFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException
import org.elasticsearch.node.Node
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by andrej.babolcai on 2. 2. 2016.
  *
  */
class ESWriter(numPartitions: Int,
               workdirName: String,
               indexName: String,
               documentType: String,
               mapping: String,
               indexSettings: String,
               snapshotrepoName: String,
               snapshotName: String,
               destDFSDir: String,
               hadoopConfResources: Seq[String],
               _IDField: Option[String]
              ) extends Serializable {
  @transient
  private val logger: Logger = LoggerFactory.getLogger(classOf[ESWriter])

  private def getIndexRequest(data: Map[String, Object]): IndexRequest = {
    val indexRequest = new IndexRequest()

    //Extract _id field value
    val (_id:Option[String], dataFiltered:Map[String,Object]) = if(_IDField.isDefined && data.contains(_IDField.get)) (Option(data(_IDField.get)), data - _IDField.get) else (None, data)

    val dataJavaMap: java.util.Map[String, Object] = new util.HashMap[String, Object]()
    dataFiltered.foreach(x => dataJavaMap.put(x._1, x._2))

    indexRequest.source(dataJavaMap)
    indexRequest.index(indexName).`type`(documentType) routing "SAME_VALUE"

    //Add document id
    if(_id.isDefined) indexRequest.id(_id.get.toString) else indexRequest
  }

  private def getBulkRequest(bulkData: Seq[Map[String, Object]]): BulkRequest = {
    val bulkRequest = new BulkRequest()

    bulkData.map(getIndexRequest).foreach(bulkRequest.add)
    bulkRequest
  }

  //Automatic resource management (ARM) removing directories on path
  def deletePathARM(resource: java.nio.file.Path)(block: java.nio.file.Path => Unit) {
    try {
      block(resource)
    } finally {
      if (resource != null) {
        //FileUtils.deleteDirectory(new java.io.File(resource.toString))
      }
    }
  }

  def processPartition(context: TaskContext, data: Iterator[Map[String, Object]]): Unit = {
    try {
      val dirPath = java.nio.file.Paths.get(workdirName + s"/offline-indexing-${indexName}_" + context.partitionId() + "_" + context.attemptNumber() + "_" + java.util.UUID.randomUUID())
      deletePathARM(java.nio.file.Files.createDirectory(dirPath)) {
        indexDirPath => {
          //Create temporary directory in work directory
          val indexDirname = indexDirPath.toString + "/" + indexName + "_shard" + context.partitionId() + "_" + context.attemptNumber()

          val snapshotDirectoryName: String = indexDirPath.toString + "/repo_" + indexName
          val nodeSettings: Settings = Settings.builder
            .put("http.enabled", false)
            .put("transport.type", "local")
            .put("processors", 1)
            .put("node.name", "ESWriter_" + indexName + context.partitionId())
            .put("path.data", indexDirname)
            //.put("bootstrap.memory_lock", true)
            .put("indices.store.throttle.type", "none")
            .put("indices.memory.index_buffer_size", "5%")
            .put("indices.fielddata.cache.size", "0%")
            .put("path.repo", snapshotDirectoryName)
            .put("path.home", "bla")
            .build

          val node: Node = new Node(nodeSettings)
          node.start
          val client: Client = node.client

          try {
            client.admin.indices.prepareCreate(indexName)
              .setSettings(
                Settings.builder.loadFromSource(indexSettings)
                  .put("number_of_shards", numPartitions)
                  .put("number_of_replicas", 0)
                  .put("refresh_interval", -1)
                  .put("load_fixed_bitset_filters_eagerly", false)
                  .put("merge.policy.max_merged_segment", 256 + "mb")
                  .put("merge.policy.segments_per_tier", 4)
                  .put("merge.scheduler.max_thread_count", 1)
                  .put("compound_format", false)
                  .put("translog.flush_threshold_size", "128mb")                 
              )
              .get

            client.admin.indices.preparePutMapping(indexName).setType(documentType).setSource(mapping).get

            //Create snapshot settings
            val shouldCompress = new lang.Boolean(false)
            val snapshotRepoSettings: util.Map[String, Object] = new util.HashMap[String, Object]
            snapshotRepoSettings.put("location", ".")
            snapshotRepoSettings.put("compress", shouldCompress)
            snapshotRepoSettings.put("max_snapshot_bytes_per_sec", "400mb")

            //Prepare snapshot repository
            logger.info(s"Snapshot repository name: $snapshotRepoSettings, settings $snapshotRepoSettings)")
            client.admin().cluster().preparePutRepository(snapshotrepoName).setType("fs").setSettings(snapshotRepoSettings).get()

            type ES_MAP_TYPE = java.util.Map[String, Object]
            logger.info(s"Starting indexing $indexDirname")
            data.grouped(400).map(getBulkRequest).map(client.bulk).map(_.actionGet()).foreach(x => {
              if (x.hasFailures) {
                x.getItems.foreach(e => logger.error(x.buildFailureMessage()))
              }
            })

            logger.info(s"Finished indexing $indexDirname")

            //Flush
            logger.info("Flushing index {}", indexName)
            client.admin().indices().prepareFlush(indexName).get()

            //Optimize/merge
            logger.info("Optimizing index {}", indexName)
            client.admin().indices().prepareForceMerge(indexName).get()

            //Snapshot
            logger.info("Snapshoting index {}", indexName)
            client.admin().cluster().prepareCreateSnapshot(snapshotrepoName, snapshotName).setWaitForCompletion(true).setIndices(indexName).get()

            ///Delete index
            //logger.info("Deleting index {}", indexName)
            //client.admin().indices().delete(new DeleteIndexRequest(indexName))

            logger.info("Closing handlers")

          } catch {
            case e: Exception => {
              logger.error("Caught ES exception", e)
            }
          } finally {
            client.close()
            node.close()
          }

          //??????????????????????????
          //TODO skopirovat cele snapshot repo z kazdej shardu/spark_particie
          // a potom v skripte po skonceni spark jobu pospajat shardy
          //??????????????????????????

          ///Copy generated snapshots to hdfs
          //config hadoop
          val conf = new Configuration()
          hadoopConfResources.map(new File(_).toURI.toURL).foreach(conf.addResource)
          val dfsClient = org.apache.hadoop.fs.FileSystem.get(conf)

          import scala.collection.JavaConversions._
          //Find shard directory with data
          
          val destDFSIndexDirPath = destDFSDir + "/to_resolve/" + indexName + "/"

          val indexDir: String = FileUtils.listFilesAndDirs(
            new File(snapshotDirectoryName + "/indices"), DirectoryFileFilter.DIRECTORY, DirectoryFileFilter.DIRECTORY)
            //Tail to skip parent directory
            .tail.maxBy(FileUtils.sizeOfDirectory).toString + "/"

          println(s"Copying $indexDir to $destDFSIndexDirPath")

          dfsClient.mkdirs(new Path(destDFSIndexDirPath + "/indices/"))

          //Copy data to hdfs
          dfsClient.copyFromLocalFile(false, true,
            new Path(indexDir),
            new Path(destDFSIndexDirPath + "/indices/"))

          //println(destDFSIndexDirPath)
          //If partition id 0 copy also top level snapshot information
          if (context.partitionId() == 0) {
            val snapshotInfoFiles = FileUtils.listFiles(new File(snapshotDirectoryName), null, false)

            snapshotInfoFiles.foreach(x => dfsClient.copyFromLocalFile(false, true,
              new Path(x.getAbsolutePath),
              new Path(destDFSIndexDirPath)))

            /*//We only copy .../index file if it does not exist
            val indexFilePath: Path = new Path(snapshotDirectoryName + "/indices")
            if (!dfsClient.exists(indexFilePath)) {
              logger.info(s"Copping to $destDFSIndexDirPath")
              dfsClient.copyFromLocalFile(false, true,
                indexFilePath,
                new Path(destDFSIndexDirPath))
            }

            dfsClient.copyFromLocalFile(false, true,
              new Path(snapshotDirectoryName + "/meta-*"),
              new Path(destDFSDir))
            dfsClient.copyFromLocalFile(false, true,
              new Path(snapshotDirectoryName + "/snap-*"),
              new Path(destDFSDir))
            dfsClient.copyFromLocalFile(false, true,
              new Path(snapshotDirectoryName + "/indices/" + indexName + "/meta-" + snapshotName + ".dat"),
              new Path(destDFSDir + "/indices/" + indexName + "/meta-" + snapshotName + ".dat"))*/
          } 

          dfsClient.close()
        }
      }
    } catch {
      case x: UncategorizedExecutionException => println(x)
        throw x
    }
  }
}
