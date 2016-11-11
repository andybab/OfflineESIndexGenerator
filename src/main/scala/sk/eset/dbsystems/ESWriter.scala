package sk.eset.dbsystems

import java.io.File
import java.{lang, util}

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.DirectoryFileFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException
import org.elasticsearch.node.Node
import org.elasticsearch.node.NodeBuilder._
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
               hadoopConfResources: Seq[String]
              ) extends Serializable {
  @transient
  private val logger: Logger = LoggerFactory.getLogger(classOf[ESWriter])

  private def getIndexRequest(data: Map[String, Object]): IndexRequest = {
    val indexRequest = new IndexRequest()

    val _ID: String = "_id"

    //Extract _id field value
    val (_id, dataFiltered) = if (data.contains(_ID)) (data(_ID), data - _ID) else (None, data)

    val dataJavaMap: java.util.Map[String, Object] = new util.HashMap[String, Object]()
    dataFiltered.foreach(x => dataJavaMap.put(x._1, x._2))

    indexRequest.source(dataJavaMap)
    indexRequest.index(indexName).`type`(documentType) routing "SAME_VALUE"

    //Add document id
    indexRequest.id(_id.toString)
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
        FileUtils.deleteDirectory(new java.io.File(resource.toString))
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
            .put("processors", 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put("node.name", "ESWriter_" + indexName + context.partitionId())
            .put("path.data", indexDirname)
            .put("index.refresh_interval", -1)
            .put("index.translog.flush_threshold_size", "128mb")
            .put("bootstrap.mlockall", true)
            .put("index.load_fixed_bitset_filters_eagerly", false)
            .put("indices.store.throttle.type", "none")
            .put("indices.memory.index_buffer_size", "5%")
            .put("index.merge.policy.max_merged_segment", 256 + "mb")
            .put("index.merge.policy.segments_per_tier", 4)
            .put("index.merge.scheduler.max_thread_count", 1)
            .put("path.repo", snapshotDirectoryName)
            .put("index.compound_format", false)
            .put("indices.fielddata.cache.size", "0%")
            .put("path.home", "bla")
            .build

          val node: Node = nodeBuilder().settings(nodeSettings).local(true).node()
          node.start
          val client: Client = node.client

          try {
            client.admin.indices.prepareCreate(indexName)
              .setSettings(
                Settings.builder.loadFromSource(indexSettings)
                  .put("number_of_shards", numPartitions)
                  .put("number_of_replicas", 0)
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
            logger.info("Deleting index {}", indexName)
            client.admin().indices().delete(new DeleteIndexRequest(indexName))

            logger.info("Closing handlers")

          } catch {
            case e: Exception => {
              logger.error("Caught ES exception", e)
            }
          } finally {
            client.close()
            node.close()
          }

          ///Merge partitions
          //config hadoop
          val conf = new Configuration()
          hadoopConfResources.map(new File(_).toURI.toURL).foreach(conf.addResource)
          val dfsClient = org.apache.hadoop.fs.FileSystem.get(conf)

          import scala.collection.JavaConversions._
          //Find shard directory with data
          val shardWithData = FileUtils.listFilesAndDirs(
            new File(snapshotDirectoryName + "/indices/" + indexName), DirectoryFileFilter.DIRECTORY, DirectoryFileFilter.DIRECTORY)
            //Tail to skip parent directory
            .tail.maxBy(FileUtils.sizeOfDirectory).toString

          dfsClient.copyFromLocalFile(false, true,
            new Path(shardWithData),
            new Path(destDFSDir + "/indices/" + indexName + "/" + context.partitionId()))

          //If partition id 0 copy also top level snapshot information
          if (context.partitionId() == 0) {
            //We only copy .../index file if it does not exist
            val indexFilePath: Path = new Path(snapshotDirectoryName + "/index")
            if (!dfsClient.exists(indexFilePath)) {
              dfsClient.copyFromLocalFile(false, true,
                indexFilePath,
                new Path(destDFSDir))
            }
            dfsClient.copyFromLocalFile(false, true,
              new Path(snapshotDirectoryName + "/meta-" + snapshotName + ".dat"),
              new Path(destDFSDir))
            dfsClient.copyFromLocalFile(false, true,
              new Path(snapshotDirectoryName + "/snap-" + snapshotName + ".dat"),
              new Path(destDFSDir))
            dfsClient.copyFromLocalFile(false, true,
              new Path(snapshotDirectoryName + "/indices/" + indexName + "/meta-" + snapshotName + ".dat"),
              new Path(destDFSDir + "/indices/" + indexName + "/meta-" + snapshotName + ".dat"))
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