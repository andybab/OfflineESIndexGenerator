package sk.eset.dbsystems

import java.io.File
import java.util.UUID

import scala.util.parsing.json._
import com.cloudera.org.joda.time.IllegalInstantException
import my.elasticsearch.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import my.elasticsearch.joda.time.{DateTime, DateTimeZone, IllegalFieldValueException}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import sk.eset.dbsystems.spark._

import scala.collection.convert.WrapAsScala
import scala.io.Source
/**
  * Created by andrej.babolcai on 24. 2. 2016.
  *
  */
object OfflineIndexer {
  case class Config(
                     numShards:Int = 1,
                     indexName: String = "",
                     documentType: String = "",
                     snapshotRepoName: String = "",
                     snapshotName: String = "",
                     workDirName: String = "",
                     destDFSDir: String = "",
                     hadoopConfResource: Seq[String] = Seq(),
                     toBratislavaDate: Seq[String] = Seq(),
                     inputData: Seq[String] = Seq(),
                     mappingFile: File = new File("."),
                     settingsFile: File = new File("."),
                     tableSchema: Seq[(String,String)] = Seq(),
                     extractedFieldMapping: Map[String,String] = Map(),
                     partitionByField: Option[String] = None,
                     idField: Option[String] = None)

  def parseArgs(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("offline-indexer") {
      head("offline-indexer")

      opt[String]('I', "idField") optional() action {
        (x, c) => c.copy(idField = Some(x))
      } text "Field which value should be used as ID in elasticsearch"

      opt[String]('P', "partitionByField") optional() action {
        (x, c) => c.copy(partitionByField = Some(x))
      } text "Field by which Spark partitions the data"

      opt[Int]('p', "indexShardCnt") required() action {
        (x,c) => c.copy(numShards = x)
      } text "Number of shards per index, integer value"

      opt[String]('w', "workDirPath") required() action {
        (x,c) => c.copy(workDirName = x)
      } text "Workdir path. MUST EXIST"

      opt[String]('i', "indexName") required() valueName "<index name>" action {
        (x,c) => c.copy(indexName = x)
      } text "Name of index to be created"

      opt[String]('d', "documentType") required() valueName "<dokument type>" action {
        (x,c) => c.copy(documentType = x)
      } text "Document type for documents in index"

      opt[String]('r', "snapshotRepoName") required() valueName "<snapshot repo. name>" action {
        (x,c) => c.copy(snapshotRepoName = x)
      } text "Name of snapshot repository"

      opt[String]('s', "snapshotName") required() valueName "<snapshot name>" action {
        (x,c) => c.copy(snapshotName = x)
      } text "Name of snapshot"

      opt[String]('o', "destDFSDir") required() valueName "<destination dir in DFS>" action {
        (x,c) => c.copy(destDFSDir = x)
      } text "Output DFS path where snapshot will be created [MUST EXIST]"

      opt[Seq[String]]('c', "config") valueName "<hdfs-site.xml>,<core-site.xml>...>" action {
        (x,c) => c.copy(hadoopConfResource = x)
      } text "Optional configuration files"

      opt[Seq[String]]( "toBratislavaDate") valueName "<fieldName1>,<fieldName2>...>" action {
        (x,c) => c.copy(toBratislavaDate = x)
      } text "Optionally change field types to bratislava java date"

      opt[Seq[(String,String)]]("tableSchema") required() valueName "field1=type1,field2=type2..." action {
        (x,c) => c.copy(tableSchema = x)
      } text "Source parquet table schema"

      opt[File]('M',"mappingFile") valueName "<mappingFile.json>" action {
        (x,c) => c.copy(mappingFile = x)
      }

      opt[File]('S',"settingsFile") valueName "<settingsFile.json>" action {
        (x,c) => c.copy(settingsFile = x)
      }

      opt[Map[String,String]]("fieldsToExtractMapping") required() valueName "k1=v1,k2=v2..." action { (x, c) =>
        c.copy(extractedFieldMapping = x) } text "Fields mapping from source parquet to be extracted"

      help("help").text("prints this usage text")

      arg[String]("<dfs path> ...") minOccurs 1 action { (x, c) =>
        c.copy(inputData = c.inputData :+ x) } text "Input data to process"
    }

    //Run parser
    parser.parse(args, Config())
  }

  def get_xdna_something(xdna:String): Map[String,Int] = {
    val len = xdna.length
    return Map("count" -> len, "constant" -> 0)
  }

  def main(args: Array[String]): Unit = {
    val configOpt = OfflineIndexer.parseArgs(args)
    if(configOpt.isEmpty) { sys.exit(-1) }
    val config = configOpt.get

    var schemaStruct: StructType = new StructType
    config.tableSchema.foreach(x => schemaStruct = schemaStruct.add(x._1, x._2, nullable = true))

    val sparkConf = new SparkConf().setAppName("Index data path " + config.indexName)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //Read parquet to data frame
    val dfTemp = sqlContext.read.schema(schemaStruct).parquet(config.inputData:_*)

    import org.apache.spark.sql.functions._ // for `when` and other spark sql functions
    //Modify ID field if necessary, get original idField value or sha1 of concatenated all non null string fields
    val df =
      if(config.idField.isDefined)
        //Here we check if config.idField is null and when it is
        dfTemp.withColumn(config.idField.get,  //Replace id column
          when(dfTemp(config.idField.get).isNull, //when id column is null
            //we compute sha1 sum of a string that is a concatenation of all string fields, null fields are replaced with empty string ("")
            sha1(
              concat(
                //Get a array of column names
                dfTemp.columns
                  .map( col ) //convert them to Spark Columns
                  .map(x =>
                    when(x.isNull, "") //replace null columns with ""
                      .otherwise(x))   //when column is not null retain original value
              :_*) //Expand array so it can be used in concat function
            ) //Compute sha1 of concatenated fields
          )
          .otherwise(dfTemp(config.idField.get))) // when id column is not null retain original value
      else dfTemp

    //Check if response is null
    if (null == df) {
      return
    }

    val zoneBratislava: DateTimeZone = DateTimeZone.forID("Europe/Bratislava")
    val zoneUTC: DateTimeZone = DateTimeZone.forID("UTC")
    val datetimeFormatterBratislava: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(zoneBratislava)
    val datetimeFormatterUTC: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(zoneUTC)

    //Prepare field mapping map
    val justToObject: (String) => Object = (x:String) => x
    val toBratislavaDateObject: (String) => Object = (x:String) => {
      try {
        DateTime.parse(x, datetimeFormatterBratislava).toDate
      } catch {
        case _:IllegalFieldValueException => null
        case _:IllegalInstantException => DateTime.parse(x, datetimeFormatterUTC).toDate
        case exc:Exception => throw new RuntimeException(exc.getMessage)
      }
    }

    val fieldMapping = config.extractedFieldMapping.transform((k,v) =>
      if (config.toBratislavaDate.contains(k))
        (v,toBratislavaDateObject)
      else
        (v,justToObject))

    val broadcastedExtractedFieldMapping = sc.broadcast(fieldMapping)
    //Extract fields to index to a tuple

    //If partitioning field is defined, prepare RDD as key,value and partition acording to it
    if (config.partitionByField.isDefined) {
      val toIndexRdd = df.map(
        row => (row.getAs[String](config.partitionByField.get), broadcastedExtractedFieldMapping.value.map(
          mapping => (mapping._1, mapping._2._2(row.getAs[String](mapping._2._1)))
        ))
      )

      toIndexRdd.partitionBy(new HashPartitioner(config.numShards)).map(x => x._2).saveToESSnapshot(
        config.workDirName,
        config.indexName,
        config.documentType,
        FileUtils.readFileToString(config.mappingFile),
        FileUtils.readFileToString(config.settingsFile),
        config.snapshotName,
        config.snapshotName,
        config.destDFSDir,
        config.hadoopConfResource,
        config.idField
      )
    } else {
      val toIndexRdd = df.map(
        row => broadcastedExtractedFieldMapping.value.map(
          mapping => (mapping._1, mapping._2._2(row.getAs[String](mapping._2._1)))
        )
      )

      toIndexRdd.repartition(config.numShards).saveToESSnapshot(
        config.workDirName,
        config.indexName,
        config.documentType,
        FileUtils.readFileToString(config.mappingFile),
        FileUtils.readFileToString(config.settingsFile),
        config.snapshotName,
        config.snapshotName,
        config.destDFSDir,
        config.hadoopConfResource,
        config.idField
      )
    }

    println("All should be done, merge indices")

    ///Merge partitions
    //config hadoop
    val conf = new org.apache.hadoop.conf.Configuration()
    config.hadoopConfResource.map(new File(_).toURI.toURL).foreach(conf.addResource)
    val dfsClient = org.apache.hadoop.fs.FileSystem.get(conf)

    val local_index_0_filename = UUID.randomUUID().toString + "_index-0.json"

    dfsClient.copyToLocalFile(
      new Path(config.destDFSDir + "/to_resolve/" + config.indexName + "/index-0"),
      new Path(local_index_0_filename)
    )

    //Open and read index-0 file content
    val bufferedSource = Source.fromFile(local_index_0_filename)
    val fileContents = bufferedSource.getLines().mkString("")
    bufferedSource.close

    val input_0_data = JSON.parseFull(fileContents)
    val globalMap = input_0_data.get.asInstanceOf[Map[String,Any]]
    val indexMap = globalMap("indices").asInstanceOf[Map[String,Any]](config.indexName).asInstanceOf[Map[String,Any]]
    val indexId:String  = indexMap("id").asInstanceOf[String]
    val metaUUID:String = indexMap("snapshots").asInstanceOf[List[Map[String,Any]]].head("uuid").asInstanceOf[String]
    println(s"indexId $indexId, metaUUID $metaUUID")

    //We need to rename snap-... .dat files in all directories
    for( a <- 1 until config.numShards) {
      //Duck tape path to directory that contains snap..dat file
      val srcPathDir = config.destDFSDir + "/to_resolve/" + config.indexName + "/indices/" + a
      val shardFiles = dfsClient.listFiles(new Path(srcPathDir), false)

      var snapPath:Option[Path] = None
      while(shardFiles.hasNext) {
        val next = shardFiles.next()
        if (next.getPath.toString contains "/snap-") {
          snapPath = Some(next.getPath)
        }
      }

      if (snapPath.isEmpty) { throw new RuntimeException("Snap path is undefined") }

      //Duck tape destination snap... .dat name
      val destSnapPath = new Path(
        config.destDFSDir +
          "/to_resolve/" +
          config.indexName +
          "/indices/" +
          a +
          "/snap-" + metaUUID + ".dat" )

      print(s"rename ${snapPath.get.toString} to ${destSnapPath.toString}")
      //Rename snap file
      dfsClient.rename(snapPath.get, destSnapPath)

      //"Move" shard to ES index directory
      val destEsIndexPath = new Path(
        config.destDFSDir + "/to_resolve/" + config.indexName + "/indices/" + indexId + "/" + a
      )
      print(s"rename $srcPathDir to ${destEsIndexPath.toString}")
      dfsClient.rename(new Path(srcPathDir), destEsIndexPath)
    }

    //TODO cleanup
  }
}

