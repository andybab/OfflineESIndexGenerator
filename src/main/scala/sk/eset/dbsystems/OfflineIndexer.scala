package sk.eset.dbsystems

import java.io.File

import com.cloudera.org.joda.time.IllegalInstantException
import my.elasticsearch.joda.time.{DateTime, DateTimeZone, IllegalFieldValueException}
import my.elasticsearch.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import sk.eset.dbsystems.spark._
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
                     partitionByField: Option[String] = None)

  def parseArgs(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("offline-indexer") {
      head("offline-indexer")

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
    val df = sqlContext.read.schema(schemaStruct).parquet(config.inputData:_*)

    //Check if response is null
    if (null == df) {
      return
    }

    print("count {}", df.count())

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
        case ifve:IllegalFieldValueException => null
        case iie:IllegalInstantException => DateTime.parse(x, datetimeFormatterUTC).toDate
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
        config.hadoopConfResource)
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
        config.hadoopConfResource)
    }
  }
}
