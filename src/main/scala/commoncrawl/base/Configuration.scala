package commoncrawl.base

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Configuration {

  val watFileUrlListPath = "hdfs://master:9000/user/hduser/jpn/10k_wat_files/"

  val pathToWatFileStagingArea = "/media/priyan/data1/hdfs/temp/"



  val pathToWatFileHdfsFolder2 = "hdfs://master:9000/user/hduser/jpn/raw_wat_files_2/"

  val downloadedWatFileState = "hdfs://master:9000/user/hduser/jpn/downloaded_wat_file_list/"



  val extractedSampleWatFileList = "hdfs://master:9000/user/hduser/jpn/extracted_sample_wat_file_list/"



  val rawJpnLinksFullyQualifiedHiveTableName = "RAW.JPN_WAT_HTTP_LINKS_01"
  val rawLinksHiveTablePath = "hdfs://master:9000/user/hive/warehouse/raw.db/wat_http_links"



  val rawSampleLinksFullyQualifiedHiveTableName = "raw.sample_wat_http_links"

  val rawSampleLinksHiveTablePath = "hdfs://master:9000/user/hive/warehouse/raw.db/sample_wat_http_links"

  val pathToSampleWatFileHdfsFolder = "hdfs://master:9000/user/hduser/jpn/sample_wat_files/"

  val jpgLinksFullyQualifiedHiveTableName = "curated.jpg_http_links"

  val jpnJpgLinks01FullyQualifiedHiveTableName = "curated.jpn_jpg_http_links_01"
  val jpgLinksHiveTablePath = "hdfs://master:9000/user/hive/warehouse/curated.db/jpn_jpg_http_links"

  val jpgLinks01HiveTablePath = "hdfs://master:9000/user/hive/warehouse/curated.db/jpn_jpg_http_links01"


  val pngLinksFullyQualifiedHiveTableName = "CURATED.PNG_HTTP_LINKS"

  val pngLinksHiveTablePath = "hdfs://master:9000/user/hive/warehouse/curated.db/png_http_links"

 // val explodedJpnRawWatFilesHiveTable: HiveTable = HiveTable(rawLinksFullyQualifiedHiveTableName, rawLinksHiveTablePath)

  val explodedSampleRawWatFilesHiveTable: DeltaLocalTable = DeltaLocalTable(rawSampleLinksFullyQualifiedHiveTableName, rawSampleLinksHiveTablePath,null)

  val jpgLinksHiveTable: DeltaLocalTable = DeltaLocalTable(jpgLinksFullyQualifiedHiveTableName, jpgLinksHiveTablePath,null)

  val jpnJpgLinks01HiveTable: DeltaLocalTable = DeltaLocalTable(jpnJpgLinks01FullyQualifiedHiveTableName, jpgLinks01HiveTablePath,null)

  val pngLinksHiveTable: DeltaLocalTable = DeltaLocalTable(pngLinksFullyQualifiedHiveTableName, pngLinksHiveTablePath,null)



 // val oneExplodedJpnRawWatFilesHiveTable: DeltaLocalTable = DeltaLocalTable(oneRawJpnLinksFullyQualifiedHiveTableName, oneRawLinksHiveTablePath,null)

  val oneJpnJpgLinksFullyQualifiedHiveTableName = "curated.one_jpn_jpg_http_links"
  val oneJpgLinks01HiveTablePath = "hdfs://master:9000/user/hive/warehouse/curated.db/one_jpn_jpg_http_links"
  val oneJpnJpgLinksHiveTable: DeltaLocalTable = DeltaLocalTable(oneJpnJpgLinksFullyQualifiedHiveTableName, oneJpgLinks01HiveTablePath,null)

  val pathToSingleJpnWatFileHdfsFolder = "hdfs://master:9000/user/hduser/jpn/sample_wat_files/one"
  val extractedSingleJpnWatFileList = "hdfs://master:9000/user/hduser/jpn/extracted_one_wat_file_list/"

  //KR

  val downloadedKrWatFileState = "hdfs://master:9000/user/hduser/kr/downloaded_wat_file_list/"

  val watFileKrUrlListPath = "hdfs://master:9000/user/hduser/kr/10k_wat_files/"

  val pathToKrWatFileHdfsFolder1 = "hdfs://master:9000/user/hduser/kr/raw_wat_files_1/"

  def createSparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.sql.caseSensitive", "true")
      .set("HADOOP_USER_NAME", "hduser") // Use local for testing, replace with your cluster settings if applicable
      .set("spark.hadoop.fs.defaultFS", "hdfs://master:9000")
      .set("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")

    SparkSession.builder.config(conf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def createSparkSessionWithDelta(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.sql.caseSensitive", "true")
      .set("HADOOP_USER_NAME", "hduser") // Use local for testing, replace with your cluster settings if applicable
      .set("spark.hadoop.fs.defaultFS", "hdfs://master:9000")
      .set("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")



    SparkSession.builder.config(conf)
      .getOrCreate()
  }

  // TODO : https://stackoverflow.com/questions/43619137/hive-tables-not-found-in-spark-sql-spark-sql-analysisexception-in-cloudera-vm

}
