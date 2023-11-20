package commoncrawl.base

import commoncrawl.base.ExtractWatContents.{convertFromHdfsToHive, convertWarcFilePathsToWatFilePaths}
import commoncrawl.base.FileOperations.exportHiveTableToHdfsCsv
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ConvertWarcToWat {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("WatFileDownloader")
      .setMaster("local[*]")
      .set("HADOOP_USER_NAME", "hduser") // Use local for testing, replace with your cluster settings if applicable
      .set("spark.hadoop.fs.defaultFS", "hdfs://master:9000")

    implicit val spark: SparkSession = SparkSession.builder.config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS RAW ");

    val warcLinksFullyQualifiedHiveTableName = "RAW.JPN_WARC_FILE_NAMES"
    val watLinksFullyQualifiedHiveTableName = "RAW.JPN_WAT_FILE_NAMES"
    val warcUrlHiveTable: HiveTable = HiveTable(warcLinksFullyQualifiedHiveTableName, "")
    val watUrlHiveTable: HiveTable = HiveTable(watLinksFullyQualifiedHiveTableName, "")

    convertFromHdfsToHive("jpn/10k_warc_files/10k_warc_files.csv",warcUrlHiveTable)
    convertWarcFilePathsToWatFilePaths(warcUrlHiveTable,watUrlHiveTable)
    exportHiveTableToHdfsCsv(watUrlHiveTable,"jpn/10k_wat_files")
  }

}
