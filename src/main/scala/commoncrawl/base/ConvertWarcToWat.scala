package commoncrawl.base

import commoncrawl.base.Configuration.createSparkSession
import commoncrawl.base.ExtractWatContents.{convertFromHdfsToDelta, convertWarcFilePathsToWatFilePaths}
import commoncrawl.base.FileOperations.exportHiveTableToHdfsCsv
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ConvertWarcToWat {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSession("ConvertWatToWarc")

    spark.sql("CREATE DATABASE IF NOT EXISTS RAW ");

    val warcLinksFullyQualifiedHiveTableName = "RAW.JPN_WARC_FILE_NAMES"
    val watLinksFullyQualifiedHiveTableName = "RAW.JPN_WAT_FILE_NAMES"
    val warcUrlHiveTable: DeltaLocalTable = DeltaLocalTable(warcLinksFullyQualifiedHiveTableName, "",null)
    val watUrlHiveTable: DeltaLocalTable = DeltaLocalTable(watLinksFullyQualifiedHiveTableName, "",null)

    convertFromHdfsToDelta("jpn/10k_warc_files/10k_warc_files.csv",warcUrlHiveTable)
    convertWarcFilePathsToWatFilePaths(warcUrlHiveTable,watUrlHiveTable)
    exportHiveTableToHdfsCsv(watUrlHiveTable,"jpn/10k_wat_files")
  }


}
