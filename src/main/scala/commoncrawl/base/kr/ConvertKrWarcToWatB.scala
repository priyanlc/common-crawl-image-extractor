package commoncrawl.base.kr

import commoncrawl.base.Configuration.createSparkSession
import commoncrawl.base.ExtractWatContents.{convertFromHdfsToDelta, convertWarcFilePathsToWatFilePaths}
import commoncrawl.base.FileOperations.exportHiveTableToHdfsCsv
import commoncrawl.base.DeltaLocalTable
import org.apache.spark.sql.SparkSession

object ConvertKrWarcToWatB {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSession("ConvertKrWatToWarc")

    val warcLinksFullyQualifiedHiveTableName = "raw.kr_warc_file_names"
    val watLinksFullyQualifiedHiveTableName = "raw.kr_wat_file_names"
    val warcUrlHiveTable: DeltaLocalTable = DeltaLocalTable(warcLinksFullyQualifiedHiveTableName, "",null)
    val watUrlHiveTable: DeltaLocalTable = DeltaLocalTable(watLinksFullyQualifiedHiveTableName, "",null)

    convertFromHdfsToDelta("kr/10k_warc_files/south_korea_10k_warc_files.csv", warcUrlHiveTable)
    convertWarcFilePathsToWatFilePaths(warcUrlHiveTable, watUrlHiveTable)
    exportHiveTableToHdfsCsv(watUrlHiveTable, "kr/10k_wat_files")
  }

}
