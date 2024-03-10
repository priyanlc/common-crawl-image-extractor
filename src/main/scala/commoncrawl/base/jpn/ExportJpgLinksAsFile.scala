package commoncrawl.base.jpn

import commoncrawl.base.Configuration.{createSparkSession, createSparkSessionWithDelta, jpnJpgLinks01HiveTable, oneJpnJpgLinksHiveTable}
import commoncrawl.base.FileOperations.exportHiveTableToHdfsCsv
import commoncrawl.base.jpn.JpnConfiguration.jpnJpgLinksDeltaTable
import org.apache.spark.sql.SparkSession

object ExportJpgLinksAsFile {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSessionWithDelta("ExportJpgLinks")
    exportHiveTableToHdfsCsv(jpnJpgLinksDeltaTable,"jpn/export_jpg_links/jpg_links_4kwat_file.csv")
  }

}
