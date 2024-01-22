package commoncrawl.base.jpn

import commoncrawl.base.Configuration.{createSparkSession, jpnJpgLinks01HiveTable, oneJpnJpgLinksHiveTable}
import commoncrawl.base.FileOperations.exportHiveTableToHdfsCsv
import org.apache.spark.sql.SparkSession

object ExportJpgLinksAsFile {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession("ExportJpgLinks")
   // exportHiveTableToHdfsCsv(jpnJpgLinks01HiveTable,"jpn/export_jpg_links/jpg_links_01.csv")
    exportHiveTableToHdfsCsv(oneJpnJpgLinksHiveTable,"jpn/export_jpg_links/jpg_links_one_file.csv")
  }

}
