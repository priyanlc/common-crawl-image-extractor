package commoncrawl.base

import commoncrawl.base.Configuration.{createSparkSession, pngLinksHiveTable}
import commoncrawl.base.ExtractWatContents.extractPngFromAllUrls
import org.apache.spark.sql.SparkSession

object ExtractPngLinks {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSession("ExtractPngLinks")
   // extractPngFromAllUrls(explodedJpnRawWatFilesHiveTable, pngLinksHiveTable)

  }

}
