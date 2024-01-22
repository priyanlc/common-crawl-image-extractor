package commoncrawl.base

import commoncrawl.base.Configuration.{createSparkSession, explodedSampleRawWatFilesHiveTable, jpgLinksHiveTable}
import commoncrawl.base.ExtractWatContents.extractJpgJpegFromAllUrls
import org.apache.spark.sql.SparkSession

object ExtractSampleJpgLinks {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSession("ExtractSampleJpgLinks")
    extractJpgJpegFromAllUrls(explodedSampleRawWatFilesHiveTable, jpgLinksHiveTable)

  }

}
