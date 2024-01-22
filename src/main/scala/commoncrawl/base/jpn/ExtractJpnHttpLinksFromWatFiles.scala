package commoncrawl.base.jpn

import commoncrawl.base.Configuration.{createSparkSession, createSparkSessionWithDelta}
import commoncrawl.base.ExtractWatContents.processRawWatFiles
import commoncrawl.base.jpn.JpnConfiguration.{explodedJpnRawWatFilesDeltaTable, extractedJpnWatFileList, pathToJpnWatFileHdfsFolder}
import org.apache.spark.sql.SparkSession

object ExtractJpnHttpLinksFromWatFiles {
  /**
   *
   *
   */
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpnLinksFromWatFiles")
    val processedFileNames = processRawWatFiles(pathToJpnWatFileHdfsFolder,extractedJpnWatFileList)(explodedJpnRawWatFilesDeltaTable)
    processedFileNames.foreach(f=>println(f))
  }

}
