package commoncrawl.base

import commoncrawl.base.Configuration.{createSparkSession, createSparkSessionWithDelta, explodedSampleRawWatFilesHiveTable, extractedSampleWatFileList, pathToSampleWatFileHdfsFolder}
import commoncrawl.base.ExtractWatContents.processRawWatFiles
import org.apache.spark.sql.SparkSession

object ExtractSampleLinksFromWatFiles {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSession("ExtractSampleLinksFromWatFiles")
    val processedFileNames = processRawWatFiles(pathToSampleWatFileHdfsFolder, extractedSampleWatFileList)(explodedSampleRawWatFilesHiveTable)
    processedFileNames.foreach(f => println(f))
  }

}
