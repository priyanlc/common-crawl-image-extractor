package commoncrawl.base.kr

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.ExtractWatContents.processRawWatFiles
import commoncrawl.base.kr.KrConfiguration.{explodedRawWatFilesDeltaTable, extractedKrWatFileList, pathToKrWatFileHdfsFolder}
import org.apache.spark.sql.SparkSession

object ExtractHttpLinksFromKoreaWatFiles {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractHttpLinksFromKoreaWatFiles")
    val processedFileNames = processRawWatFiles(pathToKrWatFileHdfsFolder,extractedKrWatFileList)(explodedRawWatFilesDeltaTable)
    processedFileNames.foreach(f=>println(f))
  }

}
