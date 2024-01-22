package commoncrawl.base.jpn

//import commoncrawl.base.Configuration.{createSparkSession, createSparkSessionWithDelta, extractedSingleJpnWatFileList, oneExplodedJpnRawWatFilesHiveTable, pathToSingleJpnWatFileHdfsFolder}
import commoncrawl.base.ExtractWatContents.processRawWatFiles
import org.apache.spark.sql.SparkSession

object ExtractJpnLinksFromSingleWatFile {

  def main(args: Array[String]): Unit = {

//    implicit val spark: SparkSession = createSparkSession("ExtractOneJpnLinksFromWatFiles")
  //  implicit val spark: SparkSession = createSparkSessionWithDelta("SparkSessionWithDelta")
    //val processedFileNames = processRawWatFiles(pathToSingleJpnWatFileHdfsFolder, extractedSingleJpnWatFileList)(oneExplodedJpnRawWatFilesHiveTable)
    //processedFileNames.foreach(f => println(f))
  }

}
