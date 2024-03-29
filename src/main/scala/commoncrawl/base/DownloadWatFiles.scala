package commoncrawl.base

import commoncrawl.base.Configuration.{createSparkSession, downloadedWatFileState, pathToWatFileHdfsFolder2, pathToWatFileStagingArea, watFileUrlListPath}
import commoncrawl.base.FileOperations.{copyToHdfs, downloadFile, extractFileNameFromURL, getProcessedFileNames, keepTrackOfProcessedFile, pauseExecution, readWatFilesList, time}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.security.UserGroupInformation

object DownloadWatFiles {


  def main(args: Array[String]): Unit = {
    time {

      UserGroupInformation.createProxyUser("hduser", UserGroupInformation.getCurrentUser())

      if (args.length < 1 || !args(0).matches("\\d+")) {
        System.err.println("Usage: MyApp <numberOfFilesToDownload>")
        System.exit(1)
      }

      implicit val spark: SparkSession =  createSparkSession("DownloadWatFIles")

      val numberOfFilesToDownload = args(0).toInt

      var processedFiles = getProcessedFileNames(downloadedWatFileState)

      val watFiles = readWatFilesList( watFileUrlListPath)

      var filesDownloaded = 0

      // Process each file until the limit is reached
      watFiles.foreach { watFileUrl =>
        val watFileName = extractFileNameFromURL(watFileUrl)
        if (!processedFiles.contains(watFileName) && filesDownloaded < numberOfFilesToDownload) {
          if (downloadFile(watFileUrl, pathToWatFileStagingArea)) {
              pauseExecution(5)
              copyToHdfs(pathToWatFileStagingArea + watFileName, pathToWatFileHdfsFolder2)
              pauseExecution(5)
              keepTrackOfProcessedFile( watFileName, downloadedWatFileState)
              processedFiles += watFileName

              filesDownloaded += 1
          }
        }
      }

      spark.stop()
      println("Number of files downloaded " + filesDownloaded.toInt)

    }
  }

}
