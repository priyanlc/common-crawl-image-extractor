package commoncrawl.base.kr

import commoncrawl.base.Configuration.{createSparkSession, downloadedKrWatFileState, downloadedWatFileState, pathToKrWatFileHdfsFolder1, pathToWatFileHdfsFolder2, pathToWatFileStagingArea, watFileKrUrlListPath, watFileUrlListPath}
import commoncrawl.base.FileOperations.{copyToHdfs, downloadFile, extractFileNameFromURL, getProcessedFileNames, keepTrackOfProcessedFile, pauseExecution, readWatFilesList, time}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession

object DownloadKrWatFilesB {

  def main(args: Array[String]): Unit = {
    time {

      UserGroupInformation.createProxyUser("hduser", UserGroupInformation.getCurrentUser())

      if (args.length < 1 || !args(0).matches("\\d+")) {
        System.err.println("Usage: MyApp <numberOfFilesToDownload>")
        System.exit(1)
      }

      implicit val spark: SparkSession = createSparkSession("DownloadKrWatFIles")

      val numberOfFilesToDownload = args(0).toInt

      var processedFiles = getProcessedFileNames(downloadedKrWatFileState)


      
      val watFiles = readWatFilesList(watFileKrUrlListPath)

      var filesDownloaded = 0

      // Process each file until the limit is reached
      watFiles.foreach { watFileUrl =>
        val watFileName = extractFileNameFromURL(watFileUrl)
        if (!processedFiles.contains(watFileName) && filesDownloaded < numberOfFilesToDownload) {
          if (downloadFile(watFileUrl, pathToWatFileStagingArea)) {
            pauseExecution(5)
            copyToHdfs(pathToWatFileStagingArea + watFileName, pathToKrWatFileHdfsFolder1)
            pauseExecution(5)
            keepTrackOfProcessedFile(watFileName, downloadedKrWatFileState)
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
