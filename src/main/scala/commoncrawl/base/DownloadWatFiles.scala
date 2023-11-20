package commoncrawl.base

import commoncrawl.base.Configuration.{downloadedWatFileState, pathToWatFileHdfsFolder, pathToWatFileStagingArea, watFileUrlListPath}
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

      val conf = new SparkConf()
        .setAppName("WatFileDownloader")
        .setMaster("local[*]")
        .set("HADOOP_USER_NAME", "hduser")// Use local for testing, replace with your cluster settings if applicable
        .set("spark.hadoop.fs.defaultFS", "hdfs://master:9000")

      implicit val spark: SparkSession =  SparkSession.builder.config(conf)
                                            .enableHiveSupport()
                                            .getOrCreate()

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
              copyToHdfs(pathToWatFileStagingArea + watFileName, pathToWatFileHdfsFolder)
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
