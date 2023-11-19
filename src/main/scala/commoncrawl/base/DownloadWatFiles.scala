package commoncrawl.base

import commoncrawl.base.Configuration.{downloadedWatFileState, pathToWatFileHdfsFolder, pathToWatFileStagingArea, watFileUrlListPath}
import commoncrawl.base.FileOperations.{copyToHdfs, downloadFile, extractFileNameFromURL, getProcessedFileNames, keepTrackOfProcessedFile, moveToArchive, pauseExecution, readWatFilesList, time}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.SparkSession

object DownloadWatFiles {


  /*
    Hey Chat,
    Consider the following code extract and descriptions. I want to combine them to a main method.

    Description of the main method - I want load a file which will contain wat files links.
    I want to specify how many files need to be downloaded in one go.
    These wat files need to downloaded sequentially.

    For each downloaded file I need to send it to a HDFS location.
    I then want to archive this file.
    The file already downloaded should be saved in a separate location. This list will be used to
    compare against the already downloaded list. Already downloaded files should not be downloaded again.
   */


  def main(args: Array[String]): Unit = {
    time {

      if (args.length < 1 || !args(0).matches("\\d+")) {
        System.err.println("Usage: MyApp <numberOfFilesToDownload>")
        System.exit(1)
      }

      implicit val spark: SparkSession = SparkSession.builder()
        .appName("My Spark Application")
        .config("spark.master", "local")
        .getOrCreate()

      val numberOfFilesToDownload = args(0).toInt

      val hdfsClient = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      var processedFiles = getProcessedFileNames(downloadedWatFileState)

      val watFiles = readWatFilesList(hdfsClient, watFileUrlListPath)

      var filesDownloaded = 0

      // Process each file until the limit is reached
      watFiles.foreach { watFileUrl =>
        val watFileName = extractFileNameFromURL(watFileUrl)
        if (!processedFiles.contains(watFileName) && filesDownloaded < numberOfFilesToDownload) {
          downloadFile(watFileUrl, pathToWatFileStagingArea)
          pauseExecution(5)
          copyToHdfs(pathToWatFileStagingArea + watFileName, pathToWatFileHdfsFolder, hdfsClient)
          pauseExecution(5)
          keepTrackOfProcessedFile(hdfsClient, watFileName, downloadedWatFileState)
          processedFiles += watFileName

          filesDownloaded += 1
        }
      }

      spark.stop()

    }
  }

}
