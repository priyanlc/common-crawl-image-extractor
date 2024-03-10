package commoncrawl.base

import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.util.{Try, Using}
import org.apache.spark.sql.{AnalysisException, SparkSession}

import scala.collection.mutable.ListBuffer
import java.io.BufferedReader
import java.io.InputStreamReader
import scala.util.control.NonFatal

object FileOperations {

  def moveToArchive(filePath: String, archivePath: String): Unit = {
    val sourcePath = Paths.get(filePath)
    val destinationPath = Paths.get(archivePath, sourcePath.getFileName.toString)

    Files.move(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING)
  }


  def copyToHdfs(localFilePath: String, hdfsDestPath: String, maxRetries: Int = 3)(implicit spark: SparkSession): Try[Unit] = {
    val hdfsClient = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    def attemptCopy(retries: Int): Try[Unit] = {
      Using.Manager { use =>
        val localPath = new Path(localFilePath)
        val destPath = new Path(hdfsDestPath)

        // Check if local file exists
        if (!new java.io.File(localFilePath).exists()) {
          throw new java.io.FileNotFoundException(s"Local file not found: $localFilePath")
        }

        // Create destination directory if it doesn't exist
        if (!hdfsClient.exists(destPath)) {
          hdfsClient.mkdirs(destPath)
        }

        val destFilePath = new Path(hdfsDestPath + "/" + localPath.getName)

        // Copy file from local file system to HDFS
        hdfsClient.copyFromLocalFile(localPath, destFilePath)
        println("### Copied to HDFS " +localPath.getName)
      }.recoverWith {
        case NonFatal(e) if retries > 0 =>
          println(s"Failed to copy file to HDFS, retrying... ($retries retries left), error: ${e.getMessage}")
          attemptCopy(retries - 1)
        case NonFatal(e) =>
          println(s"Failed to copy file to HDFS, no more retries, error: ${e.getMessage}")
          scala.util.Failure(e)
      }
    }

    attemptCopy(maxRetries)
  }


  import java.io.{BufferedInputStream, FileOutputStream, IOException}
  import java.net.{HttpURLConnection, URL}
  import scala.annotation.tailrec

  def downloadFile(watFileUrl: String, destinationPath: String): Boolean = {
    @tailrec
    def attemptDownload(retries: Int): Boolean = {
      try {
        val url = new URL(watFileUrl)
        val connection = url.openConnection().asInstanceOf[HttpURLConnection]

        try {
          connection.setRequestMethod("GET")

          val responseCode = connection.getResponseCode
          if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException(s"Server returned HTTP response code: $responseCode for URL: $watFileUrl")
          }

          val inputStream = new BufferedInputStream(connection.getInputStream)
          val fileOutputStream = new FileOutputStream(s"$destinationPath/${url.getPath.split("/").last}")

          try {
            val buffer = new Array[Byte](1024)
            var bytesRead: Int = 0

            while ( {
              bytesRead = inputStream.read(buffer)
              bytesRead != -1
            }) {
              fileOutputStream.write(buffer, 0, bytesRead)
            }
            println("### Downloaded file " + watFileUrl)
            true
          } finally {
            inputStream.close()
            fileOutputStream.close()
          }
        } finally {
          connection.disconnect()
        }
      } catch {
        case e: Exception =>
          if (retries > 0) {
            println(s"Failed to download file, retrying in 5 seconds... ($retries retries left), error: ${e.getMessage}")
            Thread.sleep(5000) // Wait for 5 seconds before retrying
            attemptDownload(retries - 1)
          } else {
            println(s"Failed to download file, no more retries, error: ${e.getMessage}")
            false
          }
      }
    }

    attemptDownload(10) // Number of retries is set to 10
  }


  def exportHiveTableToHdfsCsv(deltaTable: DeltaLocalTable, hdfsPath: String)(implicit spark: SparkSession): Unit = {
    val warcFileListDf= spark.read.format("delta").load(deltaTable.deltaTablePathExtended)
    import spark.implicits._
    warcFileListDf.coalesce(100)
      .write
      .option("sep", "|")
      .mode("overwrite")
      .csv(hdfsPath)
  }

  def listHdfsFiles(rawWatFilesPath: String)(implicit spark: SparkSession): Array[String] = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // List files in HDFS folder
    val fileStatuses = fs.listStatus(new Path(rawWatFilesPath))
    val filePaths = fileStatuses.map(_.getPath.toString)
    fs.close()
    filePaths
  }

  def getProcessedFileNames(processedFilePath: String)(implicit spark: SparkSession): Set[String] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var processedFiles = Set[String]()
    val processedFilesPath = new Path(processedFilePath)
    if (fs.exists(processedFilesPath)) {
      val processedFileStatuses = fs.listStatus(processedFilesPath)
      processedFileStatuses.foreach { status =>
        processedFiles += extractFileNameFromURL(status.getPath.toString)
      }
    }
    fs.close()
    processedFiles
  }



  def readWatFilesList(watFilePath: String)(implicit spark: SparkSession): Seq[String] = {

    val hdfsClient = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val directoryPath = new Path(watFilePath)
    val fileNames = ListBuffer[String]()

    if (hdfsClient.exists(directoryPath)) {
      val statusList: Array[FileStatus] = hdfsClient.listStatus(directoryPath)
      for (status <- statusList) {
        val filePath = status.getPath
        val inputStream = hdfsClient.open(filePath)
        val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))

        try {
          var line: String = bufferedReader.readLine()
          while (line != null) {
            fileNames += line
            line = bufferedReader.readLine()
          }
        } finally {
          bufferedReader.close()
          inputStream.close()
        }
      }
    }

    fileNames.toSeq
  }

  def keepTrackOfProcessedFile(fileName: String, processedFolderPath: String)(implicit spark: SparkSession): Unit = {
    val hdfsClient = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val processedFilePath = new Path(processedFolderPath, fileName)
    hdfsClient.create(processedFilePath).close()
  }

  def extractFileNameFromURL(url: String): String = {
    val lastSlashIndex = url.lastIndexOf("/")
    if (lastSlashIndex >= 0 && lastSlashIndex < url.length - 1) {
      url.substring(lastSlashIndex + 1)
    } else {
      ""
    }
  }

  def time[R](block: => R): R = {
    val start = System.nanoTime()
    val result = block // call-by-name
    val end = System.nanoTime()
    println(s"Elapsed time: ${(end - start) / (1e9*60)} minutes")
    result
  }

  def pauseExecution(seconds: Int): Unit = {
    Thread.sleep(seconds * 1000L) // Multiply by 1000 to convert seconds to milliseconds
  }

  def doesDeltaTableExist(tablePath: String)(implicit spark: SparkSession): Boolean = {
    try {
      // Attempt to read the Delta table
      spark.read.format("delta").load(tablePath)
      true
    } catch {
      case e: AnalysisException if e.message.contains("Path does not exist") =>
        false
      case e: Throwable => throw e // Re-throw other exceptions
    }
  }

}
