package commoncrawl.base

import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.io.{BufferedInputStream, FileOutputStream}
import java.net.URL
import scala.util.{Try, Using}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.{BufferedInputStream, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import scala.util.{Try, Using}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.{Try, Using}
import scala.util.control.NonFatal
import java.io.{BufferedInputStream, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import scala.util.{Try, Using}
import java.io.{BufferedInputStream, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import scala.util.{Failure, Success, Try}
import java.io.{BufferedInputStream, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import scala.util.{Failure, Success, Try}
import java.io.{BufferedInputStream, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}



object FileOperations {

  def moveToArchive(filePath: String, archivePath: String): Unit = {
    val sourcePath = Paths.get(filePath)
    val destinationPath = Paths.get(archivePath, sourcePath.getFileName.toString)

    Files.move(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING)
  }




  def copyToHdfs(localFilePath: String, hdfsDestPath: String, hdfsClient: FileSystem, maxRetries: Int = 3): Try[Unit] = {
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




  def downloadFile(watFileUrl: String, destinationPath: String): Try[Unit] = {
    @tailrec
    def attemptDownload(retries: Int): Try[Unit] = {
      Try {
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
          } finally {
            inputStream.close()
            fileOutputStream.close()
          }
        } finally {
          connection.disconnect()
        }
        println("### Downloaded  file " +watFileUrl)
      } match {
        case Success(_) =>
          Success(())
        case Failure(e) =>
          if (retries > 0) {
            println(s"Failed to download file, retrying in 5 seconds... ($retries retries left), error: ${e.getMessage}")
            Thread.sleep(10000) // Wait for 5 seconds before retrying
            attemptDownload(retries - 1)
          } else {
            Failure(e)
          }
      }
    }

    attemptDownload(10) // Number of retries is set to 10
  }

  def exportHiveTableToHdfsCsv(hiveTable: HiveTable, hdfsPath: String)(implicit spark: SparkSession): Unit = {
    val warcFileListDf = spark.table(hiveTable.hiveTableName)
    import spark.implicits._
    warcFileListDf.coalesce(1)
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



  def readWatFilesList(hdfsClient: FileSystem, watFilePath: String): Seq[String] = {

    val directoryPath = new Path(watFilePath)
    val fileNames = ListBuffer[String]()

    if (hdfsClient.isDirectory(directoryPath)) {
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

  def keepTrackOfProcessedFile(hdfsClient: FileSystem, fileName: String, processedFolderPath: String): Unit = {
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

}
