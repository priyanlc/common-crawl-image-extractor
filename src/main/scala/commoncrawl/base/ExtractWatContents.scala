package commoncrawl.base

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.explode

case class HiveTable(hiveTableName: String, hiveTablePath: String)

object ExtractWatContents {

  import org.apache.spark.sql.{DataFrame, SparkSession}



  def extractWATContentsAsDataframe(fullyQualifiedFilePath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val watFilesJsonRawDF = spark.read.json(fullyQualifiedFilePath)
    val payloadDF = watFilesJsonRawDF.select("Envelope")
    val htmlResponseMetadataDf = payloadDF.select("Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata")
    val linksDf = htmlResponseMetadataDf.select("HTML-Metadata.Links")
    val explodedLinksDf = linksDf.select(explode($"Links")).as("exploded_links").select("exploded_links.*")

    explodedLinksDf

  }

  def createHiveTableIfNotExist(fullyQualifiedTableName: String, hiveTablePath: String, df: DataFrame): Unit = {
       df.write.mode(SaveMode.Overwrite).option("path", hiveTablePath)saveAsTable(fullyQualifiedTableName)
  }

  private def appendDataframeToHiveTable(fullyQualifiedTableName: String, df: DataFrame): Unit ={
    df.write.mode(SaveMode.Append)saveAsTable(fullyQualifiedTableName)
  }

  def insertOrAppendToHiveTable( dataFrame: DataFrame)(hiveTable: HiveTable)(implicit spark: SparkSession): Unit = {

    if(spark.catalog.tableExists(hiveTable.hiveTableName))
      appendDataframeToHiveTable(hiveTable.hiveTableName,dataFrame)
    else
      createHiveTableIfNotExist(hiveTable.hiveTableName,hiveTable.hiveTablePath,dataFrame)

  }

  // Function to process a file
  def processFile(fullyQualifiedWatFileName:String, processedFolderPath: String)(hiveTable: HiveTable)(implicit spark: SparkSession): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    extractWatFileToHiveTable(fullyQualifiedWatFileName)(hiveTable)

    val fileName = new Path(fullyQualifiedWatFileName).getName
    val processedFilePath = new Path(processedFolderPath, s"processed_$fileName")
    fs.create(processedFilePath).close()
  }

  private def extractWatFileToHiveTable(fullyQualifiedWatFileName:String)(hiveTable: HiveTable)(implicit spark: SparkSession): Unit = {
    val df = extractWATContentsAsDataframe(fullyQualifiedWatFileName)
    insertOrAppendToHiveTable(df)(hiveTable)
  }

  def listHdfsFiles(rawWatFilesPath: String)(implicit spark: SparkSession): Array[String] = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // List files in HDFS folder
    val fileStatuses = fs.listStatus(new Path(rawWatFilesPath))
    val filePaths = fileStatuses.map(_.getPath.toString)
    fs.close()
    filePaths
  }

  def getProcessedFileNames(processedFilePath:String)(implicit spark:SparkSession): Set[String] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var processedFiles = Set[String]()
    val processedFilesPath = new Path(processedFilePath)
    if (fs.exists(processedFilesPath)) {
      val processedFileStatuses = fs.listStatus(processedFilesPath)
      processedFileStatuses.foreach { status =>
        processedFiles += status.getPath.toString
      }
    }
    fs.close()
    processedFiles
  }

  def processRawWatFiles(rawWatFilesPath:String,processedFilePath:String)(hiveTable: HiveTable)(implicit spark:SparkSession) : Set[String] ={

   var processedFiles =  getProcessedFileNames(processedFilePath)
   val watFilePaths =  listHdfsFiles(rawWatFilesPath)

    watFilePaths.foreach { filePath =>
      if (!processedFiles.contains(filePath)) {
        // Process the file
        processFile(filePath,processedFilePath)(hiveTable)
        // Add the processed file to the set
        processedFiles += filePath
      }
    }
    processedFiles
  }

}
