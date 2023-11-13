package commoncrawl.base

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, explode}

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
    val flattenedDF = explodedLinksDf.select(
      col("col.path").alias("path"),
      col("col.target").alias("target"),
      col("col.text").alias("text"),
      col("col.title").alias("title"),
      col("col.url").alias("url")
    )
    flattenedDF

  }

  def createHiveTableIfNotExist(fullyQualifiedTableName: String, hiveTablePath: String, df: DataFrame): Unit = {
       df.write.mode(SaveMode.Overwrite)
         .format("parquet")
         .option("path", hiveTablePath)
         .saveAsTable(fullyQualifiedTableName)
  }

  private def appendDataframeToHiveTable(fullyQualifiedTableName: String, df: DataFrame): Unit ={
    df.write.mode(SaveMode.Append)
      .format("parquet")
      .saveAsTable(fullyQualifiedTableName)
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

 def  extractJpgJpegFromAllUrls(explodedRawWatFilesHiveTable:HiveTable,jpgUrlHiveTable:HiveTable)(implicit spark:SparkSession): Unit = {
   val dfRawUnfilteredUrls= spark.read.table(explodedRawWatFilesHiveTable.hiveTableName)
   val jpgUrlsDF = dfRawUnfilteredUrls
                            .filter(dfRawUnfilteredUrls("url").isNotNull)
                            .where("lower(url) like 'http%.jpg'")

   val jpegUrlsDF = dfRawUnfilteredUrls
                            .filter(dfRawUnfilteredUrls("url").isNotNull)
                            .where("lower(url) like 'http%.jpeg'")

   jpgUrlsDF.printSchema()

   insertOrAppendToHiveTable(jpgUrlsDF)(jpgUrlHiveTable)
   insertOrAppendToHiveTable(jpegUrlsDF)(jpgUrlHiveTable)

 }

def extractPngFromAllUrls(explodedRawWatFilesHiveTable: HiveTable, pngUrlHiveTable: HiveTable)(implicit spark: SparkSession): Unit = {
    val dfRawUnfilteredUrls = spark.read.table(explodedRawWatFilesHiveTable.hiveTableName)
    val pngUrlsDF = dfRawUnfilteredUrls
      .filter(dfRawUnfilteredUrls("url").isNotNull)
      .where("lower(url) like 'http%.png'")

    insertOrAppendToHiveTable(pngUrlsDF)(pngUrlHiveTable)

}

 def  deduplicateUrls(urlHiveTable: HiveTable, distinctUrlHiveTable: HiveTable)(implicit spark: SparkSession) : Unit = {
   val dfUrls = spark.read.table(urlHiveTable.hiveTableName)
   val dfDeduplicatedUrls= dfUrls.dropDuplicates("url")

   insertOrAppendToHiveTable(dfDeduplicatedUrls)(distinctUrlHiveTable)

 }


  def exportHiveTableToHdfsCsv(hiveTable: HiveTable, hdfsPath: String)(implicit spark: SparkSession): Unit = {
    val warcFileListDf = spark.table(hiveTable.hiveTableName)
    import spark.implicits._
    warcFileListDf.coalesce(1).write.option("sep", "|").csv(hdfsPath)
  }



}
