package commoncrawl.base

import commoncrawl.base.FileOperations.{getProcessedFileNames, keepTrackOfProcessedFile, listHdfsFiles}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.{SaveMode, functions}
import org.apache.spark.sql.functions.{col, explode, lit, regexp_replace}

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
    extractWatFileToHiveTable(fullyQualifiedWatFileName)(hiveTable)
    val fileName = new Path(fullyQualifiedWatFileName).getName
    val hdfsClient = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    keepTrackOfProcessedFile(fileName,processedFolderPath)

  }

  private def extractWatFileToHiveTable(fullyQualifiedWatFileName:String)(hiveTable: HiveTable)(implicit spark: SparkSession): Unit = {
    val df = extractWATContentsAsDataframe(fullyQualifiedWatFileName)
    insertOrAppendToHiveTable(df)(hiveTable)
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

  def convertFromHdfsToHive(hdfsPath: String,hiveTable: HiveTable)(implicit spark: SparkSession): Unit = {
    val csvDF = spark.read
      .option("header", "true") // use "true" if your file has a header row, else "false"
      .option("inferSchema", "true") // to automatically infer data types; else, define the schema manually
      .csv(hdfsPath)

    csvDF.write.mode("overwrite").saveAsTable(hiveTable.hiveTableName)
  }

  def convertWarcFilePathsToWatFilePaths(warcFileListTable: HiveTable, watFileDestination: HiveTable)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val prefixVal = "https://data.commoncrawl.org/"

    val warcFileListDf = spark.table(warcFileListTable.hiveTableName)
    val intermediateWatFileNameDf = warcFileListDf
                                    .withColumn("wat_filename_temp", regexp_replace(warcFileListDf("warc_filename"), "\\/warc\\/", "\\/wat\\/"))
                                    .drop("warc_filename")
    val watFileNameDf = intermediateWatFileNameDf
                          .withColumn("wat_filename", regexp_replace(intermediateWatFileNameDf("wat_filename_temp"), "\\.warc", "\\.warc.wat"))
                          .drop("wat_filename_temp")

    watFileNameDf.withColumn("full_wat_path", functions.concat(lit(prefixVal), watFileNameDf.col("wat_filename")))
      .drop("wat_filename")
      .write.mode(SaveMode.Overwrite).saveAsTable(watFileDestination.hiveTableName)

   spark.table(watFileDestination.hiveTableName)
  }


}
