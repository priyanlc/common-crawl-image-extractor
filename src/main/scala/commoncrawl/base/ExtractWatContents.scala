package commoncrawl.base

import commoncrawl.base.FileOperations.{getProcessedFileNames, keepTrackOfProcessedFile, listHdfsFiles}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, functions}
import org.apache.spark.sql.functions.{col, explode, lit, regexp_replace}

case class DeltaLocalTable(deltaTableName: String, deltaTablePath: String, deltaTablePathExtended: String)

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

  def createDeltaTableIfNotExist(deltaTable:DeltaLocalTable, df: DataFrame): Unit = {
       df.write.mode(SaveMode.Overwrite)
         .format("delta")
         .save(deltaTable.deltaTablePathExtended)

  }

  private def appendDataframeToDeltaTable(deltaTable:DeltaLocalTable, df: DataFrame): Unit ={
      df.write.format("delta")
         .mode(SaveMode.Append).save(deltaTable.deltaTablePathExtended)
  }

  def insertOrAppendToDeltaTable(dataFrame: DataFrame)(deltaTableToSave: DeltaLocalTable)(implicit spark: SparkSession): Unit = {

    val doesTableExist= FileOperations.doesDeltaTableExist(deltaTableToSave.deltaTablePathExtended)
    if(doesTableExist)
      appendDataframeToDeltaTable(deltaTableToSave,dataFrame)
    else
      createDeltaTableIfNotExist(deltaTableToSave,dataFrame)
  }

  // Function to process a file
  def processFile(fullyQualifiedWatFileName:String, processedFolderPath: String)(hiveTable: DeltaLocalTable)(implicit spark: SparkSession): Unit = {
    extractWatFileToHiveTable(fullyQualifiedWatFileName)(hiveTable)
    val fileName = new Path(fullyQualifiedWatFileName).getName
    val hdfsClient = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    keepTrackOfProcessedFile(fileName,processedFolderPath)

  }

  private def extractWatFileToHiveTable(fullyQualifiedWatFileName:String)(hiveTable: DeltaLocalTable)(implicit spark: SparkSession): Unit = {
    val df = extractWATContentsAsDataframe(fullyQualifiedWatFileName)
    insertOrAppendToDeltaTable(df)(hiveTable)
  }

/*
 *   @param:rawWatFilesPath - This is where the RAW wat files will be located
 *   @param:processedFilePath - This is where the list of processed files will be located
 *   @param:explodedFileLocation - This is where the exploded wat files will be saved
 */
  def processRawWatFiles(rawWatFilesPath:String,processedFilePath:String)(explodedFileLocation: DeltaLocalTable)(implicit spark:SparkSession) : Set[String] ={

   var processedFiles =  getProcessedFileNames(processedFilePath)
   val watFilePaths =  listHdfsFiles(rawWatFilesPath)

    def extractFileName(hdfsPath: String): String = {
      val pathElements = hdfsPath.split("/")
      val fullFileName = pathElements.last

      // If you are sure that the format always remains the same, you can use substring to extract the specific part
      fullFileName
    }

    watFilePaths.foreach { filePath =>
      if (!processedFiles.contains(extractFileName(filePath))) {
        // Process the file
        processFile(filePath,processedFilePath)(explodedFileLocation)
        // Add the processed file to the set
        processedFiles += filePath
      }
    }
    processedFiles
  }

 def  extractJpgJpegFromAllUrls(explodedRawWatFilesDeltaTable:DeltaLocalTable, jpgUrlDeltaTable:DeltaLocalTable)(implicit spark:SparkSession): Unit = {
   //val dfRawUnfilteredUrls= spark.read.format("delta").table(explodedRawWatFilesDeltaTable.deltaTableName)
   val dfRawUnfilteredUrls= spark.read.format("delta").load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

   val jpgUrlsDF = dfRawUnfilteredUrls
                            .filter(dfRawUnfilteredUrls("url").isNotNull)
                            .where("lower(url) like 'http%.jpg'").repartition(50)

   val jpegUrlsDF = dfRawUnfilteredUrls
                            .filter(dfRawUnfilteredUrls("url").isNotNull)
                            .where("lower(url) like 'http%.jpeg'").repartition(50)


   insertOrAppendToDeltaTable(filterJpnDomainFiles(jpgUrlsDF))(jpgUrlDeltaTable)
   insertOrAppendToDeltaTable(filterJpnDomainFiles(jpegUrlsDF))(jpgUrlDeltaTable)

 }

def extractPngFromAllUrls(explodedRawWatFilesDeltaTable: DeltaLocalTable, pngUrlDeltaTable: DeltaLocalTable)(implicit spark: SparkSession): Unit = {
    val dfRawUnfilteredUrls= spark.read.format("delta").load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)
    val pngUrlsDF = dfRawUnfilteredUrls
      .filter(dfRawUnfilteredUrls("url").isNotNull)
      .where("lower(url) like 'http%.png'")

    insertOrAppendToDeltaTable(filterJpnDomainFiles(pngUrlsDF))(pngUrlDeltaTable)

}

 def  deduplicateUrls(urlDeltaTable: DeltaLocalTable, distinctUrlDeltaTable: DeltaLocalTable)(implicit spark: SparkSession) : Unit = {

   val dfUrls= spark.read.format("delta").load(urlDeltaTable.deltaTablePathExtended)
   val dfDeduplicatedUrls= dfUrls.dropDuplicates("url")

   insertOrAppendToDeltaTable(dfDeduplicatedUrls)(distinctUrlDeltaTable)

 }

  def convertFromHdfsToDelta(hdfsPath: String, deltaTable: DeltaLocalTable)(implicit spark: SparkSession): Unit = {
    val csvDF = spark.read
      .option("header", "true") // use "true" if your file has a header row, else "false"
      .option("inferSchema", "true") // to automatically infer data types; else, define the schema manually
      .csv(hdfsPath)

    csvDF.write.mode("overwrite").format("delta").save(deltaTable.deltaTablePathExtended)
  }

  def convertWarcFilePathsToWatFilePaths(warcFileListTable: DeltaLocalTable, watFileDestination: DeltaLocalTable)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val prefixVal = "https://data.commoncrawl.org/"

    val warcFileListDf = DeltaTable.forPath(spark, warcFileListTable.deltaTablePathExtended).toDF
    val intermediateWatFileNameDf = warcFileListDf
                                    .withColumn("wat_filename_temp", regexp_replace(warcFileListDf("warc_filename"), "\\/warc\\/", "\\/wat\\/"))
                                    .drop("warc_filename")
    val watFileNameDf = intermediateWatFileNameDf
                          .withColumn("wat_filename", regexp_replace(intermediateWatFileNameDf("wat_filename_temp"), "\\.warc", "\\.warc.wat"))
                          .drop("wat_filename_temp")

    watFileNameDf.withColumn("full_wat_path", functions.concat(lit(prefixVal), watFileNameDf.col("wat_filename")))
      .drop("wat_filename")
      .write.format("delta").mode(SaveMode.Overwrite).save(watFileDestination.deltaTablePathExtended)


    DeltaTable.forPath(spark, watFileDestination.deltaTablePathExtended).toDF
  }

  def filterJpnDomainFiles(dfUrls: DataFrame):DataFrame = {
   dfUrls
      .where("lower(url) like '%.jp/%'")
  }

  def filterKrDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.kr/%'")
  }


}
