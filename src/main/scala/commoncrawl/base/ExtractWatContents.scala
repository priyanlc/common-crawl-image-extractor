package commoncrawl.base

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.explode

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

  def insertOrAppendToHiveTable(fullyQualifiedTableName: String, hiveTablePath: String, dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {

    if(spark.catalog.tableExists(fullyQualifiedTableName))
      appendDataframeToHiveTable(fullyQualifiedTableName,dataFrame)
    else
      createHiveTableIfNotExist(fullyQualifiedTableName,hiveTablePath,dataFrame)

  }

  def explodeWatFilesToHive(watFilePath:String,maxNumOfFilesToProcess:Int)(implicit spark: SparkSession) : Unit = {
    // get WatFileListYetToProcess
    // for each files in WAT list call insertOrAppendToHive

  }


  // Function to process a file
  def processFile(fullyQualifiedWatFileName:String, outputFolderPath: String, hiveTableName: String, hiveTablePath: String)(implicit spark: SparkSession): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // Your processing logic here
    extractWatFileToHiveTable(fullyQualifiedWatFileName,hiveTableName,hiveTablePath)

    // Save the processed file name to a different location
    val fileName = new Path(fullyQualifiedWatFileName).getName
    val outputFilePath = new Path(outputFolderPath, s"processed_$fileName")
    fs.create(outputFilePath).close()
  }

  private def extractWatFileToHiveTable(fullyQualifiedWatFileName:String, hiveTableName:String, hiveTablePath:String)(implicit spark: SparkSession): Unit = {
    val df = extractWATContentsAsDataframe(fullyQualifiedWatFileName)
    insertOrAppendToHiveTable(hiveTableName,hiveTablePath, df)
  }

}
