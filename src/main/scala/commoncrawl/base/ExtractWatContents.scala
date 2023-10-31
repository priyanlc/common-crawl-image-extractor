package commoncrawl.base

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.explode

object ExtractWatContents {

  import org.apache.spark.sql.{DataFrame, SparkSession}

  def extractWATContentsAsDataframe(folderLocation: String, watFileName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val fullPath = s"$folderLocation/$watFileName"
    val watFilesJsonRawDF = spark.read.json(fullPath)
    val payloadDF = watFilesJsonRawDF.select("Envelope")
    val htmlResponseMetadataDf = payloadDF.select("Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata")
    val linksDf = htmlResponseMetadataDf.select("HTML-Metadata.Links")
    val explodedLinksDf = linksDf.select(explode($"Links")).as("exploded_links").select("exploded_links.*")

    explodedLinksDf

  }

  def createHiveTableIfNotExist(fullyQualifiedTableName: String, path: String, df: DataFrame): Unit = {
       df.write.mode(SaveMode.Overwrite).option("path", path)saveAsTable(fullyQualifiedTableName)
  }

  private def appendDataframeToHiveTable(fullyQualifiedTableName: String, df: DataFrame): Unit ={
    df.write.mode(SaveMode.Append)saveAsTable(fullyQualifiedTableName)
  }

  def insertOrAppendToHiveTable(fullyQualifiedTableName: String, path: String, dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {

    if(spark.catalog.tableExists(fullyQualifiedTableName))
      appendDataframeToHiveTable(fullyQualifiedTableName,dataFrame)
    else
      createHiveTableIfNotExist(fullyQualifiedTableName,path,dataFrame)

  }

}
