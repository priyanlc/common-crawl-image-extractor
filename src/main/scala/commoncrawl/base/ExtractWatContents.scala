package commoncrawl.base

import org.apache.spark.sql.functions.explode

object ExtractWatContents {

  import org.apache.spark.sql.{DataFrame, SparkSession}

  def extractWATContents(folderLocation: String, watFileName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val fullPath = s"$folderLocation/$watFileName"
    val watFilesJsonRawDF = spark.read.json(fullPath)
    val payloadDF = watFilesJsonRawDF.select("Envelope")
    val htmlResponseMetadataDf = payloadDF.select("Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata")
    val linksDf = htmlResponseMetadataDf.select("HTML-Metadata.Links")
    val explodedLinksDf = linksDf.select(explode($"Links")).as("exploded_links").select("exploded_links.*")

    explodedLinksDf

  }

}
