package commoncrawl.base.jpn

//import commoncrawl.base.Configuration.{createSparkSession, explodedJpnRawWatFilesHiveTable01, jpnJpgLinks01HiveTable}
import commoncrawl.base.Configuration.{createSparkSession, createSparkSessionWithDelta}
import commoncrawl.base.ExtractWatContents.{extractJpgJpegFromAllUrls, filterJpnDomainFiles}
import commoncrawl.base.jpn.JpnConfiguration.explodedJpnRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractJpnJpgLinks {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpgLinks")
    //extractJpgJpegFromAllUrls(explodedJpnRawWatFilesHiveTable01,jpnJpgLinks01HiveTable)

    val checkpointLocationHttpSource = "/user/hive/warehouse/curated.db/checkpoint/dir/read_jpn_http"
    val checkpointLocationHttpJpgDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/write_jpn_http"


    val jpnJpgLinks = "/user/hive/warehouse/curated.db/jpn_jpg_links"

    val rawHttpLinksDF = spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)
    .load(explodedJpnRawWatFilesDeltaTable.deltaTablePathExtended)

    val jpgUrlsDF = rawHttpLinksDF
      .filter(rawHttpLinksDF("url").isNotNull)
      .where("lower(url) like 'http%.jpg'")

//    val jpegUrlsDF = rawHttpLinksDF
//      .filter(rawHttpLinksDF("url").isNotNull)
//      .where("lower(url) like 'http%.jpeg'").repartition(50)

    val jpnJpgLinksDf = filterJpnDomainFiles(jpgUrlsDF).selectExpr("url")
   // val jpnJpegLinksDf = filterJpnDomainFiles(jpegUrlsDF).selectExpr("url")

    val query1 =jpnJpgLinksDf.writeStream
                  .format("delta")
                  .outputMode("append")
                  .option("checkpointLocation", checkpointLocationHttpJpgDestination)
                  .start(jpnJpgLinks)


//    val query2 = jpnJpegLinksDf.writeStream
//      .format("delta")
//      .outputMode("append")
//      .option("checkpointLocation", checkpointLocationHttpJpgDestination)
//      .start(jpnJpgLinks)

    query1.awaitTermination()
   // query2.awaitTermination()


  }

}
