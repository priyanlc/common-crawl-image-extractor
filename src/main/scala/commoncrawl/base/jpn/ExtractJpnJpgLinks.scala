package commoncrawl.base.jpn

//import commoncrawl.base.Configuration.{createSparkSession, explodedJpnRawWatFilesHiveTable01, jpnJpgLinks01HiveTable}
import commoncrawl.base.Configuration.{createSparkSession, createSparkSessionWithDelta}
import commoncrawl.base.ExtractWatContents.{extractJpgJpegFromAllUrls, filterJpnDomainFiles}
import commoncrawl.base.jpn.JpnConfiguration.explodedJpnRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractJpnJpgLinks {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpgLinks")

    val checkpointJpnHttpJpgDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/write_jpn_http"

    val jpHttpLinks = "/user/hive/warehouse/curated.db/jp_http_links"
    val jpnJpgLinks = "/user/hive/warehouse/curated.db/jpn_jpg_links"

    val jpnHttpLinksDF = spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 4)
    .load(jpHttpLinks)

    val jpgUrlsDF = jpnHttpLinksDF
      .filter(jpnHttpLinksDF("url").isNotNull)
      .where("lower(url) like 'http%.jpg'")

    val query1 = jpgUrlsDF.writeStream
                  .format("delta")
                  .outputMode("append")
                  .option("checkpointLocation", checkpointJpnHttpJpgDestination)
                  .start(jpnJpgLinks)

    query1.awaitTermination()

  }

}
