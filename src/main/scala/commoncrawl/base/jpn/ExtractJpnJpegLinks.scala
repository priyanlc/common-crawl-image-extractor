package commoncrawl.base.jpn

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.ExtractWatContents.filterJpnDomainFiles
import commoncrawl.base.jpn.JpnConfiguration.explodedJpnRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractJpnJpegLinks {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpnJpegLinks")

    val checkpointLocationHttpJpegDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/write_jpeg_http"

    val jpnJpegLinks = "/user/hive/warehouse/curated.db/jpn_jpeg_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedJpnRawWatFilesDeltaTable.deltaTablePathExtended)

    val jpegUrlsDF = rawHttpLinksDF
      .filter(rawHttpLinksDF("url").isNotNull)
      .where("lower(url) like 'http%.jpeg'")


    val jpnJpegLinksDf = filterJpnDomainFiles(jpegUrlsDF).selectExpr("url")

    val query1 = jpnJpegLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationHttpJpegDestination)
      .start(jpnJpegLinks)


    query1.awaitTermination()



  }

}
