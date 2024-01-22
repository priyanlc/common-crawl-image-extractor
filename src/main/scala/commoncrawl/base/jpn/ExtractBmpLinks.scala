package commoncrawl.base.jpn

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.ExtractWatContents.filterJpnDomainFiles
import commoncrawl.base.jpn.JpnConfiguration.explodedJpnRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractBmpLinks {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpnBnpLinks")

    val checkpointLocationHttpBmpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/write_bmp_http"

    val jpnJpegLinks = "/user/hive/warehouse/curated.db/jpn_bmp_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedJpnRawWatFilesDeltaTable.deltaTablePathExtended)

    val bmpUrlsDF = rawHttpLinksDF
      .filter(rawHttpLinksDF("url").isNotNull)
      .where("lower(url) like 'http%.bmp'")


    val jpnBmpLinksDf = filterJpnDomainFiles(bmpUrlsDF).selectExpr("url")

    val query1 = jpnBmpLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationHttpBmpDestination)
      .start(jpnJpegLinks)


    query1.awaitTermination()



  }

}
