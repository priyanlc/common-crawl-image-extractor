package commoncrawl.base.jpn

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.ExtractWatContents.filterJpnDomainFiles
import commoncrawl.base.jpn.JpnConfiguration.explodedJpnRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractJpnPngLinks {


  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpnPngLinks")

    val checkpointLocationHttpPngDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/write_png_http"

    val jpnPngLinks = "/user/hive/warehouse/curated.db/jpn_png_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedJpnRawWatFilesDeltaTable.deltaTablePathExtended)

    val pngUrlsDF = rawHttpLinksDF
      .filter(rawHttpLinksDF("url").isNotNull)
      .where("lower(url) like 'http%.png'")


    val jpnPngLinksDf = filterJpnDomainFiles(pngUrlsDF).selectExpr("url")

    val query1 = jpnPngLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationHttpPngDestination)
      .start(jpnPngLinks)


    query1.awaitTermination()



  }

}
