package commoncrawl.base.kr

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.ExtractWatContents.filterJpnDomainFiles
import commoncrawl.base.kr.KrConfiguration
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractKrLinksS {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractKrLinks")

    val checkpointLocationHttpJpegDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/write_http"
    val krJpegLinks = "/user/hive/warehouse/curated.db/kr_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val jpegUrlsDF = rawHttpLinksDF
      .filter(rawHttpLinksDF("url").isNotNull)
      .where("lower(url) like 'http%.jpeg'")

    val jpnJpegLinksDf = filterJpnDomainFiles(jpegUrlsDF).selectExpr("url")

    val query1 = jpnJpegLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationHttpJpegDestination)
      .start(krJpegLinks)

    query1.awaitTermination()

  }

}
