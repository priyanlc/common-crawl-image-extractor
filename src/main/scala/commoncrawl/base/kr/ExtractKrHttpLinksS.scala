package commoncrawl.base.kr

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.ExtractWatContents.{filterJpnDomainFiles, filterKrDomainFiles}
import commoncrawl.base.kr.KrConfiguration
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractKrHttpLinksS {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractKrLinks")

    val checkpointLocationKrHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/write_http"
    val krHttpLinks = "/user/hive/warehouse/curated.db/kr_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val krLinksDf = filterKrDomainFiles(rawHttpLinksDF).selectExpr("url")

    val query1 = krLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationKrHttpDestination)
      .start(krHttpLinks)

    query1.awaitTermination()

  }

}
