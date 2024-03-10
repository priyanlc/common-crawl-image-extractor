package commoncrawl.base.kr.domains

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import commoncrawl.base.kr.domains.KrDomainsConfiguration.{checkpointLocationDaumHttpDestination, checkpointLocationNaverHttpDestination, filterDaumDomainFiles, filterKakaoDomainFiles}
import org.apache.spark.sql.SparkSession

object ExtractDomainSpecificHttpLinks3S {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractKrDomainLinks3")

    val krHttpLinks = "/user/hive/warehouse/curated.db/kr_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val daumLinksDf = filterDaumDomainFiles(rawHttpLinksDF).selectExpr("url")

    val query1 = daumLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationDaumHttpDestination)
      .start(krHttpLinks)

    query1.awaitTermination()

  }

}
