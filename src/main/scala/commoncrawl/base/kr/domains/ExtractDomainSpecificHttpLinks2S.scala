package commoncrawl.base.kr.domains

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import commoncrawl.base.kr.domains.KrDomainsConfiguration.{checkpointLocationKakakoHttpDestination, checkpointLocationMusinsaHttpDestination, filterKakaoDomainFiles, filterMunisaDomainFiles, filterNaverDomainFiles}
import org.apache.spark.sql.SparkSession

object ExtractDomainSpecificHttpLinks2S {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractKrDomainLinks2")

    val krHttpLinks = "/user/hive/warehouse/curated.db/kr_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val kakaoLinksDf = filterMunisaDomainFiles(rawHttpLinksDF).selectExpr("url")

    val query1 = kakaoLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation",checkpointLocationMusinsaHttpDestination)
      .start(krHttpLinks)

    query1.awaitTermination()

  }

}
