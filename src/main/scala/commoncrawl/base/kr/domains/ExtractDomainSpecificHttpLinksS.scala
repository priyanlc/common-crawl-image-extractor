package commoncrawl.base.kr.domains

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import commoncrawl.base.kr.domains.KrDomainsConfiguration.{checkpointLocationCoupangHttpDestination, checkpointLocationJoinsHttpDestination, checkpointLocationKakakoHttpDestination, filterCoupangDomainFiles, filterJoinsDomainFiles, filterKakaoDomainFiles, filterNaverDomainFiles}
import org.apache.spark.sql.SparkSession

object ExtractDomainSpecificHttpLinksS {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractKrDomainLinks")

    val krHttpLinks = "/user/hive/warehouse/curated.db/kr_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val coupangLinksDf = filterJoinsDomainFiles(rawHttpLinksDF).selectExpr("url")

    val query1 = coupangLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationJoinsHttpDestination)
      .start(krHttpLinks)


    query1.awaitTermination()

  }

}
