package commoncrawl.base.kr.domains

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import commoncrawl.base.kr.domains.KrDomainsConfiguration.{checkpointLocationCoupangHttpDestination, checkpointLocationNateHttpDestination, filterCoupangDomainFiles, filterNateDomainFiles}
import org.apache.spark.sql.SparkSession

object ExtractDomainSpecificHttpLinks4S {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractKrDomainLinks4s")

    val krHttpLinks = "/user/hive/warehouse/curated.db/kr_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val nateLinksDf = filterNateDomainFiles(rawHttpLinksDF).selectExpr("url")

    val query1 = nateLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationNateHttpDestination)
      .start(krHttpLinks)

    query1.awaitTermination()

  }

}
