package commoncrawl.base.kr.domains

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import commoncrawl.base.kr.domains.KrDomainsConfiguration.{checkpointLocationChosunHttpDestination, filterChosunDomainFiles}
import org.apache.spark.sql.SparkSession

object ExtractDomainSpecificHttpLinks5S {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractKrDomainLinks")

    val krHttpLinks = "/user/hive/warehouse/curated.db/kr_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val coupangLinksDf = filterChosunDomainFiles(rawHttpLinksDF).selectExpr("url")

    val query1 = coupangLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationChosunHttpDestination)
      .start(krHttpLinks)

    query1.awaitTermination()

  }


}
