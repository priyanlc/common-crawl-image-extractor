package commoncrawl.base.jpn

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import commoncrawl.base.ExtractWatContents.{filterJpnDomainFiles, filterKrDomainFiles}
import commoncrawl.base.kr.KrConfiguration.explodedRawWatFilesDeltaTable
import org.apache.spark.sql.SparkSession

object ExtractJpHttpLinksS {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpLinks")

    val checkpointLocationJpHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/jp/write_http"
    val jpHttpLinks = "/user/hive/warehouse/curated.db/jp_http_links"

    val rawHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(explodedRawWatFilesDeltaTable.deltaTablePathExtended)

    val jpLinksDf = filterJpnDomainFiles(rawHttpLinksDF).selectExpr("url")

    val query1 = jpLinksDf.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocationJpHttpDestination)
      .start(jpHttpLinks)

    query1.awaitTermination()

  }

}
