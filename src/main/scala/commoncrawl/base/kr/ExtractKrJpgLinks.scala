package commoncrawl.base.kr

import commoncrawl.base.Configuration.createSparkSessionWithDelta
import org.apache.spark.sql.SparkSession

object ExtractKrJpgLinks {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSessionWithDelta("ExtractJpgLinks")

    val checkpointJrHttpJpgDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/write_kr_jpg"

    val krHttpLinks = "/user/hive/warehouse/curated.db/kr_http_links"
    val krJpgLinks = "/user/hive/warehouse/curated.db/kr_jpg_links"

    val krHttpLinksDF = spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 4)
      .load(krHttpLinks)

    val krJpgUrlsDF = krHttpLinksDF
      .filter(krHttpLinksDF("url").isNotNull)
      .where("lower(url) like 'http%.jpg'")

    val query1 = krJpgUrlsDF.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointJrHttpJpgDestination)
      .start(krJpgLinks)

    query1.awaitTermination()

  }

}
