package commoncrawl.base.jpn

import commoncrawl.base.Configuration.{createSparkSession, createSparkSessionWithDelta, jpgLinks01HiveTablePath, jpnJpgLinks01FullyQualifiedHiveTableName}
import commoncrawl.base.DeltaLocalTable
import org.apache.spark.sql.SparkSession

object TestDelta {

  def main(args: Array[String]): Unit = {

//    val jpnJpgLinks01FullyQualifiedHiveTableName = "curated.jpn_jpg_http_links_01"
//    val jpgLinksHiveTablePath = "hdfs://master:9000/user/hive/warehouse/curated.db/jpn_jpg_http_links"
//    val jpnJpgLinks01HiveTable: HiveTable = HiveTable(jpnJpgLinks01FullyQualifiedHiveTableName, jpgLinks01HiveTablePath)

    implicit val spark: SparkSession = createSparkSessionWithDelta("SparkSessionWithDelta")
    val data = spark.range(0, 5)
    data.write.format("delta").save("/tmp/delta-table")
  }

}
