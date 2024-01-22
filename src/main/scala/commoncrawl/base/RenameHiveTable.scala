package commoncrawl.base

import commoncrawl.base.Configuration.createSparkSession
import commoncrawl.base.ExtractWatContents.insertOrAppendToDeltaTable
import org.apache.spark.sql.SparkSession

object RenameHiveTable {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession("ConvertWatToWarc")

   // val dfRawUnfilteredUrls= spark.read.parquet( "hdfs://master:9000/user/hive/warehouse/raw.db/wat_http_links")

    val df=  spark.read.table("curated.jpn_jpg_http_links_01")

    println("##### Count " + df.count())
    df.printSchema()
//212,571,552
   // insertOrAppendToHiveTable(dfRawUnfilteredUrls)(explodedJpnRawWatFilesHiveTable)
//6,119,648

  }

}
