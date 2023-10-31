package commoncrawl.base

import com.holdenkarau.spark.testing.SharedSparkContext
import commoncrawl.base.ExtractWatContents.{createHiveTableIfNotExist, extractWATContentsAsDataframe, insertOrAppendToHiveTable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class ExtractWATContentsTest extends AnyFunSuite with SharedSparkContext{

  implicit var spark: SparkSession = _
  var fs: FileSystem = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db LOCATION 'tmp/test_db1.db' ");
    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  }
  
  val rawWatFilePath = "src/test/resources/raw_wat"
  val outputPath = "src/test/resources/output_parquet"
  val sampleWatFileName = "CC-MAIN-sample.warc.wat.gz"
  val fullyQualifiedTableName = "test_db.raw_links"
  val tablePath = "spark-warehouse/tmp/test_db1.db/raw_links"

  test("extractWATContents should return a DataFrame from WAT file") {
    val result = extractWATContentsAsDataframe(rawWatFilePath,sampleWatFileName)
    println(result.count())
    assert(result.count()>0)
  }


  test("insert hive table with dataframe when the table does not exist") {
    spark.sql(s"DROP TABLE IF EXISTS $fullyQualifiedTableName ")
    val raw_links_path=new Path("spark-warehouse/tmp/test_db1.db/raw_links")
    fs.delete(raw_links_path,true)

    val dataFrame = extractWATContentsAsDataframe(rawWatFilePath, sampleWatFileName)
    insertOrAppendToHiveTable(fullyQualifiedTableName,tablePath,dataFrame)
    spark.sql(s"refresh table $fullyQualifiedTableName")

    assert(spark.read.table(fullyQualifiedTableName).count() > 0)
  }

  test("insert hive table with dataframe when the table exist") {

    val dataFrame = extractWATContentsAsDataframe(rawWatFilePath, sampleWatFileName)
    insertOrAppendToHiveTable(fullyQualifiedTableName,tablePath, dataFrame)
    spark.sql(s"refresh table $fullyQualifiedTableName")

    assert(spark.read.table(fullyQualifiedTableName).count() > 0)
  }

  //https://medium.com/expedia-group-tech/unit-testing-apache-spark-applications-using-hive-tables-ec653c6f25be

  // next test, a folder will contain a list of files.  I want to read this full list. For each file in the list I want to call this above function. 
  //
}
