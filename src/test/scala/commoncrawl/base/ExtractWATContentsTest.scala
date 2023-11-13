package commoncrawl.base

import com.holdenkarau.spark.testing.Prettify.maxNumberOfShownValues
import com.holdenkarau.spark.testing.SharedSparkContext
import commoncrawl.base.ExtractWatContents.{createHiveTableIfNotExist, deduplicateUrls, exportHiveTableToHdfsCsv, extractJpgJpegFromAllUrls, extractPngFromAllUrls, extractWATContentsAsDataframe, getProcessedFileNames, insertOrAppendToHiveTable, listHdfsFiles, processFile, processRawWatFiles}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark

class ExtractWATContentsTest extends AnyFunSuite with SharedSparkContext{

  implicit var spark: SparkSession = _
  var fs: FileSystem = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().enableHiveSupport().config(sc.getConf).getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db LOCATION 'tmp/test_db1.db' ");
    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  }
  
  val rawWatFilePath = "src/test/resources/hdfs_file_system/raw_wat"
  val outputPath = "src/test/resources/output_parquet"
  val jpgExportPath = "src/test/resources/hdfs_file_system/outputs/jpg_export_path"

  val sampleWatFileName = "CC-MAIN-sample.warc.wat.gz"

  val rawLinksFullyQualifiedHiveTableName = "test_db.raw_links"
  val rawLinksHiveTablePath = "spark-warehouse/tmp/test_db1.db/raw_links"

  val jpgLinksFullyQualifiedHiveTableName = "test_db.jpg_links"
  val jpgLinksHiveTablePath = "spark-warehouse/tmp/test_db1.db/jpg_links"

  val jpgDistinctLinksFullyQualifiedHiveTableName = "test_db.jpg_distinct_links"
  val jpgDistinctLinksHiveTablePath = "spark-warehouse/tmp/test_db1.db/jpg_distinct_links"

  val pngLinksFullyQualifiedHiveTableName = "test_db.png_links"
  val pngLinksHiveTablePath = "spark-warehouse/tmp/test_db1.db/png_links"

  val fullWatFilePath = s"$rawWatFilePath/$sampleWatFileName"
  val processedFilePath = "src/test/resources/hdfs_file_system/outputs/processed_file_path"

  val explodedRawWatFilesHiveTable: HiveTable = HiveTable(rawLinksFullyQualifiedHiveTableName, rawLinksHiveTablePath)
  val jpgUrlHiveTable: HiveTable = HiveTable(jpgLinksFullyQualifiedHiveTableName, jpgLinksHiveTablePath)
  val jpgDistinctUrlHiveTable: HiveTable = HiveTable(jpgDistinctLinksFullyQualifiedHiveTableName, jpgDistinctLinksHiveTablePath)
  val pngUrlHiveTable: HiveTable = HiveTable(pngLinksFullyQualifiedHiveTableName, pngLinksHiveTablePath)



  test("extractWATContents should return a DataFrame from WAT file") {

    val result = extractWATContentsAsDataframe(fullWatFilePath)
    println(result.count())
    assert(result.count()>0)
  }

  test("insert hive table with dataframe when the table does not exist") {
    spark.sql(s"DROP TABLE IF EXISTS $rawLinksFullyQualifiedHiveTableName ")
    val raw_links_path=new Path("spark-warehouse/tmp/test_db1.db/raw_links")
    fs.delete(raw_links_path,true)

    val dataFrame = extractWATContentsAsDataframe(fullWatFilePath)
    insertOrAppendToHiveTable(dataFrame)(explodedRawWatFilesHiveTable)
    spark.sql(s"refresh table $rawLinksFullyQualifiedHiveTableName")
    assert(spark.read.table(rawLinksFullyQualifiedHiveTableName).count() > 0)
  }

  test("insert hive table with dataframe when the table exist") {

    val dataFrame = extractWATContentsAsDataframe(fullWatFilePath)
    insertOrAppendToHiveTable(dataFrame)(explodedRawWatFilesHiveTable)
    spark.sql(s"refresh table $rawLinksFullyQualifiedHiveTableName")
    assert(spark.read.table(rawLinksFullyQualifiedHiveTableName).count() > 0)
  }

  test("explode wat file and save to hive table and save the file name in hdfs") {

  processFile(fullWatFilePath, processedFilePath)(explodedRawWatFilesHiveTable)
    spark.sql(s"refresh table $rawLinksFullyQualifiedHiveTableName")
    assert(spark.read.table(rawLinksFullyQualifiedHiveTableName).count() > 0)
  }

  test("list the number of files in the hdfs folder location") {
    val watFiles = listHdfsFiles(rawWatFilePath)
    assert(watFiles.length > 0)
  }

  test("Load the set of processed files from a persistent location") {
    val processedFileNames = getProcessedFileNames(processedFilePath)
    assert(processedFileNames.nonEmpty)
  }

  test("Process files one by one, excluding those already processed") {
    val processedFileNames = processRawWatFiles(rawWatFilePath,processedFilePath)(explodedRawWatFilesHiveTable)
    assert(processedFileNames.nonEmpty)
  }

  // need to extract .jpg files from the links
  test("extract .jpg and .jpeg urls from the table with all urls") {
    extractJpgJpegFromAllUrls(explodedRawWatFilesHiveTable,jpgUrlHiveTable)
    assert(spark.read.table(jpgUrlHiveTable.hiveTableName).count() > 0)
  }

  // need to extract .png files from the links
  test("extract .png urls from the table with all urls") {
    extractPngFromAllUrls(explodedRawWatFilesHiveTable, pngUrlHiveTable)
    assert(spark.read.table(pngUrlHiveTable.hiveTableName).count() > 0)
  }

  test("deduplicate jpg urls and save them to a table") {
    deduplicateUrls(jpgUrlHiveTable,jpgDistinctUrlHiveTable);
    assert(spark.read.table(jpgUrlHiveTable.hiveTableName).count() > spark.read.table(jpgDistinctUrlHiveTable.hiveTableName).count())
  }


  test("test exporting a hive table to hdfs file system") {
    exportHiveTableToHdfsCsv(jpgDistinctUrlHiveTable, jpgExportPath)
    val exportFile = listHdfsFiles(jpgExportPath)
    assert(exportFile.length>0)

  }

  override def afterAll(): Unit = {
    val jpg_export_path=new Path(jpgExportPath)
    fs.delete(jpg_export_path,true)

    val output_processed_path = new Path(processedFilePath)
    fs.delete(output_processed_path, true)

    fs.close()
  }


  def createSparkDfWithUrlCol(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    // Sample data
    val data = Seq(
      ("path1", "target1", "text1", "title1", "http://example.com/page1"),
      ("path2", "target2", "text2", "title2", "http://example.com/page2"),
      ("path3", "target3", "text3", "title3", "http://example.com/page1"), // Duplicate URL
      ("path4", "target4", "text4", "title4", "http://example.com/page3")
    )

    // Create a DataFrame with the specified schema
    val df = data.toDF("path", "target", "text", "title", "url")
    df
  }

}

