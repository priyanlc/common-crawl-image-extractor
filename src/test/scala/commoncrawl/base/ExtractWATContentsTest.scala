package commoncrawl.base

import com.holdenkarau.spark.testing.Prettify.maxNumberOfShownValues
import com.holdenkarau.spark.testing.SharedSparkContext
import commoncrawl.base.ExtractWatContents.{convertFromHdfsToDelta, convertWarcFilePathsToWatFilePaths, createDeltaTableIfNotExist, deduplicateUrls, extractJpgJpegFromAllUrls, extractPngFromAllUrls, extractWATContentsAsDataframe, insertOrAppendToDeltaTable, processFile, processRawWatFiles}
import commoncrawl.base.FileOperations.{copyToHdfs, downloadFile, exportHiveTableToHdfsCsv, extractFileNameFromURL, getProcessedFileNames, keepTrackOfProcessedFile, listHdfsFiles, moveToArchive, readWatFilesList}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.SparkConf

import java.io.File
import java.nio.file.Paths

class ExtractWATContentsTest extends AnyFunSuite with SharedSparkContext{

  implicit var spark: SparkSession = _
  var fs: FileSystem = _
  var hdfsClient: FileSystem =_

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  def createSparkSessionWithDelta(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.sql.caseSensitive", "true")
      .set("HADOOP_USER_NAME", "hduser") // Use local for testing, replace with your cluster settings if applicable
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    SparkSession.builder.config(conf)
      .getOrCreate()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = createSparkSessionWithDelta("test_app")
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
    spark.sql("USE DATABASE test_db");
    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    hdfsClient = FileSystem.get(new Configuration())
    val sparkConf = spark.sparkContext.getConf
    sparkConf.getAll.foreach(println)
  }

  val rawWatFilePath = "src/test/resources/hdfs_file_system/raw_wat"
  val outputPath = "src/test/resources/output_parquet"
  val jpgExportPath = "src/test/resources/hdfs_file_system/outputs/jpg_export_path"

  val sampleWatFileName = "CC-MAIN-sample.warc.wat.gz"

  val jpgLinksFullyQualifiedDeltaTableName = "test_db.jpg_links"
  val jpgLinksDeltaTablePath = "test_db.db/jpg_links"
  val jpgLinksExtendedDeltaTablePath = "spark-warehouse/test_db.db/jpg_links"

  val jpgDistinctLinksFullyQualifiedHiveTableName = "test_db.jpg_distinct_links"
  val jpgDistinctLinksHiveTablePath = "test_db.db/jpg_distinct_links"
  val jpgDistinctExtendedLinksHiveTablePath = "spark-warehouse/test_db.db/jpg_distinct_links"

  val pngLinksFullyQualifiedDeltaTableName = "test_db.png_links"
  val pngLinksDeltaTablePath = "test_db.db/png_links"
  val pngLinksExtendedDeltaTablePath = "spark-warehouse/test_db.db/png_links"

  val warcLinksFullyQualifiedHiveTableName = "test_db.warc_links"
  val warcLinksHiveTablePath = "test_db.db/warc_links"
  val warcLinksExtendedHiveTablePath = "spark-warehouse/test_db.db/warc_links"

  val watLinksFullyQualifiedHiveTableName = "test_db.wat_links"
  val watLinksHiveTablePath = "test_db.db/wat_links"
  val watLinksExtendedHiveTablePath = "spark-warehouse/test_db.db/wat_links"

  val fullWatFilePath = s"$rawWatFilePath/$sampleWatFileName"
  val processedFilePath = "src/test/resources/hdfs_file_system/outputs/processed_file_path"

  val rawLinksFullyQualifiedDeltaTableName = "test_db.raw_links"
  val rawLinksDeltaTablePath = "test_db.db/raw_links"
  val rawLinksExtendedDeltaTablePath = "spark-warehouse/test_db.db/raw_links"

  val explodedRawWatFilesDeltaTable: DeltaLocalTable = DeltaLocalTable(rawLinksFullyQualifiedDeltaTableName, rawLinksDeltaTablePath,rawLinksExtendedDeltaTablePath)
  val jpgUrlDeltaTable: DeltaLocalTable = DeltaLocalTable(jpgLinksFullyQualifiedDeltaTableName, jpgLinksDeltaTablePath,jpgLinksExtendedDeltaTablePath)

  val jpgDistinctUrlDeltaTable: DeltaLocalTable = DeltaLocalTable(jpgDistinctLinksFullyQualifiedHiveTableName, jpgDistinctLinksHiveTablePath,jpgDistinctExtendedLinksHiveTablePath)

  val pngUrlDeltaTable: DeltaLocalTable = DeltaLocalTable(pngLinksFullyQualifiedDeltaTableName, pngLinksDeltaTablePath,pngLinksExtendedDeltaTablePath)

  val warcUrlHiveTable: DeltaLocalTable = DeltaLocalTable(warcLinksFullyQualifiedHiveTableName, warcLinksHiveTablePath,warcLinksExtendedHiveTablePath)
  val watUrlHiveTable: DeltaLocalTable = DeltaLocalTable(watLinksFullyQualifiedHiveTableName, watLinksHiveTablePath,watLinksExtendedHiveTablePath)

  val warcFileNamesPath = "src/test/resources/hdfs_file_system/warc_filenames/warc_filename.csv" ;
  val watCsvExport = "src/test/resources/hdfs_file_system/outputs/wat_filename.csv" ;

  val watFileNamesPath = "src/test/resources/hdfs_file_system/outputs/wat_file_path" ;

  val pathToWatFileArchiveFolder = "src/test/resources/wat_files_archived"


  val tempDeltaTablePath = "spark-warehouse/test_db.db/temp_table"


  test("extractWATContents should return a DataFrame from WAT file") {
    val result = extractWATContentsAsDataframe(fullWatFilePath)
    println(result.count())
    assert(result.count()>0)
  }

  test("insert delta table with dataframe when the table does not exist") {
    spark.sql(s"DROP TABLE IF EXISTS $rawLinksFullyQualifiedDeltaTableName ")
    val raw_links_path=new Path("spark-warehouse/test_db.db/raw_links")
    fs.delete(raw_links_path,true)

    val dataFrame = extractWATContentsAsDataframe(fullWatFilePath)
    insertOrAppendToDeltaTable(dataFrame)(explodedRawWatFilesDeltaTable)

    assert(spark.read.format("delta").load(explodedRawWatFilesDeltaTable.deltaTablePathExtended).count() > 0)
  }

  test("insert delta table with dataframe when the table exist") {
    val dataFrame = extractWATContentsAsDataframe(fullWatFilePath)
    insertOrAppendToDeltaTable(dataFrame)(explodedRawWatFilesDeltaTable)
    assert(spark.read.format("delta").load("spark-warehouse/"+rawLinksDeltaTablePath).count() > 0)
  }

  test("explode wat file and save to delta table and save the file name in hdfs") {
    processFile(fullWatFilePath, processedFilePath)(explodedRawWatFilesDeltaTable)
    assert(spark.read.format("delta").load(rawLinksExtendedDeltaTablePath).count() > 0)
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
    val processedFileNames = processRawWatFiles(rawWatFilePath,processedFilePath)(explodedRawWatFilesDeltaTable)
    assert(processedFileNames.nonEmpty)
  }

  // need to extract .jpg files from the links
  test("extract .jpg and .jpeg urls from the table with all urls") {
    import io.delta.tables._
    extractJpgJpegFromAllUrls(explodedRawWatFilesDeltaTable,jpgUrlDeltaTable)
    val deltaTable = DeltaTable.forPath(spark, jpgUrlDeltaTable.deltaTablePathExtended)
    assert(deltaTable.toDF.count() >= 0)
  }

  // need to extract .png files from the links
  test("extract .png urls from the table with all urls") {
    import io.delta.tables._
    extractPngFromAllUrls(explodedRawWatFilesDeltaTable, pngUrlDeltaTable)
    val deltaTable = DeltaTable.forPath(spark, jpgUrlDeltaTable.deltaTablePathExtended)
    assert(deltaTable.toDF.count() >= 0)
  }

  test("deduplicate jpg urls and save them to a table") {
    deduplicateUrls(jpgUrlDeltaTable,jpgDistinctUrlDeltaTable);
    val jpgDeltaTable = DeltaTable.forPath(spark, jpgUrlDeltaTable.deltaTablePathExtended)
    val jpgDistinctDeltaTable = DeltaTable.forPath(spark, jpgDistinctUrlDeltaTable.deltaTablePathExtended)
    assert(jpgDeltaTable.toDF.count() >= jpgDistinctDeltaTable.toDF.count())
  }


  test("test exporting a hive table to hdfs file system") {
    exportHiveTableToHdfsCsv(jpgDistinctUrlDeltaTable, jpgExportPath)
    val exportFile = listHdfsFiles(jpgExportPath)
    assert(exportFile.length>0)

  }


  test("test exporting WARC file names file from hdfs to hive table") {
    convertFromHdfsToDelta(warcFileNamesPath, warcUrlHiveTable)
    assert(DeltaTable.forPath(spark, warcUrlHiveTable.deltaTablePathExtended).toDF.count()>0)
  }


  test("convert warc file names to wat file names and save in hdfs file system") {
   val df= convertWarcFilePathsToWatFilePaths(warcUrlHiveTable, watUrlHiveTable)
    assert(DeltaTable.forPath(watUrlHiveTable.deltaTablePathExtended).toDF.count() > 0)
    df.show(false)
  }


  test("export wat file to csv") {
    convertWarcFilePathsToWatFilePaths(warcUrlHiveTable, watUrlHiveTable)
    exportHiveTableToHdfsCsv(watUrlHiveTable,watCsvExport)
    val exportFile = listHdfsFiles(watCsvExport)
    assert(exportFile.length > 0)

  }

  test("doesDeltaTableExist returns true for existing table") {

    if(!FileOperations.doesDeltaTableExist(tempDeltaTablePath)) {
      // Setting up a temporary Delta table
      val df = createSparkDfWithUrlCol(createSparkSessionWithDelta("createSession"))
      df.write.format("delta").save(tempDeltaTablePath)
    }
    // Test if the table exists
    assert(FileOperations.doesDeltaTableExist(tempDeltaTablePath))

    // Cleanup
    spark.read.format("delta").load(tempDeltaTablePath).write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(tempDeltaTablePath)
  }

  test("doesDeltaTableExist returns false for non-existing table") {
    val nonExistentTablePath = "nonExistentDeltaTable"
    assert(!FileOperations.doesDeltaTableExist(nonExistentTablePath))
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
    val data = Seq(1, 2, 3).toDF()
    data
  }


  test("read wat file list ") {
   val watFileList =  readWatFilesList(watCsvExport)
    assert(watFileList.nonEmpty)
  }

  test("Test extract file name from URL") {
    val url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296949642.35/wat/CC-MAIN-20230331113819-20230331143819-00137.warc.wat.gz"
    val watFileName = extractFileNameFromURL(url)
    assert(watFileName=="CC-MAIN-20230331113819-20230331143819-00137.warc.wat.gz")
  }


  test("Keep track of processed files ") {
    val url = "CC-MAIN-20230331113819-20230331143819-00137.warc.wat.gz"
    keepTrackOfProcessedFile(url,pathToWatFileArchiveFolder)

    val watFileArchive = listHdfsFiles(pathToWatFileArchiveFolder)
    assert(watFileArchive.length > 0)

  }


  //keepTrackOfProcessedFile(hdfsClient: FileSystem, fileName: String, processedFolderPath: String)

}

