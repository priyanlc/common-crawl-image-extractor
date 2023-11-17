package commoncrawl.base

import com.holdenkarau.spark.testing.Prettify.maxNumberOfShownValues
import com.holdenkarau.spark.testing.SharedSparkContext
import commoncrawl.base.ExtractWatContents.{convertFromHdfsToHive, convertWarcFilePathsToWatFilePaths, createHiveTableIfNotExist, deduplicateUrls, extractJpgJpegFromAllUrls, extractPngFromAllUrls, extractWATContentsAsDataframe, insertOrAppendToHiveTable, processFile, processRawWatFiles}
import commoncrawl.base.FileOperations.{copyToHdfs, downloadFile, exportHiveTableToHdfsCsv, extractFileNameFromURL, getProcessedFileNames, keepTrackOfProcessedFile, listHdfsFiles, moveToArchive, readWatFilesList}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark

import java.nio.file.Paths

class ExtractWATContentsTest extends AnyFunSuite with SharedSparkContext{

  implicit var spark: SparkSession = _
  var fs: FileSystem = _
  var hdfsClient: FileSystem =_
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().enableHiveSupport().config(sc.getConf).getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db LOCATION 'tmp/test_db1.db' ");
    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    hdfsClient = FileSystem.get(new Configuration())

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

  val warcLinksFullyQualifiedHiveTableName = "test_db.warc_links"
  val warcLinksHiveTablePath = "spark-warehouse/tmp/test_db1.db/warc_links"

  val watLinksFullyQualifiedHiveTableName = "test_db.wat_links"
  val watLinksHiveTablePath = "spark-warehouse/tmp/test_db1.db/wat_links"

  val fullWatFilePath = s"$rawWatFilePath/$sampleWatFileName"
  val processedFilePath = "src/test/resources/hdfs_file_system/outputs/processed_file_path"

  val explodedRawWatFilesHiveTable: HiveTable = HiveTable(rawLinksFullyQualifiedHiveTableName, rawLinksHiveTablePath)
  val jpgUrlHiveTable: HiveTable = HiveTable(jpgLinksFullyQualifiedHiveTableName, jpgLinksHiveTablePath)
  val jpgDistinctUrlHiveTable: HiveTable = HiveTable(jpgDistinctLinksFullyQualifiedHiveTableName, jpgDistinctLinksHiveTablePath)
  val pngUrlHiveTable: HiveTable = HiveTable(pngLinksFullyQualifiedHiveTableName, pngLinksHiveTablePath)

  val warcUrlHiveTable: HiveTable = HiveTable(warcLinksFullyQualifiedHiveTableName, warcLinksHiveTablePath)
  val watUrlHiveTable: HiveTable = HiveTable(watLinksFullyQualifiedHiveTableName, watLinksHiveTablePath)

  val warcFileNamesPath = "src/test/resources/hdfs_file_system/warc_filenames/warc_filename.csv" ;
  val watCsvExport = "src/test/resources/hdfs_file_system/outputs/wat_filename.csv" ;

  val watFileNamesPath = "src/test/resources/hdfs_file_system/outputs/wat_file_path" ;

  val pathToWatFileArchiveFolder = "src/test/resources/wat_files_archived"







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


  test("test exporting WARC file names file from hdfs to hive table") {
    convertFromHdfsToHive(warcFileNamesPath, warcUrlHiveTable)
    assert(spark.read.table(warcUrlHiveTable.hiveTableName).count() > 0)
  }


  test("convert warc file names to wat file names and save in hdfs file system") {
   val df= convertWarcFilePathsToWatFilePaths(warcUrlHiveTable, watUrlHiveTable)
    assert(spark.read.table(watUrlHiveTable.hiveTableName).count() > 0)
    df.show(false)
  }


  test("export wat file to vcv") {
    convertWarcFilePathsToWatFilePaths(warcUrlHiveTable, watUrlHiveTable)
    exportHiveTableToHdfsCsv(watUrlHiveTable,watCsvExport)
    val exportFile = listHdfsFiles(watCsvExport)
    assert(exportFile.length > 0)

  }

  //exportHiveTableToHdfsCsv

  override def afterAll(): Unit = {
    val jpg_export_path=new Path(jpgExportPath)
    fs.delete(jpg_export_path,true)

    val output_processed_path = new Path(processedFilePath)
    fs.delete(output_processed_path, true)


    fs.close()
  }

  /*
    I want a process to download from a list of wat files and upload automatically to hdfs.
    1. Need to have a list of WAT files will full URLs
    2. Need a bash script or a python script to use this file and download files sequentially or parallel.
    3. When the file is downloaded ( the script should detect when the file is fully downloaded ) ( to a specific folder ) it should be uploaded to hdfs.
    4. The original files should be moved to achieve folder.

    I need a Scala script to download WAT files.
    The file containing the WAT files list will be saved in a HDFS folder.
    After a WAT file is downloaded to a staging area it should be copied to a HDFS folder location with hdfs commands.
    The original file should be moved to a archived folder.
    Another file should be update with the WAT file name to keep the state of downloaded file.
    I should be able to specify as an command line argument the number of WAT files I need to download from the file containing the WAT files list.

   */

  /*

  Hey Chat,
  Consider the following code extract and descriptions. I want to combine them to a main method.

  Description of the main method - I want load a file which will contain wat files links.
  I want to specify how many files need to be downloaded in one go.
  These wat files need to downloaded sequentially.

  For each downloaded file I need to send it to a HDFS location.
  I then want to archive this file.
  The file already downloaded should be saved in a separate location. This list will be used to
  compare against the already downloaded list. Already downloaded files should not be downloaded again.






  */



//  def createSparkDfWithUrlCol(implicit spark: SparkSession): DataFrame = {
//    import spark.implicits._
//    // Sample data
//    val data = Seq(
//      ("path1", "target1", "text1", "title1", "http://example.com/page1"),
//      ("path2", "target2", "text2", "title2", "http://example.com/page2"),
//      ("path3", "target3", "text3", "title3", "http://example.com/page1"), // Duplicate URL
//      ("path4", "target4", "text4", "title4", "http://example.com/page3")
//    )
//
//    // Create a DataFrame with the specified schema
//    val df = data.toDF("path", "target", "text", "title", "url")
//    df
//  }


//  test("move the wat file to archived folder location") {
//    // Arrange
//    val filePath = "src/test/resources/hdfs_file_system/wat_staging_area/CC-MAIN-sample.warc.wat.gz"
//    val archivePath = "src/test/resources/hdfs_file_system/outputs/wat_files_archived/"
//
//    // Act
//    moveToArchive(filePath, archivePath)
//
//    val watFiles = listHdfsFiles(archivePath)
//    // Assert
//    assert(watFiles.length > 0)
//
//  }


//  test("move file to hdfs from local") {
//    // Arrange
//    val filePath = "src/test/resources/wat_staging_area/CC-MAIN-sample.warc.wat.gz"
//    val hdfsPath = "src/test/resources/hdfs_file_system/outputs/raw_wat/"
//
//    // Act
//    copyToHdfs(filePath, hdfsPath, hdfsClient = hdfsClient)
//
//    val watFiles = listHdfsFiles(hdfsPath)
//    // Assert
//    assert(watFiles.length > 0)
//
//  }


  //downloadFile

//  test("download wat file to staging folder") {
//    // Arrange
//    val filePath = "src/test/resources/wat_staging_area"
//    val watUrl = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296949642.35/wat/CC-MAIN-20230331113819-20230331143819-00137.warc.wat.gz"
//
//    // Act
//    downloadFile(watUrl, filePath)
//
//    val watFiles = listHdfsFiles(filePath)
//    // Assert
//    assert(watFiles.length > 0)
//
//  }


  test("read wat file list ") {
   val watFileList =  readWatFilesList(hdfsClient,watCsvExport)
    assert(watFileList.nonEmpty)
  }

  test("Test extract file name from URL") {
    val url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296949642.35/wat/CC-MAIN-20230331113819-20230331143819-00137.warc.wat.gz"
    val watFileName = extractFileNameFromURL(url)
    assert(watFileName=="CC-MAIN-20230331113819-20230331143819-00137.warc.wat.gz")
  }


  test("Keep track of processed files ") {
    val url = "CC-MAIN-20230331113819-20230331143819-00137.warc.wat.gz"
    keepTrackOfProcessedFile(hdfsClient,url,pathToWatFileArchiveFolder)

    val watFileArchive = listHdfsFiles(pathToWatFileArchiveFolder)
    assert(watFileArchive.length > 0)

  }


  //keepTrackOfProcessedFile(hdfsClient: FileSystem, fileName: String, processedFolderPath: String)

}

