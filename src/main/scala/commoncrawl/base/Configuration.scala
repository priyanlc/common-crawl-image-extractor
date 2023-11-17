package commoncrawl.base

import org.apache.hadoop.fs.FileSystem

object Configuration {

  val watFileUrlListPath = "src/test/resources/hdfs_file_system/outputs/wat_filename.csv"

  val pathToWatFileStagingArea = "src/test/resources/hdfs_file_system/outputs/wat_staging_area/"

  val pathToWatFileHdfsFolder = "src/test/resources/hdfs_file_system/outputs/raw_wat"

  val downloadedWatFileState = "src/test/resources/hdfs_file_system/outputs/downloaded_wat_file_list"


}
