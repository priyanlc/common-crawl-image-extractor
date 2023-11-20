package commoncrawl.base

import org.apache.hadoop.fs.FileSystem

object Configuration {

  val watFileUrlListPath = "hdfs://master:9000/user/hduser/jpn/10k_wat_files/"

  val pathToWatFileStagingArea = "/media/priyan/data1/hdfs/temp/"

  val pathToWatFileHdfsFolder = "hdfs://master:9000/user/hduser/jpn/raw_wat_files/"

  val downloadedWatFileState = "hdfs://master:9000/user/hduser/jpn/downloaded_wat_file_list/"

}
