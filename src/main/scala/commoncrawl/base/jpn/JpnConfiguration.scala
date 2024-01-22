package commoncrawl.base.jpn

import commoncrawl.base.DeltaLocalTable



object JpnConfiguration {

  private val rawJpnLinksTableName = "RAW.JPN_WAT_HTTP_LINKS"
  private val rawJpnLinksDeltaTablePath = "hdfs://master:9000/user/hive/warehouse/raw.db/jpn_wat_http_links_delta"
  val explodedJpnRawWatFilesDeltaTable: DeltaLocalTable = DeltaLocalTable(rawJpnLinksTableName, rawJpnLinksDeltaTablePath, rawJpnLinksDeltaTablePath)

  val pathToJpnWatFileHdfsFolder = "hdfs://master:9000/user/hduser/jpn/raw_wat_files_2/"
  val extractedJpnWatFileList = "hdfs://master:9000/user/hduser/jpn/extracted_wat_file_list"

}
