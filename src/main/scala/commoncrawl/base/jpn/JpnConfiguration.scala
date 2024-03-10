package commoncrawl.base.jpn

import commoncrawl.base.DeltaLocalTable



object JpnConfiguration {

  private val rawJpnLinksTableName = "RAW.WAT_HTTP_LINKS"
  private val rawJpnLinksDeltaTablePath = "hdfs://master:9000/user/hive/warehouse/raw.db/wat_http_links_delta"
  val explodedJpnRawWatFilesDeltaTable: DeltaLocalTable = DeltaLocalTable(rawJpnLinksTableName, rawJpnLinksDeltaTablePath, rawJpnLinksDeltaTablePath)


  private val jpnJpgLinksTableName = ""
  private val jpnJpgLinksDeltaTablePath = "hdfs://master:9000/user/hive/warehouse/curated.db/jpn_jpg_links/"
  val jpnJpgLinksDeltaTable: DeltaLocalTable = DeltaLocalTable(jpnJpgLinksTableName, jpnJpgLinksDeltaTablePath, jpnJpgLinksDeltaTablePath)

  val pathToJpnWatFileHdfsFolder = "hdfs://master:9000/user/hduser/jpn/raw_wat_files_2/"
  val extractedJpnWatFileList = "hdfs://master:9000/user/hduser/jpn/extracted_wat_file_list"

}
