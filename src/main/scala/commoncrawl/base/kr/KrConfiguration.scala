package commoncrawl.base.kr

import commoncrawl.base.DeltaLocalTable

object KrConfiguration {

  private val rawKrHttpLinksTableName = "RAW.WAT_HTTP_LINKS"
  private val rawKrLinksDeltaTablePath = "hdfs://master:9000/user/hive/warehouse/raw.db/wat_http_links_delta"
  val explodedRawWatFilesDeltaTable: DeltaLocalTable = DeltaLocalTable(rawKrHttpLinksTableName, rawKrLinksDeltaTablePath, rawKrLinksDeltaTablePath)


}
