package commoncrawl.base.kr.domains

import org.apache.spark.sql.DataFrame

object KrDomainsConfiguration {

  /*
    Naver: www.naver.com
    Kakao: www.kakao.com
    Daum: www.daum.net
    Coupang: www.coupang.com
    TMON: www.tmon.co.kr
    Nate: www.nate.com
    Auction: www.auction.co.kr
    Gmarket: www.gmarket.co.kr
    11st: www.11st.co.kr
    Donga.com: www.donga.com
    Chosun.com: www.chosun.com
    Joins: www.joins.com
    YNA (Yonhap News Agency): www.yna.co.kr
    Nexon: www.nexon.com
    Netmarble: www.netmarble.com
    Yes24: www.yes24.com
    Interpark: www.interpark.com
    Musinsa: www.musinsa.com
    Danawa: www.danawa.com
    NHN: www.nhn.com
   */

  val checkpointLocationNaverHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/naver_write_http"
  val checkpointLocationKakakoHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/kakao_write_http"
  val checkpointLocationDaumHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/daum.net_write_http"
  val checkpointLocationCoupangHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/coupang.com_write_http"
  val checkpointLocationNateHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/nate_write_http"
  val checkpointLocationChosunHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/chosun_write_http"
  val checkpointLocationJoinsHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/joins_write_http"
  val checkpointLocationMusinsaHttpDestination = "/user/hive/warehouse/curated.db/checkpoint/dir/kr/domain/musinsa_write_http"

  def filterNaverDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like 'www.naver.com/%'")
  }

  def filterKakaoDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.kakao.com/%'")
  }

  def filterDaumDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.daum.net/%'")
  }

  def filterCoupangDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.coupang.com/%'")
  }

  def filterNateDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.nate.com/%'")
  }


  def filterChosunDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.chosun.com/%'")
  }

  def filterJoinsDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.joins.com/%'")
  }

  def filterMunisaDomainFiles(dfUrls: DataFrame):DataFrame = {
    dfUrls
      .where("lower(url) like '%.musinsa.com/%'")
  }




}
