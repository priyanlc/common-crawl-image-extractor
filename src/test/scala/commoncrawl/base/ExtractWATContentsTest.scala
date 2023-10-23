package commoncrawl.base

import commoncrawl.base.ExtractWatContents.extractWATContents
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ExtractWATContentsTest extends AnyFunSuite with BeforeAndAfterAll{

  implicit var spark: SparkSession = _
  override protected def beforeAll(): Unit =
    spark = SparkSession.builder()
      .appName("testing spark")
      .master("local[1]")
      .getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  val folderLocation = "src/test/resources"
  val watFileName = "CC-MAIN-sample.warc.wat.gz"

  test("extractWATContents should return a DataFrame from WAT file") {
    val expectedPath = s"$folderLocation/$watFileName"
    val result = extractWATContents(folderLocation,watFileName)
    println(result.count())
    assert(result.count()>0)
  }
}
