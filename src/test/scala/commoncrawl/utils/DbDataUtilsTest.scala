package commoncrawl.utils

import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.tukaani.xz.lz.Matches

class DbDataUtilsTest extends AnyFunSuite with SharedSparkContext {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db LOCATION 'tmp/test_db1.db' ");
  }

//  test("Counts based on various ranges") {
//    spark.sql("CREATE DATABASE IF NOT EXISTS test_db LOCATION 'tmp/test_db1.db' ");
//    spark.sql("DROP DATABASE IF EXISTS test_db ");
//  }

}
