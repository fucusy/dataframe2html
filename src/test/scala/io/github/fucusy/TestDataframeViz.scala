package io.github.fucusy

import org.scalatest.funsuite.AnyFunSuite
import DataframeViz.displayDataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => F}
import spark.implicits._

import scala.io.Source

class TestDataframeViz extends AnyFunSuite {
  test("Test data2html") {
    val c2d = Seq(
      ("bob", "dance", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", "swimming", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png")
    ).toDF("")


//    val dataSchema = StructType(
////      Array(StructField("name", StringType, true),
////        StructField("hobby", StringType, true),
////        StructField("picture", StringType, true)
////    ))
////
////    val spark = SparkSession
////      .builder()
////      .appName("Spark SQL basic example")
////      .config("spark.some.config.option", "some-value")
////      .getOrCreate()
////
////    val data = spark.createDataFrame(spark.sparkContext.parallelize(c2d), dataSchema)
    
    val title = "Users"
    val html = DataframeViz.displayDataFrame(c2d, Seq("picture"), title)
    val trueHtml = Source.fromFile("src/test/resources/data2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
}
