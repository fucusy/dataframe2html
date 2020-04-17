package io.github.fucusy

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}

import scala.io.Source

class TestDataframeViz extends AnyFunSuite {
  test("Test data2html") {
    val c2d = Seq(
      ("bob", "dance", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", "swimming", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png")
      .toDF("name", "hobby", "picture")
    val title = "Users"
    val html = TestDataframeViz.displayDataFrame(c2d, Seq("picture"), title)
    val trueHtml = Source.fromFile("src/test/resources/data2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
}
