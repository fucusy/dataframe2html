package io.github.fucusy

import io.github.fucusy.VizImplicit.VizDataFrame
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class TestVizImplicit extends AnyFunSuite {
  test("Test dataframe2html auto convert img url") {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello world")
    lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val c2d = Seq(
      ("bob", "dance", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", "swimming", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png")
    ).toDF("name", "hobby", "picture")
    val title = "Users"
    val html = c2d.toHTML()
    val trueHtml = Source.fromFile("src/test/resources/data2html_no_title.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
  test("Test dataframe2html2D") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello world")
    lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val c2d = Seq(
      ("bob", "dance", 1, "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", "swimming", 1, "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png"),
      ("rose", "no", 2, "https://raw.githubusercontent.com/fucusy/dataframe2html/41a1baae2dec4d8a815ceceece77d61213d4b1c1/src/test/resources/rose.png"),
      ("plum blossom", "no", 2, "https://raw.githubusercontent.com/fucusy/dataframe2html/41a1baae2dec4d8a815ceceece77d61213d4b1c1/src/test/resources/plum_blossom.png")

    ).toDF("name", "hobby", "row", "picture")

    val html = c2d.toHTML2D()
    val trueHtml = Source.fromFile("src/test/resources/dataframe2html2D_no_title.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
}
