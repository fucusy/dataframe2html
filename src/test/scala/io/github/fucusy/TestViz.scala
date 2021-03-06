package io.github.fucusy

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import io.github.fucusy.VizImplicit.VizDataFrame

import scala.io.Source

class TestViz extends AnyFunSuite {

  test("Test data2html auto detect img url") {
    val c2d = Seq(
      ("name", Seq("bob", "amy")),
      ("null_col", Seq("null", "null")),
      ("hobby", Seq("dance", "swimming")),
      ("picture", Seq("https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png",
        "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png")))
    val title = "Users"
    val html = Viz.data2html(c2d, title)
    val trueHtml = Source.fromFile("src/test/resources/dataframe2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
  test("Test dataframe2html") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello world")
    lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val c2d = Seq(
      ("bob", null, "dance", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", null, "swimming", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png")
    ).toDF("name", "null_col", "hobby", "picture")

    val title = "Users"
    val html = Viz.dataframe2html(c2d, title)
    val htmlFromImplicit = c2d.toHTML(title = title)
    val trueHtml = Source.fromFile("src/test/resources/dataframe2html.html").mkString("")

    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", ""))
    assert(htmlFromImplicit.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", ""))
  }

  test("Test dataframe2html2D") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello world")
    lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val c2d = Seq(
      ("bob", "dance", 1, 1, "Users", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", "swimming", 1, 2, "Users", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png"),
      ("rose", "no", 2, 1, "Flowers", "https://raw.githubusercontent.com/fucusy/dataframe2html/41a1baae2dec4d8a815ceceece77d61213d4b1c1/src/test/resources/rose.png"),
      ("plum blossom", "no", 2, 2, "Flowers", "https://raw.githubusercontent.com/fucusy/dataframe2html/41a1baae2dec4d8a815ceceece77d61213d4b1c1/src/test/resources/plum_blossom.png")

    ).toDF("name", "hobby", "row_order", "col_order", "title", "picture")

    val html = Viz.dataframe2html2D(c2d, "row_order", "col_order", rowTitleCol = Some("title"))
    val htmlFromImplicit = c2d.toHTML2D("row_order", "col_order", rowTitleCol = Some("title"))
    val trueHtml = Source.fromFile("src/test/resources/dataframe2html2D.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", ""))
    assert(htmlFromImplicit.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", ""))
  }
  test("Test dataframe2html2D noRowTitle") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello world")
    lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val c2d = Seq(
      ("bob", "dance", 1, 1, "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", "swimming", 1, 2, "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png"),
      ("rose", "no", 2, 1, "https://raw.githubusercontent.com/fucusy/dataframe2html/41a1baae2dec4d8a815ceceece77d61213d4b1c1/src/test/resources/rose.png"),
      ("plum blossom", "no", 2, 2, "https://raw.githubusercontent.com/fucusy/dataframe2html/41a1baae2dec4d8a815ceceece77d61213d4b1c1/src/test/resources/plum_blossom.png")

    ).toDF("name", "hobby", "row_order", "col_order", "picture")

    val html = Viz.dataframe2html2D(c2d, "row_order", "col_order")
    val htmlFromImplicit = c2d.toHTML2D("row_order", "col_order")
    val trueHtml = Source.fromFile("src/test/resources/dataframe2html2DNoRowTitle.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", ""))
    assert(htmlFromImplicit.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", ""))
  }
}
