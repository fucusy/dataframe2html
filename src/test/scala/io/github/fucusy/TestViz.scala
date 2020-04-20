package io.github.fucusy

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class TestViz extends AnyFunSuite {
  test("Test data2html") {
    val c2d = Seq(
      ("name", Seq("bob", "amy")),
      ("hobby", Seq("dance", "swimming")),
      ("picture", Seq("https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png",
        "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png")))
    val title = "Users"
    val html = Viz.data2html(c2d, Seq("picture"), title)
    val trueHtml = Source.fromFile("src/test/resources/data2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }

  test("Test data2html auto detect img url") {
    val c2d = Seq(
      ("name", Seq("bob", "amy")),
      ("hobby", Seq("dance", "swimming")),
      ("picture", Seq("https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png",
        "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png")))
    val title = "Users"
    val html = Viz.data2html(c2d, title)
    val trueHtml = Source.fromFile("src/test/resources/data2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
  test("Test dataFrame2html") {
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
    val html = Viz.dataFrame2html(c2d, Seq("picture"), title)
    val trueHtml = Source.fromFile("src/test/resources/data2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }

  test("Test dataFrame2html2D") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("hello world")
    lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val c2d = Seq(
      ("bob", "dance", 1, 1, "Users","https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/bob.png"),
      ("amy", "swimming", 1, 2, "Users", "https://raw.githubusercontent.com/fucusy/dataframe2html/57c8b41dfc7368ad7d371c0a94614412abfb1de6/src/test/resources/amy.png"),
      ("rose", "no", 2, 1,"Flowers", "https://raw.githubusercontent.com/fucusy/dataframe2html/5c3f7f85a1160d8696f3f57b29690a4e92739c7e/src/test/resources/rose.png"),
      ("plum blossom", "no", 2, 2,"Flowers", "https://raw.githubusercontent.com/fucusy/dataframe2html/5c3f7f85a1160d8696f3f57b29690a4e92739c7e/src/test/resources/plum_blossom.png")

    ).toDF("name", "hobby", "col", "row", "title", "picture")

    val html = Viz.dataFrame2html2D(c2d)
    val trueHtml = Source.fromFile("src/test/resources/dataFrame2html2D.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
}
