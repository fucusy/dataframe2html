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
  test("Test dataframe2html") {
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
    val html = Viz.dateframe2html(c2d, Seq("picture"), title)
    val trueHtml = Source.fromFile("src/test/resources/data2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
}
