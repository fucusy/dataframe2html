package io.github.fucusy

import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class TestViz extends AnyFunSuite {
  test("Test data2html") {
    val c2d = Seq(("name", Seq("bob", "amy")), ("hobby", Seq("dance", "swimming")))
    val title = "Users"
    val html = Viz.data2html(c2d, Seq(), title)
    val trueHtml = Source.fromFile("src/test/resources/data2html.html").mkString("")
    assert(html.replaceAll("\\s", "") == trueHtml.replaceAll("\\s", "")
    )
  }
}
