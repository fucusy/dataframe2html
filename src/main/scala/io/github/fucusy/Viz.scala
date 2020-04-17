package io.github.fucusy

object Viz {

  def data2html(column2data: Seq[(String, Seq[String])], imgCols: Seq[String], title: String): String = {
    ""
    val tableContent = column2data.map{
      case (colName: String, data: Seq[String]) =>
        val dataHtml = data.map(element => s"<td>$element</td>").mkString("")
        s"<tr><th>$colName</th>$dataHtml</tr>"
    }.mkString("")
    s"""
       |<html>
       |<body>
       |<h3>$title</h3>
       |<table>
       |  <tbody>
       |  $tableContent
       |  </tbody>
       |</table>
       |</body>
       |</html>
       |""".stripMargin
  }

}
