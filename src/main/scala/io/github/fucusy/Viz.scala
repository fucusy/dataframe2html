package io.github.fucusy

object Viz {

  def imgUrl2tag(url: String) = s"""<img src="$url"/>"""

  def isImgUrl(s: String): Boolean = {
    val imgSuffix = Seq("jpg", "png", "gif")
    if (s.contains(".")) {
      val suffix = s.split("\\.").last
      s.startsWith("http") && imgSuffix.contains(suffix)
    } else{
      false
    }
  }

  /** *
   * convert data to html
   *
   * @param column2data the data, each record contains column name, and a list of string
   * @param imgCols     indicate the image url columns, the url will be convert to img tag
   * @param title
   * @return
   */
  def data2html(column2data: Seq[(String, Seq[String])], imgCols: Seq[String], title: String): String = {
    val tableContent = column2data.map {
      case (colName: String, elements: Seq[String]) =>
        val dataHtml = elements
          .map {
            element =>
              if (imgCols.contains(colName)) {
                imgUrl2tag(element)
              } else {
                element
              }
          }
          .map(element => s"<td>$element</td>").mkString("")
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


  /** *
   * convert data to html, it will automatically convert image url to img tag in html
   *
   * @param column2data the data, each element contains column name, and a list of string
   * @param title
   * @return
   */
  def data2html(column2data: Seq[(String, Seq[String])], title: String): String = {
    val updatedData = column2data.map {
      case (colName: String, elements: Seq[String]) => (colName, elements.map { element =>
        if (isImgUrl(element)) {
          imgUrl2tag(element)
        } else {
          element
        }
      })
    }
    data2html(updatedData, Seq(), title)
  }
}
