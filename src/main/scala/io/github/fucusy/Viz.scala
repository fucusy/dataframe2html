package io.github.fucusy

import org.apache.spark.sql.{DataFrame, Row, functions => F}
import org.apache.spark.sql.expressions.Window

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
  /**
    * suppose df have A, B, C, D, order these five columns
    * first step: we get four information then can generate html
    * - the description of whole df
    * - the value of columns. make it a sequence: Seq[String, Seq[String] =
    * Seq(A -> Seq[....],
    * B -> Seq[....],
    * C -> Seq[....],
    * D -> Seq[....]) the seq value must in order
    * - imageCol: tell us which column is image.
    * - columnNames
    *
    * second step: using the four information got form step 1 to generate html
    *
    * @param df
    * @param imageCol
    * @param title : the description of whole df
    * @param limitShowNumber
    */

  def dateframe2html(df: DataFrame,
                       imageCol: Seq[String],
                       title: String,
                       limitShowNumber: Int = -1): String = {
    val ActualLimitShowNumber = if(limitShowNumber == -1){df.count()} else{limitShowNumber}
    val columnNames: Seq[String] = df.columns

    val newDF = df
      .withColumn("order", F.row_number.over(Window.orderBy(F.col(columnNames(0)).desc)))
      .filter(F.col("order") <= ActualLimitShowNumber)

    val contentInfo =
      columnNames.map {
        col =>
          (col, newDF
            .select(F.col(col), F.col("order"))
            .collect()
            .map{case Row(col: String, order: Int) =>
              (col, order)}
            .sortBy(_._2)
            .map(_._1)
            .toSeq
          )
      }

    val html = data2html(contentInfo, imageCol, title)
    html

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
