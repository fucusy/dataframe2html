package io.github.fucusy

import org.apache.spark.sql.{DataFrame, Row, functions => F}

object Viz {
  def imgUrl2tag(url: String) = s"""<img src="$url" height="250" width="250"/>"""

  def isImgUrl(s: String): Boolean = {
    val imgSuffix = Seq("jpg", "png", "gif")
    if (s.contains(".")) {
      val suffix = s.split("\\.").last
      s.startsWith("http") && imgSuffix.contains(suffix)
    } else {
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
   * D -> Seq[....]) if the seq value is image, we will handle it automatically and specially.
   * - columnNames
   *
   * second step: using the four information got form step 1 to generate html
   *
   * @param df
   * @param title   : the description of whole df
   * @param limitShowNumber
   */
  def dataframe2html(df: DataFrame,
                     title: String,
                     limitShowNumber: Int = -1): String = {
    val contentInfo = dataframe2data(df, limitShowNumber)
    val html = data2html(contentInfo, title)
    html
  }

  /**
    *
    * @param df which need to contain column "row" to let us know which row you want to show. you should start with 1 in this column
    *           df also need to contain column "title" to let us know the title of each visualization row.
    *           you need to give the same title the all data you want to show in the same visualization row.
    *           or we will randomly choose one title.
    * @param limitShowNumber
    * @return
    */

  def dataframe2html2D(df: DataFrame,
                       limitShowNumber: Int = -1): String = {
    require(df.columns.contains("row") && df.columns.contains("title"))
    val rowNum = df.select("row").distinct().count().toInt
    val tables = (1 to rowNum)
      .map {
        case i =>
          val oneRowDF = df.filter(F.col("row") === i)
             .drop("row")
          val title = oneRowDF.select("title").first().getString(0)
          val contentInfo = dataframe2data(oneRowDF.drop("title"), limitShowNumber)
          data2table(contentInfo, title)
      }
      .mkString("\n")
    s"""
        <html>
          <body>$tables</body>
        </html>
      """
  }

  def dataframe2data(df: DataFrame, limitShowNumber: Int = -1): Seq[(String, Seq[String])] = {
    val columnNames: Seq[String] = df.columns
    val collectDF = df.select(columnNames.head, columnNames.tail: _*)
      .collect()
      .map { r: Row =>
        r.toSeq.map { item =>
          if (item == null) {
            "null"
          } else {
            item.toString
          }
        }
      }

    val actualLimitShowNumber = if (limitShowNumber == -1) {
      collectDF(0).size
    } else {
      limitShowNumber
    }
    columnNames.zipWithIndex.map {
      case (col: String, idx: Int) => (col, collectDF.map(_ (idx)).slice(0, actualLimitShowNumber).toSeq)
    }
  }


  /**
    * convert data to html, it will automatically convert image url to img tag in html
    *
    * @param column2data the data, each element contains column name, and a list of string
    * @param title
    * @return
    */
  def data2html(column2data: Seq[(String, Seq[String])], title: String): String = {
    val tableContent = data2table(column2data, title)
    s"""
       |<html>
       |<body>
       |  $tableContent
       |</body>
       |</html>
       |""".stripMargin
  }

  /**
    * convert data to the table part of html, it will automatically convert image url to img tag in html
    *
    * @param column2data the data, each record contains column name, and a list of string
    * @param title
    * @return
    */
  def data2table(column2data: Seq[(String, Seq[String])], title: String): String = {
    val updatedData = column2data.map {
      case (colName: String, elements: Seq[String]) => (colName, elements.map { element =>
        if (isImgUrl(element)) {
          imgUrl2tag(element)
        } else {
          element
        }
      })
    }
    val tableContent = updatedData.map {
      case (colName: String, elements: Seq[String]) =>
        val dataHtml = elements
          .map(element => s"<td>$element</td>").mkString("")
        s"<tr><th>$colName</th>$dataHtml</tr>"
    }.mkString("")
    s"""
       |<h3>$title</h3>
       |<table>
       |  <tbody>
       |  $tableContent
       |  </tbody>
       |</table>
       |""".stripMargin
  }
}
