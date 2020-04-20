package io.github.fucusy

import org.apache.spark.sql.{DataFrame, Row, functions => F}

object Viz {
  def imgUrl2tag(url: String) = s"""<img src="$url"/>"""

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
   * D -> Seq[....]) the seq value must in order
   * - columnNames
   *
   * second step: using the four information got form step 1 to generate html
   *
   * @param df
   * @param imgCols : tell us which column is image.
   * @param title   : the description of whole df
   * @param limitShowNumber
   */
  def dataFrame2html(df: DataFrame,
                     imgCols: Seq[String],
                     title: String,
                     limitShowNumber: Int = -1): String = {
    val contentInfo = dataFrame2data(df, limitShowNumber)
    val html = data2html(contentInfo, imgCols, title)
    html
  }

  def dataFrame2html2D(df: DataFrame,
                       limitShowNumber: Int = -1): String = {
    require(df.columns.contains("row") && df.columns.contains("col") && df.columns.contains("title"))

    val rowNum: Int = df.select("col").agg(F.max("col").as("max_col"))
      .select("max_col")
      .first()
      .getInt(0)
    val tables = (1 to rowNum)
      .map {
        case i =>
          val oneRowDF = df.filter(F.col("col") === i)
             .drop("col", "row")
          val title = oneRowDF.select("title").first().getString(0)
          val contentInfo = dataFrame2data(oneRowDF.drop("title"), limitShowNumber)
        data2table(contentInfo, title)
      }
      .mkString("\n")
    s"""
        <html>
          <body>$tables</body>
        </html>
      """
  }

  /**
   * Automatically convert img url to img tag
   * @param df
   * @param title   : the description of whole df
   * @param limitShowNumber
   */
  def dataFrame2html(df: DataFrame,
                     title: String,
                     limitShowNumber: Int): String = {
    val contentInfo = dataFrame2data(df, limitShowNumber)
    val html = data2html(contentInfo, title)
    html
  }

  def dataFrame2data(df: DataFrame, limitShowNumber: Int = -1): Seq[(String, Seq[String])] = {
    val columnNames: Seq[String] = df.columns
    val collectDF = df.select(columnNames.head, columnNames.tail: _*)
      .collect()
      .map { r: Row => r.toSeq.map(_.toString) }

    val actualLimitShowNumber = if (limitShowNumber == -1) {
      collectDF(0).size
    } else {
      limitShowNumber
    }
    columnNames.zipWithIndex.map {
      case (col: String, idx: Int) => (col, collectDF.map(_ (idx)).slice(0, actualLimitShowNumber).toSeq)
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
    * convert data to html
    *
    * @param column2data the data, each record contains column name, and a list of string
    * @param imgCols     indicate the image url columns, the url will be convert to img tag
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
