package io.github.fucusy

import org.apache.spark.sql.{Column, DataFrame, Row, functions => F}
import org.apache.spark.sql.expressions.Window

object Viz {
  def imgUrl2tag(url: String) = s"""<img src="$url" width="100"/>"""

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
   * @param title : the description of whole df
   * @param limitShowNumber
   */
  def dataframe2html(df: DataFrame,
                     title: String,
                     limitShowNumber: Int = -1): String = {
    val contentInfo = dataframe2data(df, limitShowNumber = limitShowNumber)
    val html = data2html(contentInfo, title)
    html
  }

  /**
    * this method will change dataframe to html which is very convenient to show as a 2D visualization.
    * since it's a 2D visualization, you must provide the rowOrderCol
    * @param df: the dataframe you want to convert to html
    * @param rowOrderCol: the row order column
    * @param colOrderCol:the col order column
    * @param rowTitleCol: the row title column, for contents in the same row,
    *                   you must provide the same title, if no, we will randomly choose one.
    * @param limitShowNumber
    * @return
    */

  def dataframe2html2D(df: DataFrame,
                       rowOrderCol: String,
                       colOrderCol: String,
                       rowTitleCol: Option[String] = None,
                       limitShowNumber: Int = -1
                       ): String = {
    require(df.columns.contains(rowOrderCol))
    val addRowTitleDF = df.transform(addRowTitle(rowTitleCol))

    val rowTitleColumn: Column = rowTitleCol match {
      case Some(rowTitle) => df.col(rowTitle)
      case None      => df.col("row_title")
    }
    val rowNumList = addRowTitleDF.select(rowOrderCol)
      .distinct()
      .collect
      .map(_.getAs[Int](rowOrderCol))
      .sorted
    val tables = rowNumList
      .map {
        i =>
          val oneRowDF = df.filter(F.col(rowOrderCol) === i)
            .drop(rowOrderCol)
          val title = oneRowDF.select(rowTitleColumn).first().getString(0)
          val contentInfo = dataframe2data(oneRowDF.drop(rowTitleColumn).orderBy(colOrderCol), limitShowNumber)
          data2table(contentInfo, title)
      }
      .mkString("\n")
    warpBody(tables)
  }

  private def addRowTitle(rowTitle: Option[String])(df: DataFrame): DataFrame = {
    if(rowTitle != None) {
      df
    }else{
      df.withColumn("row_title", F.row_number.over(Window.orderBy(F.col(df.columns(0)).desc)))
    }
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
    warpBody(tableContent)
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
       |<div style="background-color:lightblue"><h3>$title</h3></div>
       |<table class="table table-bottom table-hover table-sm">
       |  <tbody>
       |  $tableContent
       |  </tbody>
       |</table>
       |""".stripMargin
  }

  def warpBody(body: String): String = {
    s"""
       |<html>
       |<head>
       |    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
       |</head>
       |<body> $body </body>
       |</html>
       |""".stripMargin
  }
}
