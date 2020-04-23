package io.github.fucusy

import org.apache.spark.sql.{DataFrame, Row, functions => F}
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
    val contentInfo = dataframe2data(df, limitShowNumber)
    val html = data2html(contentInfo, title)
    html
  }

  /**
   *
   * @param df which need to contain column "row_order" to let us know which row you want to show. you should start with 1 in this column
   *           df also need to contain column "title" to let us know the title of each visualization row.
   *           you need to give the same title the all data you want to show in the same visualization row.
   *           or we will randomly choose one title.
   * @param limitShowNumber
   * @return
   */

  def dataframe2html2D(df: DataFrame,
                       limitShowNumber: Int = -1): String = {
    require(df.columns.contains("row_order") && df.columns.contains("title"))
    val rowNum = df.select("row_order").distinct().count().toInt
    val tables = (1 to rowNum)
      .par
      .map {
        i =>
          val oneRowDF = df.filter(F.col("row_order") === i)
            .drop("row_order")
          val title = oneRowDF.select("title").first().getString(0)
          val contentInfo = dataframe2data(oneRowDF.drop("title"), limitShowNumber)
          data2table(contentInfo, title)
      }
      .mkString("\n")
    warpBody(tables)
  }

  private def addColOrder(df: DataFrame): DataFrame = {
    if(df.columns.contains("col_order")) {
      df
    }else{
      df.withColumn("col_order", F.row_number.over(Window.orderBy(F.col(df.columns(0)).desc)))
    }
  }

  def dataframe2data(df: DataFrame, limitShowNumber: Int = -1): Seq[(String, Seq[String])] = {
    val columnNames: Seq[String] = df.columns

    val ActualLimitShowNumber = if(limitShowNumber == -1){df.count()} else{limitShowNumber}
    val newDF = df
      .transform(addColOrder)
      .filter(F.col("col_order") <= ActualLimitShowNumber)

    val contentInfo =
      columnNames
        .filter(_ != "col_order")
        .map { col =>
          (col, newDF
            .withColumn(col, F.col(col).cast("string"))
            .withColumn(col, F.when(F.col(col).isNull, "null").otherwise(F.col(col)))
            .select(F.col(col), F.col("col_order"))
            .collect()
            .map{case Row(col: String, col_order: Int) =>
              (col, col_order)}
            .sortBy(_._2)
            .map(_._1)
            .toSeq
          )
      }
    contentInfo
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
