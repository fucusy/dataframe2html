package io.github.fucusy

import org.joda.time.{DateTime, Days}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window


object dataframe2html {
  private val limitShowNumber = 10

  /**
    * suppose df have A, B, C, D, order these five columns
    * first step: we get four information then can generate html
    * - the description of whole df
    * - the value of columns. make it a map contentInfoMap: Map[String, Seq[String] =
    *      Map(A -> Seq[....],
    *          B -> Seq[....],
    *          C -> Seq[....],
    *          D -> Seq[....]) the seq value must in order
    * - imageCol: tell us which column is image.
    * - columnNames
    *
    * second step: using the four information got form step 1 to generate table
    * @param df
    * @param orderCol
    * @param imageCol
    * @param limitShowNumber
    */

  def displayDataFrame(df: DataFrame,
                       orderCol: Option[String],
                       imageCol: Seq[String],
                       limitShowNumber: Option[Int]): String ={
    require(df.columns.contains("image_col") && df.columns.contains("description"))
    df.withColumn("order", F.row_number.over(Window.orderBy($"description")))
      .filter($"order" <= limitShowNumber)
    // a b c d order
    val columnNames: Seq[String] = df.columns

    val contentInfoMap =
      columnNames.map{
        col => col->df
          .orderBy("order")
          .select(F.col(col))
          .collect()
          .toSeq()
      }.toMap
    val description = df.select("description").head().getAs[String]("description")

    val imageCol = df.select("image_col").head().getSeq[String](0)

    val table = generateTable(description, contentInfoMap, columnNames, imageCol)

    s"""
      <!DOCTYPE html>
        <html>
          <head>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
            <style>p.red { color: red; } p.more { color: red; font-weight: bold; }</style>
          </head>
          <body>$table</body>
        </html>
      """
  }

  /**
    * generate HTML string for displaying a solo container
    *
    * @param description       Description of a sequence of video id.
    * @param contentInfoMap Concrete information of videoIds
    * @param fieldsToShow      Information needed to show
    * @param showedNum         The num of video limited to show, default 16
    * @return HTML string
    */
  def generateTable(description: String,
                    contentInfoMap: Map[String, Seq[String]],
                    fieldsToShow: Seq[String]): String = {
    s"""<div style="background-color:lightblue"><h3>$description: </h3></div>""" + constructHTMLTable(contentInfoMap,
      fieldsToShow)
  }



  def displayMultipleDataFrame(df1: DataFrame, df2: DataFrame) = {
    require(df1.columns.contains("image_col") && df1.columns.contains("title"))
    require(df2.columns.contains("image_col") && df2.columns.contains("title"))
    //    displayImage

    // make it to 2D with df1 col 0, df2 col 1

  }

  def display2DDataFrame(df: DataFrame) {
    require(df.columns.contains("image_col") && df.columns.contains("title")
      && df.columns.contains("row") && df.columns.contains("col"))
    // drop "col"


  }

  /**
    *
    * @param contentMap field -> a list of value
    * @param fieldsToShow fields
    */
  private def constructHTMLTable(contentMap: Map[String, Seq[String]], fieldsToShow: Seq[String]): String = {
    val rows = fieldsToShow.map {
      case x @ i if contentMap contains i =>
        constructHTMLRow(x, contentMap(i))

    }
    s"""
       |<table class="table table-bottom table-hover table-sm">
       |  <tbody>${rows.mkString("\n")}</tbody>
       |</table>
     """.stripMargin
  }
  /**
    * Construct HTML Row by using one field info of a seq.
    */
  private def constructHTMLRow(field: String, values: Seq[String]): String = {
    val content = if (field == "post_image" || field == "hero_images") {
      """<img src="%s" width="100px">"""
    } else {
      "%s"
    }
    val tds = values.map { v =>
      if (v != "") s"<td>$content</td>".format(v) else s"<td></td>"
    }
    s"<tr><th>$field</th>${tds.mkString("")}</tr>"
  }
}

