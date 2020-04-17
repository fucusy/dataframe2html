package io.github.fucusy

import org.apache.spark.sql.{DataFrame, Row, functions => F}
import org.apache.spark.sql.expressions.Window
import Viz.data2html


object DataframeViz {
  private val limitShowNumber = 10

  /**
    * suppose df have A, B, C, D, order these five columns
    * first step: we get four information then can generate html
    * - the description of whole df
    * - the value of columns. make it a map contentInfoMap: Map[String, Seq[String] =
    * Map(A -> Seq[....],
    * B -> Seq[....],
    * C -> Seq[....],
    * D -> Seq[....]) the seq value must in order
    * - imageCol: tell us which column is image.
    * - columnNames
    *
    * second step: using the four information got form step 1 to generate table
    *
    * @param df
    * @param orderCol
    * @param imageCol
    * @param limitShowNumber
    */

  def displayDataFrame(df: DataFrame,
                       imageCol: Seq[String],
                       limitShowNumber: Option[Int],
                       title: String): String = {
    val columnNames: Seq[String] = df.columns
    df.withColumn("order", F.row_number.over(Window.orderBy(F.col(columnNames(0)))))
      .filter(F.col("order") <= limitShowNumber)
    val title = df.select("title").head().getAs[String]("title")
    val contentInfoMap =
      columnNames.map {
        col =>
          (col, df
            .select(F.col(col), F.col("order"))
            .collect()
            .map{case Row(col: String, order: Int) =>
              (col, order)}
            .sortBy(_._2)
            .map(_._1)
              .toSeq
          )
      }
    val imageCol = df.select("image_col").head().getSeq[String](0)
    val html = data2html(contentInfoMap, imageCol, title)
    html

  }

  def displayMultipleDataFrame(df1: DataFrame, df2: DataFrame) = {
    require(df1.columns.contains("title") && df2.columns.contains("title"))
    //    displayImage
    // make it to 2D with df1 col 0, df2 col 1

  }

  def display2DDataFrame(df: DataFrame) {
    require(df.columns.contains("title") && df.columns.contains("row") && df.columns.contains("col"))
    // drop "col"


  }
}

