package io.github.fucusy

import org.apache.spark.sql.{DataFrame, Row, functions => F}
import org.apache.spark.sql.expressions.Window
import Viz.data2html


object DataframeViz {

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
                       title: String,
                       limitShowNumber: Int = 10): String = {
    val columnNames: Seq[String] = df.columns

    val newDF = df
      .withColumn("order", F.row_number.over(Window.orderBy(F.col(columnNames(0)).desc)))
      .filter(F.col("order") <= limitShowNumber)

    val contentInfoMap =
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

    val html = data2html(contentInfoMap, imageCol, title)
    html

  }

  def displayMultipleDataFrame(df1: DataFrame, df2: DataFrame) = {
    //    displayImage
    // make it to 2D with df1 col 0, df2 col 1

  }

  def display2DDataFrame(df: DataFrame) {
    require(df.columns.contains("row") && df.columns.contains("col"))
    // drop "col"


  }
}

