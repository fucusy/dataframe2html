package io.github.fucusy

import org.joda.time.{DateTime, Days}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
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
                       limitShowNumber: Option[Int]): String = {
    val columnNames: Seq[String] = df.columns
    df.withColumn("order", F.row_number.over(Window.orderBy(F.col(columnNames(0)))))
      .filter($"order" <= limitShowNumber)
    val contentInfoMap =
      columnNames.map {
        col =>
          (col, df
            .orderBy("order")
            .select(F.col(col))
            .collect()
            .toSeq())
      }
    val description = df.select("description").head().getAs[String]("description")
    val imageCol = df.select("image_col").head().getSeq[String](0)
    val table = data2html(contentInfoMap, description, imageCol)

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

