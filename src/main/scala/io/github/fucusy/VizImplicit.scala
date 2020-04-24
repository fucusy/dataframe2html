package io.github.fucusy

import org.apache.spark.sql.DataFrame

object VizImplicit {

  implicit class VizDataFrame(df: DataFrame) {
    def toHTML(title: String = "", limit: Int = 100): String = {
      Viz.dataframe2html(df, title, limit)
    }
    def toHTML2D(rowOrder: String,
                 colOrderCol: Option[String] = None,
                 rowTitleCol: Option[String] = None,
                 limit: Int = 100
                ): String = {

      Viz.dataframe2html2D(df, rowOrder, colOrderCol, rowTitleCol, limit)
    }
  }
}
