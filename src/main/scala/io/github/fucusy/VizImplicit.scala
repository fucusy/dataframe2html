package io.github.fucusy

import org.apache.spark.sql.DataFrame

object VizImplicit {

  implicit class VizDataFrame(df: DataFrame) {
    def toHTML(title: String = "", limit: Int = 100): String = {
      Viz.dataframe2html(df, title, limit)
    }
  }
}
