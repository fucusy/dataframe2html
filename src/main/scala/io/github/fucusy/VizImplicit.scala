package io.github.fucusy

import org.apache.spark.sql.{DataFrame, functions => F}

object VizImplicit {

  implicit class VizDataFrame(df: DataFrame) {
    def toHTML(title: String = "", limit: Int = 100): String = {
      Viz.dataframe2html(df, title, limit)
    }
    def toHTML2D(limit: Int = 100): String = {
      val columnNames = df.columns
      if(columnNames.contains("title")){
        Viz.dataframe2html2D(df, limit)
      }
      else{
        Viz.dataframe2html2D(df.withColumn("title", F.lit("")), limit)
      }

    }
  }
}
