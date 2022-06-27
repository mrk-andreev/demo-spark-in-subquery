package com.example

import org.apache.spark.sql.DataFrame

object PlaygroundUtil {
  def in(main: DataFrame, dict: DataFrame, joinColumn: String): DataFrame = {
    main.join(dict, Seq(joinColumn), "left_semi")
  }

  def notIn(main: DataFrame, dict: DataFrame, joinColumn: String): DataFrame = {
    main.join(dict, Seq(joinColumn), "left_anti")
  }
}
