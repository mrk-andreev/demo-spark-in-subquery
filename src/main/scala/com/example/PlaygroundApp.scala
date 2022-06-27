package com.example

import org.apache.spark.sql.SparkSession

object PlaygroundApp {
  final val master = "local[*]"
  final val mainDataframePath = "data/main.csv"
  final val dictDataframePath = "data/dict.csv"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Spark Playground Application")
      .master(master)
      .getOrCreate()

    val main = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(mainDataframePath)

    val dict = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(dictDataframePath)

    PlaygroundUtil.in(main, dict, "index").show()
    PlaygroundUtil.notIn(main, dict, "index").show()
  }
}
