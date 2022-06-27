package com.example

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite

class PlaygroundUtilTest extends AnyFunSuite with SharedSparkContext {
  var main: DataFrame = _
  var dict: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    main = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", "Berlin"),
          Row("b", "Madrid"),
          Row("c", "Rome"),
        )
      ),
      new StructType()
        .add(StructField("index", StringType))
        .add(StructField("city", StringType))
    )

    dict = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a"),
          Row("b"),
        )
      ),
      new StructType()
        .add(StructField("index", StringType))
    )
  }

  test("in") {
    val result = PlaygroundUtil.in(main, dict, "index").orderBy("index")
    val expected = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", "Berlin"),
          Row("b", "Madrid"),
        )
      ),
      new StructType()
        .add(StructField("index", StringType))
        .add(StructField("city", StringType))
    ).orderBy("index")

    assert(result.schema === expected.schema)
    assert(result.collect() === expected.collect())
  }

  test("not in") {
    val result = PlaygroundUtil.notIn(main, dict, "index").orderBy("index")
    val expected = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("c", "Rome"),
        )
      ),
      new StructType()
        .add(StructField("index", StringType))
        .add(StructField("city", StringType))
    ).orderBy("index")

    assert(result.schema === expected.schema)
    assert(result.collect() === expected.collect())
  }

  test("semi vs inner") {
    val dictWithDuplicates = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a"),
          Row("b"),
          Row("b"),
        )
      ),
      new StructType()
        .add(StructField("index", StringType))
    )
    val result = main.join(dictWithDuplicates, Seq("index"), "inner").orderBy("index")
    val expected = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", "Berlin"),
          Row("b", "Madrid"),
          Row("b", "Madrid"),
        )
      ),
      new StructType()
        .add(StructField("index", StringType))
        .add(StructField("city", StringType))
    ).orderBy("index")

    assert(result.schema === expected.schema)
    assert(result.collect() === expected.collect())
  }
}
