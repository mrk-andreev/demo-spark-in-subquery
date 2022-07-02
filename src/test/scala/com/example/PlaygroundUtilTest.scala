package com.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.funsuite.AnyFunSuite


case class Data(index: String, city: String)

case class Dict(index: String)

class PlaygroundUtilTest extends AnyFunSuite with SharedSparkContext {
  var main: Dataset[Data] = _
  var dict: Dataset[Dict] = _

  def createDataDataset(values: Seq[Data]): Dataset[Data] = {
    spark.createDataset(values)(ExpressionEncoder[Data]())
  }

  def createDictDataset(values: Seq[Dict]): Dataset[Dict] = {
    spark.createDataset(values)(ExpressionEncoder[Dict]())
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    main = createDataDataset(Seq(Data("a", "Berlin"), Data("b", "Madrid"), Data("c", "Rome")))
    dict = createDictDataset(Seq(Dict("a"), Dict("b")))
  }

  test("in") {
    val result = main.join(dict, Seq("index"), "left_semi")
    val expected = createDataDataset(Seq(Data("a", "Berlin"), Data("b", "Madrid"))).orderBy("index")

    assert(result.schema === expected.schema)
    assert(result.collect() === expected.toDF().collect())
  }

  test("not in") {
    val result = main.join(dict, Seq("index"), "left_anti")
    val expected = createDataDataset(Seq(Data("c", "Rome"))).orderBy("index")

    assert(result.schema === expected.schema)
    assert(result.collect() === expected.toDF().collect())
  }

  test("semi vs inner") {
    val dictWithDuplicates = createDictDataset(Seq(Dict("a"), Dict("b"), Dict("b")))
    val result = main.join(dictWithDuplicates, Seq("index"), "inner").orderBy("index")
    val expected = createDataDataset(Seq(Data("a", "Berlin"), Data("b", "Madrid"), Data("b", "Madrid"))).orderBy("index")

    assert(result.schema === expected.schema)
    assert(result.collect() === expected.toDF().collect())
  }
}
