package com.app

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver
import scala.collection.JavaConversions._

class WordCountReducerTest extends HadoopTestCase[Text, IntWritable, Text, IntWritable] {
  override val driver = new ReduceDriver[Text, IntWritable, Text, IntWritable]()

  def setupDriver() {
    driver.setReducer(new WordCountReducer)
  }

  describe ("Word count reducer") {
    it("should just get single value") {
      val dogValues: List[IntWritable] = List(5)

      driver.withInput("dog", dogValues)
      driver.withOutput("dog", 5)

      driver.runTest()
    }

    it("should sum multiple values") {
      val dogValues: List[IntWritable] = List(5, 11)

      driver.withInput("dog", dogValues)
      driver.withOutput("dog", 16)

      driver.runTest()
    }
  }
}
