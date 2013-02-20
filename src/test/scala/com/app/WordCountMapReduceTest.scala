package com.app

import org.apache.hadoop.io.{LongWritable, IntWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver

class WordCountMapReduceTest extends HadoopTestCase[LongWritable, Text, Text, IntWritable] {
  val mapper =  new WordCountMap
  val reducer = new WordCountReducer
  val driver = new MapReduceDriver[LongWritable, Text, Text, IntWritable, Text, IntWritable](mapper, reducer)

  def setupDriver() {

  }

  describe ("mapping and reducing to count words") {
    it("should map and than reduce results") {
      driver.withInput(1, "testing testing one one testing false")
      driver.withOutput("false", 1).withOutput("one", 2).withOutput("testing", 3)
      driver.runTest()
    }
  }
}
