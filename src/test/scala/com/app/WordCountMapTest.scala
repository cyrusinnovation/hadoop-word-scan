package com.app

import org.apache.hadoop.mrunit.mapreduce.MapDriver

import org.slf4j.LoggerFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.conf.Configuration

class WordCountMapTest extends HadoopTestCase[LongWritable, Text, Text, IntWritable] {
  val logger = LoggerFactory.getLogger(classOf[WordCountMapTest])
  override val mapDriver = new MapDriver[LongWritable, Text, Text, IntWritable]()

  describe ("A default word counter") {
    it("should be able to count the occurrences of all words in an input") {
      mapDriver.withInput(1, "testing testing one one testing false")
      mapDriver.withOutput("false", 1).withOutput("testing", 3).withOutput("one", 2)
      mapDriver.runTest()
    }

    it("should be able to handle an empty input") {
      mapDriver.withInput(1, "")
      mapDriver.runTest()
    }

    it("should be able to handle lots of whitespace appropriately") {
      mapDriver.withInput(1, "testing      one      testing")
      mapDriver.withOutput("testing", 2).withOutput("one", 1)
      mapDriver.runTest()
    }
  }

  describe ("A word counter for only specific words") {
    it("should only count words I am looking for") {
      val configuration = new Configuration
      configuration.setStrings(WordCountMap.SEARCH_STRINGS_KEY, "foo", "bar")

      mapDriver.setConfiguration(configuration)
      mapDriver.withInput(1, "t abba abba zabba pony foo foo bar bar bar")
      mapDriver.withOutput("foo", 2).withOutput("bar", 3)
      mapDriver.runTest()
    }
  }

  def setupDriver() {
    mapDriver.setMapper(new WordCountMap)
  }
}