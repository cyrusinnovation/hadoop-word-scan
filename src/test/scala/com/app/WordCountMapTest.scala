package com.app

import org.apache.hadoop.mrunit.mapreduce.MapDriver

import org.slf4j.LoggerFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.conf.Configuration

class WordCountMapTest extends HadoopTestCase[LongWritable, Text, Text, IntWritable] {
  val logger = LoggerFactory.getLogger(classOf[WordCountMapTest])
  override val driver = new MapDriver[LongWritable, Text, Text, IntWritable]()

  def setupDriver() {
    driver.setMapper(new WordCountMap)
  }

  describe ("A default word counter") {
    it("should be able to count the occurrences of all words in an input") {
      driver.withInput(1, "testing testing one one testing false")
      driver.withOutput("false", 1).withOutput("testing", 3).withOutput("one", 2)
      driver.runTest()
    }

    it("should be able to handle an empty input") {
      driver.withInput(1, "")
      driver.runTest()
    }

    it("should be able to handle lots of whitespace appropriately") {
      driver.withInput(1, "testing      one      testing")
      driver.withOutput("testing", 2).withOutput("one", 1)
      driver.runTest()
    }
  }

  describe ("A word counter for only specific words") {
    it("should only count words I am looking for") {
      driver.getConfiguration.setStrings(WordCountMap.SEARCH_STRINGS_KEY, "foo", "bar")

      driver.withInput(1, "t abba abba zabba pony foo foo bar bar bar")
      driver.withOutput("foo", 2).withOutput("bar", 3)
      driver.runTest()
    }
  }
}