package com.app

import org.scalatest.{FunSpec}

import org.scalatest.matchers.ShouldMatchers
import org.slf4j.LoggerFactory
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapTest extends FunSpec with ShouldMatchers {
  val logger = LoggerFactory.getLogger(classOf[WordCountMapTest])

  describe ("counting words") {
    it("should do some shit") {
      val mapper: Mapper[LongWritable, Text, Text, Text] = new LogScanMapper

      val mapDriver = new MapDriver[LongWritable, Text, Text, Text]()
      mapDriver.getConfiguration.setQuietMode(false)
      mapDriver.setMapper(mapper)

      mapDriver.withInput(1, "world Hello World")
      mapDriver.withOutput("world", "a")
      val result = mapDriver.runTest()
    }
  }
}
