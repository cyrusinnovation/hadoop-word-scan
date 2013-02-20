package com.app

import org.scalatest.{BeforeAndAfter, FunSpec}
import org.apache.hadoop.mrunit.mapreduce.MapDriver

import org.scalatest.matchers.ShouldMatchers
import org.slf4j.LoggerFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}

class WordCountMapTest extends FunSpec with ShouldMatchers with HadoopImplicitConversions with BeforeAndAfter {
  val logger = LoggerFactory.getLogger(classOf[WordCountMapTest])
  val mapper = new WordCountMap
  val mapDriver = new MapDriver[LongWritable, Text, Text, IntWritable]()

  before {
    mapDriver.getConfiguration.setQuietMode(false)
    mapDriver.setMapper(mapper)
  }

  after {
    mapDriver.resetOutput()
  }

  describe ("counting words") {
    it("should be able to count the occurences of all words in an input") {
      mapDriver.withInput(1, "testing testing one one testing false")
      mapDriver.withOutput("false", 1).withOutput("testing", 3).withOutput("one", 2)
      mapDriver.runTest()
    }

    it("should be able to handle an empty input") {
      mapDriver.withInput(1, "")
      mapDriver.runTest()
    }
  }
}
