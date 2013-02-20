package com.app

import org.scalatest.FunSpec
import org.apache.hadoop.mrunit.mapreduce.MapDriver

import org.scalatest.matchers.ShouldMatchers
import org.slf4j.LoggerFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapTest extends FunSpec with ShouldMatchers with HadoopImplicitConversions{
  val logger = LoggerFactory.getLogger(classOf[WordCountMapTest])

  describe ("counting words") {
    it("should do some shit") {
      val mapper: Mapper[LongWritable, Text, Text, IntWritable] = new WordCountMap

      val mapDriver = new MapDriver[LongWritable, Text, Text, IntWritable]()
      mapDriver.getConfiguration.setQuietMode(false)
      mapDriver.setMapper(mapper)

      mapDriver.withInput(1, "world Hello World")
      mapDriver.withOutput("world", 1)
      mapDriver.runTest()
    }
  }
}
