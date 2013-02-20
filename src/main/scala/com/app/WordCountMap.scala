package com.app

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.{IntWritable, Text, LongWritable}
import scala.collection.JavaConversions._


class WordCountMap extends Mapper[LongWritable, Text, Text, IntWritable] {

  private val word: Text = new Text()
  private val count: IntWritable = new IntWritable()

  type MapperContext = Mapper[LongWritable, Text, Text, IntWritable]#Context

  private def createSearchStringFilter(strings: List[String]): String => Boolean = {
    if (strings.isEmpty) { x => true} else { x => strings.contains(x) }
  }

  override def map(key: LongWritable, value: Text, context: MapperContext) {
    val searchStrings = context.getConfiguration.getStringCollection(WordCountMap.SEARCH_STRINGS_KEY)
    val searchFilter = createSearchStringFilter(searchStrings.toList)

    val wordsToCounts = value.toString
                        .split(" ")
                        .filterNot(_.isEmpty)
                        .filter(searchFilter)
                        .groupBy(word => word)
                        .mapValues(_.size)

    wordsToCounts.foreach {
      tuple =>
        word.set(tuple._1)
        count.set(tuple._2)
        context.write(word, count)
    }
  }
}

object WordCountMap {
  val SEARCH_STRINGS_KEY = "searchValues"
}