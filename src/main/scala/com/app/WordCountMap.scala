package com.app

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.{IntWritable, Text, LongWritable}


class WordCountMap extends Mapper[LongWritable, Text, Text, IntWritable] {

  private val word:Text = new Text()
  private val count:IntWritable = new IntWritable()

  type MapperContext = Mapper[LongWritable, Text, Text, IntWritable]#Context

  override def map(key: LongWritable, value: Text, context: MapperContext):Unit = {
    def wordsToCounts = value.toString.split(" ").groupBy(word => word).mapValues(_.size)

    wordsToCounts.foreach{ tuple =>
      word.set(tuple._1)
      count.set(tuple._2)
      context.write(word, count)
    }
  }

}
