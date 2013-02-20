package com.app

import scala.collection.JavaConversions._
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class WordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  val result = new IntWritable()

  type ReducerContext = Reducer[Text,IntWritable,Text,IntWritable]#Context

  override def reduce (key: Text, values: java.lang.Iterable[IntWritable], context: ReducerContext) {
    result.set(values.foldLeft(0) {_ + _.get()})
    context.write(key, result)
  }

}
