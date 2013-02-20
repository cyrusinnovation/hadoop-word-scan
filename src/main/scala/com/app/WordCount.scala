package com.app

import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object WordCount {

  def main (args: Array[String]) {
    val input =  args(0)
    val output = args(1)

    val searchStrings = args.drop(2)
    val conf: Configuration = new Configuration
    conf.setStrings(WordCountMap.SEARCH_STRINGS_KEY, searchStrings:_*)

    val job: Job = new Job(conf)

    job.setJarByClass(WordCount.getClass)
    job.setMapperClass(classOf[WordCountMap])
    job.setReducerClass(classOf[WordCountReducer])
    job.setCombinerClass(classOf[WordCountReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    val outputPath: Path = new Path(output)

    FileInputFormat.addInputPath(job, new Path(input))

    FileOutputFormat.setOutputPath(job, outputPath)
    outputPath.getFileSystem(conf).delete(outputPath, true)
    job.waitForCompletion(true)
  }

}
