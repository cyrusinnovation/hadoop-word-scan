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
    val conf: Configuration = new Configuration

    val input = util.Arrays.copyOfRange(args, 0, args.length - 1)
    val output = args(args.length - 1)

    val job: Job = new Job(conf)

    job.setJarByClass(WordCount.getClass)
    job.setMapperClass(classOf[WordCountMap])
    job.setReducerClass(classOf[WordCountReducer])
    job.setCombinerClass(classOf[WordCountReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    val outputPath: Path = new Path(output)

    FileInputFormat.addInputPath(job, new Path(input(0)))

    FileOutputFormat.setOutputPath(job, outputPath)
    outputPath.getFileSystem(conf).delete(outputPath, true)
    job.waitForCompletion(true)
  }

}
