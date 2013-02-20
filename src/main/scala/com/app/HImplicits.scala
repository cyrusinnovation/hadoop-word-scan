package com.app

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.fs.Path

trait HImplicits {
  implicit def Text2String(t: Text): String = t.toString

  implicit def String2Text(s: String): Text = new Text(s)

  implicit def toLongWritable(l: Long): LongWritable = new LongWritable(l)

  implicit def LongWritable2Long(l: LongWritable): Long = l.get()

  implicit def intToIntWritable(l: Int) : IntWritable = new IntWritable(l)

  implicit def toPath(s: String): Path = new Path(s)
}
