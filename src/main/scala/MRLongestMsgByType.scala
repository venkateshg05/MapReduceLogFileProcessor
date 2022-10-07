import HelperUtils.Parameters
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.IOException
import java.util
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*

object MRLongestMsgByType:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      val line = value.toString.split(" ")
      val msgTime = line(0)
      val msgType = line(2)
      val logMsg = line(line.length - 1)

      val pattern = Pattern.compile(Parameters.injectedPattern)
      val matcher = pattern.matcher(logMsg)
      val foundPattern = matcher.find()

      val msgTypes = List("ERROR","INFO","WARN","DEBUG")

      if (msgTypes.contains(msgType) && foundPattern) {
        word.set(msgType)
        output.collect(word, new IntWritable(matcher.group(0).length))
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      def getMax(value1:IntWritable, value2:IntWritable):IntWritable = if value1.get() > value2.get() then value1 else value2
      val maxLen = values.asScala.reduceLeft(getMax)
      output.collect(key, maxLen)

  @main def runMRLongestMsgByType(inputPath: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRLongestMsgByType")

    conf.set("fs.defaultFS", "local")

    conf.set("mapreduce.job.maps", "2")
    conf.set("mapreduce.job.reduces", "1")

    conf.set("mapreduce.output.textoutputformat.separator", ",")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    JobClient.runJob(conf)
