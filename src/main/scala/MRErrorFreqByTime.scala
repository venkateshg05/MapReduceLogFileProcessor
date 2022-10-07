import HelperUtils.{Parameters, GetTimeInterval}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import java.io.IOException
import java.time.{Duration, LocalTime}
import java.util
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*

object MRErrorFreqByTime:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      val line = value.toString.split(" ")
      val msgTime = line(0)
      val msgType = line(2)
      val logMsg = line(line.length - 1)

      val pattern = Pattern.compile(Parameters.injectedPattern)
      val msgHasPattern = pattern.matcher(logMsg).find()

      if (msgType == "ERROR" && msgHasPattern) {
        val msgTimeStamp = LocalTime.parse(msgTime)
        val startTime = LocalTime.parse(Parameters.startTime)
        val interval = Parameters.timeInterval
        val endTime = LocalTime.parse(Parameters.endTime)
        val totalDuration = Duration.between(startTime, endTime).toMinutes()
        val timeInterval = GetTimeInterval.getTimeInterval(startTime, 0, totalDuration.toInt / interval, msgTimeStamp, interval)
        word.set(msgType + ", " + timeInterval)
        output.collect(word, one)
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


  @main def runMRErrorFreqByTime(inputPath: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRErrorFreqByTime")

    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "8")
    conf.set("mapreduce.job.reduces", "1")

    conf.set("mapred.textoutputformat.separator", ",")

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
