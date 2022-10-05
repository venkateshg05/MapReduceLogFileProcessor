import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.time.LocalTime
import java.util
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

object MRFreqByTime:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val word = new Text()

    @tailrec
    def getTimeInterval(
       startTime: LocalTime,
       endTime: LocalTime,
       lowerBound: LocalTime,
       upperBound: LocalTime,
       time: LocalTime,
       interval: Int
     ): String = {
      // print(lowerBound, time, upperBound, interval)
      // println()
      if (time.compareTo(endTime) > -1) "Terminate"
      else {
        val gtLowerBound = time.compareTo(lowerBound) == 1
        val ltUpperBound = time.compareTo(upperBound) == -1
        if ((gtLowerBound && ltUpperBound))
          lowerBound.toString + ", " + upperBound.toString
        else
          getTimeInterval(
            startTime, endTime, upperBound.plusMinutes(1), upperBound.plusMinutes(interval), time, interval
          )
      }
    }

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val msgTime:String = line.substring(0,5)
      val msgType:String = line.split(" ")(2)
      word.set(msgType + ", " + msgTime)
      output.collect(word, one)

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


  @main def runMRFreqByTime(inputPath: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRFreqByTime")

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