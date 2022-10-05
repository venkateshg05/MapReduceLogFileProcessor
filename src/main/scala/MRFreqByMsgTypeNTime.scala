import HelperUtils.Parameters
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.time.{Duration, LocalTime}
import java.util
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

object MRFreqByMsgTypeNTime:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val word = new Text()

    @tailrec
    private def getTimeInterval(startTime: LocalTime, lowerBound: Int, upperBound: Int, time: LocalTime, interval: Int): String = {

      // calculate the mid index
      val sum = lowerBound + upperBound
      val mid = if sum % 2 == 0 then (sum) / 2 else (sum / 2) + 1

      // calculate the actual time interval for the begin, mid & end indices
      val intervalBegin = startTime.plusMinutes(lowerBound * interval)
      val intervalEnd = startTime.plusMinutes(upperBound * interval)
      val intervalMid = startTime.plusMinutes(mid * interval)

      // Terminating condition - log time lies in previous interval [begin, end]
      if (mid >= upperBound)
        intervalBegin.plusMinutes(1).toString + ", " + intervalEnd.toString
      else {
        // Terminating condition - log time lies in interval end boundaries [begin, end]
        if (time.compareTo(intervalEnd) == 0) {
          intervalEnd.minusMinutes(interval - 1).toString + ", " + intervalEnd.toString
        }
        else if (time.compareTo(intervalMid) == 0) {
          intervalMid.minusMinutes(interval - 1).toString + ", " + intervalMid.toString
        }
        // smaller interval possible, continue binary search
        else if (time.isAfter(intervalBegin) && time.isBefore(intervalMid)) {
          getTimeInterval(startTime, lowerBound, mid, time, interval)
        } else {
          getTimeInterval(startTime, mid, upperBound, time, interval)
        }
      }
    }

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val msgTime:String = line.substring(0,5)
      val msgType:String = line.split(" ")(2)

      val msgTimeStamp = LocalTime.parse(msgTime)
      val startTime = LocalTime.parse(Parameters.startTime)
      val interval = Parameters.timeInterval
      val endTime = LocalTime.parse(Parameters.endTime)
      val totalDuration = Duration.between(startTime, endTime).toMinutes()
      val timeInterval = getTimeInterval(startTime, 0, totalDuration.toInt / interval, msgTimeStamp, interval)
      word.set(msgType + ", " + timeInterval)
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