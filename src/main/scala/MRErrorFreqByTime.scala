import HelperUtils.{Parameters, GetTimeInterval, CheckRegexPattern}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import java.io.IOException
import java.time.{Duration, LocalTime}
import java.util
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*

object MRErrorFreqByTime:
  /*
  Functionality: Runs the M/R job to calculate the distribution of errors with injected string
  */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :

    /*Defines the mapper functionality*/

//    To store the count (output value)
    private final val one = new IntWritable(1)
//    To store the output key
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      /*
      In: Takes in one line of log
      Out: Count of 1 if the error message has injected pattern
      Functionality: Mapper to find the time interval for ERROR msgs that have the injected string
      */

      /*Split the log line & get time, message type & message*/
      val line = value.toString.split(" ")
      val msgTime = line(0)
      val msgType = line(2)
      val logMsg = line(line.length - 1)

      /*Check if the log message has injected pattern*/
      val msgHasPattern = CheckRegexPattern.checkPattern(logMsg) //checkPattern(logMsg)

      /*If message type is error & has pattern*/
      if (msgType == "ERROR" && msgHasPattern) {
        val msgTimeStamp = LocalTime.parse(msgTime)
        val startTime = LocalTime.parse(Parameters.startTime)
        val interval = Parameters.timeInterval
        val endTime = LocalTime.parse(Parameters.endTime)
        val totalDuration = Duration.between(startTime, endTime).toMinutes()
        /*Find the time interval for the message*/
        val timeInterval = GetTimeInterval.getTimeInterval(startTime, 0, totalDuration.toInt / interval, msgTimeStamp, interval)
        word.set(msgType + ", " + timeInterval)
        /*Write the time interval (key) and count of one (value)*/
        output.collect(word, one)
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    /*Defines the reducer functionality*/
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    /*calculates the total ERROR msg with injected pattern for each time interval*/
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
