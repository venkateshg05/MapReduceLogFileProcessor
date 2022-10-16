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
  /*
  Functionality: Runs the M/R job to calculate the longest msg within each msg type with injected string
  */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    /*Defines the mapper functionality*/

    //    To store the output key
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

      /*
                 In: Takes in one line of log
                 Out: Count of 1 if for each message with injected pattern
                 Functionality: Mapper to find the max length msg in logs of type "ERROR","INFO","WARN","DEBUG" with injected pattern
                 */

      /*Split the log line & get message type & message*/
      val line = value.toString.split(" ")
      val msgType = line(2)
      val logMsg = line(line.length - 1)

      /*Check if the log message has injected pattern*/
      val pattern = Pattern.compile(Parameters.injectedPattern)
      val matcher = pattern.matcher(logMsg)
      val foundPattern = matcher.find()

      val msgTypes = List("ERROR","INFO","WARN","DEBUG")

      /*If message has pattern*/
      if (msgTypes.contains(msgType) && foundPattern) {
        word.set(msgType)
        output.collect(word, new IntWritable(matcher.group(0).length))
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    /*Defines the reducer functionality*/
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      /*calculates the max len of msg with injected pattern*/
      def getMax(value1:IntWritable, value2:IntWritable):IntWritable = if value1.get() > value2.get() then value1 else value2
      val maxLen = values.asScala.reduceLeft(getMax)
      output.collect(key, maxLen)
