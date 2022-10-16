import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

object MRFreqByType:
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
           Out: Count of 1 if for each message
           Functionality: Mapper to find the logs of type "ERROR","INFO","WARN","DEBUG"
           */

      val line: String = value.toString
      val msgTypes = List("ERROR","INFO","WARN","DEBUG")
      /*Split and filter for msg of one of 4 types*/
      line.split(" ")
        .filter(msgTypes.contains(_))
        .foreach { token =>
          word.set(token)
          output.collect(word, one)
        }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    /*Defines the reducer functionality*/
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      /*calculates the total for each msg type*/
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
