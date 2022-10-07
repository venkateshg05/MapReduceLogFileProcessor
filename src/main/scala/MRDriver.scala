import HelperUtils.Parameters
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import scala.jdk.CollectionConverters.*

object MRDriver {

  def runMRFreqByTypeNTime(inputPath: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRFreqByTypeNTime")

    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "8")
    conf.set("mapreduce.job.reduces", "1")

    conf.set("mapreduce.output.textoutputformat.separator", ",")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[MRFreqByMsgTypeNTime.Map])
    conf.setCombinerClass(classOf[MRFreqByMsgTypeNTime.Reduce])
    conf.setReducerClass(classOf[MRFreqByMsgTypeNTime.Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    JobClient.runJob(conf)

  def runMRFreqByType(inputPath: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRFreqByType")

    conf.set("fs.defaultFS", "local")

    conf.set("mapreduce.job.maps", "2")
    conf.set("mapreduce.job.reduces", "1")

    conf.set("mapreduce.output.textoutputformat.separator", ",")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[MRFreqByType.Map])
    conf.setCombinerClass(classOf[MRFreqByType.Reduce])
    conf.setReducerClass(classOf[MRFreqByType.Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    JobClient.runJob(conf)

  def runMRErrorFreqByTime(inputPath: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRErrorFreqByTime")

    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "8")
    conf.set("mapreduce.job.reduces", "1")

    conf.set("mapreduce.output.textoutputformat.separator", ",")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[MRErrorFreqByTime.Map])
    conf.setCombinerClass(classOf[MRErrorFreqByTime.Reduce])
    conf.setReducerClass(classOf[MRErrorFreqByTime.Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    JobClient.runJob(conf)

  def runMRLongestMsgByType(inputPath: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRLongestMsgByType")

    conf.set("fs.defaultFS", "local")

    conf.set("mapreduce.job.maps", "2")
    conf.set("mapreduce.job.reduces", "1")

    conf.set("mapreduce.output.textoutputformat.separator", ",")

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[MRLongestMsgByType.Map])
    conf.setCombinerClass(classOf[MRLongestMsgByType.Reduce])
    conf.setReducerClass(classOf[MRLongestMsgByType.Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    JobClient.runJob(conf)

  @main def runMRProcesses(inputPath: String, outputPath: String) =
    runMRLongestMsgByType(inputPath, outputPath + "\\output_LongestMsg_by_MsgType")
    runMRFreqByTypeNTime(inputPath, outputPath + "\\output_freq_by_msgType_timeInterval")
    runMRFreqByType(inputPath, outputPath + "\\output_freq_by_msgType")
    runMRErrorFreqByTime(inputPath, outputPath + "\\output_freq_by_error_timeInterval")

}
