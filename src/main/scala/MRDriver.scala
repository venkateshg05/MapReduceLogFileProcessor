import HelperUtils.Parameters
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import scala.jdk.CollectionConverters.*

object MRDriver {

  /*
    Functionality: Runs the four M/R jobs with the correct output path
   */

  def runMRFreqByTypeNTime(inputPath: String, outputPath: String, numOfMappers:String, numOfReducers:String) =
    /*
    Inputs: Takes the input & output paths, number of mappers & reducers
    Outputs: None
    Functionality: initializes the job, mappers & reducers, sets the I/O formats, runs the job
    * */


    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRFreqByTypeNTime")

    conf.set("mapreduce.job.maps", numOfMappers)
    conf.set("mapreduce.job.reduces", numOfReducers)

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

  def runMRFreqByType(inputPath: String, outputPath: String, numOfMappers:String, numOfReducers:String) =
    /*
        Inputs: Takes the input & output paths, number of mappers & reducers
        Outputs: None
        Functionality: initializes the job, mappers & reducers, sets the I/O formats, runs the job
        * */

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRFreqByType")

    conf.set("mapreduce.job.maps", numOfMappers)
    conf.set("mapreduce.job.reduces", numOfReducers)

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

  def runMRErrorFreqByTime(inputPath: String, outputPath: String, numOfMappers:String, numOfReducers:String) =
    /*
        Inputs: Takes the input & output paths, number of mappers & reducers
        Outputs: None
        Functionality: initializes the job, mappers & reducers, sets the I/O formats, runs the job
        * */

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRErrorFreqByTime")

    conf.set("mapreduce.job.maps", numOfMappers)
    conf.set("mapreduce.job.reduces", numOfReducers)

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

  def runMRLongestMsgByType(inputPath: String, outputPath: String, numOfMappers:String, numOfReducers:String) =
    /*
        Inputs: Takes the input & output paths, number of mappers & reducers
        Outputs: None
        Functionality: initializes the job, mappers & reducers, sets the I/O formats, runs the job
        * */

    val conf: JobConf = new JobConf(this.getClass)

    conf.setJobName("runMRLongestMsgByType")

    conf.set("mapreduce.job.maps", numOfMappers)
    conf.set("mapreduce.job.reduces", numOfReducers)

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
    /*
        Inputs: Takes the input & output paths, number of mappers & reducers
        Outputs: None
        Functionality: Runs the four M/R jobs with the correct output path
        * */
    runMRLongestMsgByType(inputPath, outputPath + "\\output_LongestMsg_by_MsgType", Parameters.numMappers, Parameters.numReducers)
    runMRFreqByTypeNTime(inputPath, outputPath + "\\output_freq_by_msgType_timeInterval", Parameters.numMappers, Parameters.numReducers)
    runMRFreqByType(inputPath, outputPath + "\\output_freq_by_msgType", Parameters.numMappers, Parameters.numReducers)
    runMRErrorFreqByTime(inputPath, outputPath + "\\output_freq_by_error_timeInterval", Parameters.numMappers, Parameters.numReducers)

}
