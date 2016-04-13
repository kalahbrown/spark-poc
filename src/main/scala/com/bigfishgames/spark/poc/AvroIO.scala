package com.bigfishgames.spark.poc

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext._

//https://github.com/massie/adam/blob/master/adam-commands/src/main/scala/edu/berkeley/cs/amplab/adam/serialization/AdamKryoRegistrator.scala
//
//You need to register all of you Avro SpecificClasses with Kryo and have it
//use the AvroSerializer class to encode/decode them.
//> The problem is that spark uses java serialization requiring serialized
//> objects to implement Serializable, AvroKey doesn't.
//https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/
//https://blogs.msdn.microsoft.com/bigdatasupport/2015/09/14/understanding-sparks-sparkconf-sparkcontext-sqlcontext-and-hivecontext/

//TODO: This writes a single file to disk, but the path is /gt_event/part-r-00000.avro, this is because it is writing a Hadoop File and using the Avro MR libs
//  the next area to explore is write Object, can this be done to just serialize an RDD.
//TODO: Add the ability to read schema from HDFS

object AvroIO {

  val logger = Logger.getLogger(this.getClass.getName)

  //TODO: read schema from hdfs
  def parseAvroSchema(schemaStr: String) = {
    val parse = new Schema.Parser();
    parse.parse(schemaStr)
  }

  def readAvroStream(ssc: StreamingContext, inputDirectory: String, stagingDir: String = "") = {
    ssc.fileStream[AvroKey[GenericRecord], NullWritable, AvroKeyInputFormat[GenericRecord]](inputDirectory + currentDate + stagingDir)
  }

  def dedupAvroHadoopOutputstream(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)], schemaStr: String, outputDirectory: String) = {
    logger.info("WRITING DE-DUPPED DATA TO " + outputDirectory + currentDate)
    avroRdd.reduceByKey((key, value) => key)
      .saveAsNewAPIHadoopFile(outputDirectory + currentDate, classOf[AvroKey[GenericRecord]], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]],
        createAvroJob(schemaStr).getConfiguration)
  }

  /*
   * read in one avro schema and write to another "tranformed" avro schema
   */
  def transformAvroHadoopOutputStream(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)], schemaStr: String, outputDirectory: String, transform: ((AvroKey[GenericRecord], NullWritable)) => (AvroKey[GenericRecord], NullWritable)) = {
    logger.info("WRITING AVRO TRANSFORMED DATA TO " + outputDirectory + currentDate)
    avroRdd.map(row => (transform(row))).saveAsNewAPIHadoopFile(outputDirectory + currentDate, classOf[AvroKey[GenericRecord]], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]], createAvroJob(schemaStr).getConfiguration)
  }

  private def createAvroJob(schemaStr: String) = {
    val conf = new Configuration()
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true)
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
    conf.set("avro.mo.config.namedOutput", currentTime)
    val job = Job.getInstance(conf)
    AvroJob.setOutputKeySchema(job, parseAvroSchema(schemaStr))
    job
  }

  private def currentDate = {
    val today = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(today)
  }

  private def currentTime = {
    val now = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-H.m.s.S")
    dateFormat.format(now)
  }

}