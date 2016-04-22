package com.bigfishgames.spark.poc

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext


object AvroDeDup {

  val outputSchemaStr = """{                              
              "type": "record",                                            
              "name": "RPT_GT_EVENT_STREAM",                                         
              "fields": [      
              	      {"name": "evtt", "type": ["null","string"]},                    
                      {"name": "tss", "type": ["null","long"]},  
                      {"name": "tsc", "type": ["null","long"]},  
                      {"name": "tzo", "type": ["null","int"]},   
                      {"name": "bfg", "type": ["null","string"]},           
                      {"name": "rid", "type": ["null","string"]},  
                      {"name": "chid", "type": ["null","long"]},   
                      {"name": "plt", "type": ["null","string"]},     
                      {"name": "ver", "type": ["null","string"]},   
                      {"name": "ip", "type": ["null","string"]},     
                      {"name": "bld", "type": ["null","string"]},     
                      {"name": "snid", "type": ["null","string"]},     
                      {"name": "psnid", "type": ["null","string"]},     
                      {"name": "snn", "type": ["null","int"]},                    
                      {"name": "data", "type":{"type":"string","java-class":"com.bigfishgames.biginsights.serialization.GtData"}}, 
                      {"name": "headers", "type": ["null","string"], "default": null}, 
                      {"name": "tsl", "type": ["null","long"], "default": null} 
                      ] }"""

  val logger = Logger.getLogger(this.getClass.getName)

  private def createStreamingContext = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
      .setAppName("avro de-dup")

    new StreamingContext(conf, Seconds(30))
  }
  
  //TODO: https://github.com/scopt/scopt  scopt is a command line parser for scala
  def main(args: Array[String]) {
    val ssc = createStreamingContext
    
    val inputDirectory = "hdfs://bi-mgmt02.dev.bigfishgames.com:8020/bfg/flume-gt-events/gt-writer01.int.bigfishgames.com/valid/prod/test.int10/"
    val avroStream = AvroIO.readAvroStream(ssc, inputDirectory, "/staging/")

    val outputDirectory = "hdfs://bi-mgmt02.dev.bigfishgames.com:8020/bfg/flume-gt-events/gt-writer01.int.bigfishgames.com/valid/prod/test.int10/"
    
    avroStream.foreachRDD(rdd => {
      if (!rdd.partitions.isEmpty) {
        logger.info("Writing dedup RDD { " + rdd.toString() + " }")
        AvroIO.dedupAvroHadoopOutputstream(rdd, outputSchemaStr, outputDirectory)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}