package com.bigfishgames.spark.poc

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature


object CustomEventParser {
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
                      {"name": "value", "type": ["null","string"]},
                      {"name": "ts_client_device", "type": ["null","int"]},
                      {"name": "event_name, "type": ["null","string"]}
                      ] }"""

  val logger = Logger.getLogger(this.getClass.getName)

  private def createStreamingContext = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
      .setAppName("read avro")

    new StreamingContext(conf, Seconds(10))
  }

  def transformLobbyEvent(row: (AvroKey[GenericRecord], NullWritable)): (AvroKey[GenericRecord], NullWritable) = {
    //data which is not changed
    val key = row._1
    val evtt = key.datum().get("evtt").asInstanceOf[String]
    val tss = key.datum().get("tss").asInstanceOf[Long]
    val tsc = key.datum().get("tsc").asInstanceOf[Long]
    val tzo = key.datum().get("tzo").asInstanceOf[Int]
    val bfg = key.datum().get("bfg").asInstanceOf[String]
    val rid = key.datum().get("rid").asInstanceOf[String]
    val chid = key.datum().get("chid").asInstanceOf[Long]
    val plt = key.datum().get("plt").asInstanceOf[String]
    val ver = key.datum().get("ver").asInstanceOf[String]
    val ip = key.datum().get("ip").asInstanceOf[String]
    val bld = key.datum().get("bld").asInstanceOf[String]
    val snid = key.datum().get("snid").asInstanceOf[String]
    val psnid = key.datum().get("psnid").asInstanceOf[String]
    val snn = key.datum().get("snn").asInstanceOf[Int]

    //extract fields from json string and promote to avro fields
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    val jsonMap = mapper.readValue(key.datum().get("data").asInstanceOf[String], classOf[Map[String, String]])

    val value = jsonMap.get("value").get.asInstanceOf[String]
    val tsClientDevice = jsonMap.get("ts_client_device").get.asInstanceOf[Integer]
    val eventName = jsonMap.get("event_name").get.asInstanceOf[String]

    val record = new GenericRecordBuilder(AvroIO.parseAvroSchema(outputSchemaStr))
                                          .set("evtt", evtt)
                                          .set("tss", tss)
                                          .set("tsc", tsc)
                                          .set("tzo", tzo)
                                          .set("bfg", bfg)
                                          .set("rid", rid)
                                          .set("chid", chid)
                                          .set("plt", plt)
                                          .set("ver", ver)
                                          .set("ip", ip)
                                          .set("bld", bld)
                                          .set("snid", snid)
                                          .set("psnid", psnid)
                                          .set("snn", snn)
                                          .set("value", value)
                                          .set("ts_client_device", tsClientDevice)
                                          .set("event_name", eventName)
                                          .build()
                                     

    (new AvroKey[GenericRecord](record), new NullWritable())
  }

  def main(args: Array[String]) {
    val ssc = createStreamingContext

    val inputDirectory = "hdfs://bi-mgmt02.dev.bigfishgames.com:8020/bfg/flume-gt-events/gt-writer01.int.bigfishgames.com/valid/prod/test.int10/"
    val avroStream = AvroIO.readAvroStream(ssc, inputDirectory)

    val outputDirectory = "hdfs://bi-mgmt02.dev.bigfishgames.com:8020/custom_event/test.int10"
    avroStream.foreachRDD(rdd => {
      if (!rdd.partitions.isEmpty) {
        logger.info("Writing custom event RDD { " + rdd.toString() + " }")
        AvroIO.transformAvroHadoopOutputStream(rdd, outputSchemaStr, outputDirectory, transformLobbyEvent)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}