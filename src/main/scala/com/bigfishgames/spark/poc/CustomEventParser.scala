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
                      {"name": "tss", "type": ["null","long"]} ,  
                      {"name": "tsc", "type": ["null","long"]}, 
                      {"name": "tzo", "type": ["null","int"]} , 
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
                      {"name": "ts_client_device", "type": ["null","long"]} , 
                      {"name": "derived", "type": ["null", "long"]},
                      {"name": "event_name", "type": ["null","string"]}
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
    logger.info("transforming ........................................")
    //data which is not changed
    val key = row._1
    val evtt = key.datum().get("evtt")
    val tss = key.datum().get("tss")
    val tsc = key.datum().get("tsc")
    val tzo = key.datum().get("tzo")
    val bfg = key.datum().get("bfg")
    val rid = key.datum().get("rid")
    val chid = key.datum().get("chid")
    val plt = key.datum().get("plt")
    val ver = key.datum().get("ver")
    val ip = key.datum().get("ip")
    val bld = key.datum().get("bld")
    val snid = key.datum().get("snid")
    val psnid = key.datum().get("psnid")
    val snn = key.datum().get("snn")

    //extract fields from json string and promote to avro fields
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    logger.info("Parsing Json ..................................")
    val jsonMap = mapper.readValue(key.datum().get("data").toString(), classOf[Map[String, String]])

    val value = jsonMap.get("value").getOrElse("")
    val tsClientDevice = jsonMap.get("ts_client_device").getOrElse(0L)
    val eventName = jsonMap.get("event_name").getOrElse("")
    val derived = new java.lang.Long(tsClientDevice.toString) * 10

    logger.info("Building new Avro Generic Record ..............................")
    val record = new GenericRecordBuilder(AvroIO.parseAvroSchema(outputSchemaStr))
      .set("evtt", evtt)
      .set("tss", castToLong(tss))
      .set("tsc", castToLong(tsc))
      .set("tzo", tzo)
      .set("bfg", bfg)
      .set("rid", rid)
      .set("chid",castToLong(chid))
      .set("plt", plt)
      .set("ver", ver)
      .set("ip", ip)
      .set("bld", bld)
      .set("snid", snid)
      .set("psnid", psnid)
      .set("snn", snn)
      .set("value", value)
      .set("ts_client_device", castToLong(tsClientDevice))
      .set("event_name", eventName)
      .set("derived", derived)
      .build()

    (new AvroKey[GenericRecord](record),  NullWritable.get)
 
  }

  private def castToLong(value: Any) = {
    var st: java.lang.Long = 0L

    logger.info("Casting to Java Long ................")
    if (value.isInstanceOf[Long]) {
      st = new java.lang.Long(value.asInstanceOf[Long])
    } else if (value.isInstanceOf[java.math.BigInteger]) {
      //If we want a Big Integer, change the avro schema to use java.math.BigInteger and cast to that instead of Long
      throw new Exception("BigInteger is not supported. Long is supported")
    } else if (value.isInstanceOf[Int]) {
      st = new java.lang.Long(value.asInstanceOf[Int].intValue())
    } else if (value.isInstanceOf[String]) {
      st = new java.lang.Long(value.toString())
    } else {
      throw new Exception("Type not supported " + value)
      
    }
    st
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