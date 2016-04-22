package com.bigfighsgames.spark.poc

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.scalatest._
import org.codehaus.jackson.map.annotate._

import java.text.SimpleDateFormat
import java.util.Calendar
import java.math.BigInteger

import scalax.file.Path
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.bigfishgames.spark.poc._

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

//object transformer {

//  def castToLong(value: Any) = {
//    var st: java.lang.Long = 0L
//
//    if (value.isInstanceOf[Long]) {
//      st = new java.lang.Long(value.asInstanceOf[Long])
//      print("Long")
//    } else if (value.isInstanceOf[java.math.BigInteger]) {
//      print("BigInteger")
//      //If we want a Big Integer, change the avro schema to use java.math.BigInteger and cast to that instead of Long
//      throw new Exception("BigInteger is not supported. Long is supported")
//    } else {
//      st = new java.lang.Long(value.asInstanceOf[Int].intValue())
//      print("Int")
//    }
//    st
//  }

//  def transformAvro(row: (AvroKey[GenericRecord], NullWritable)): (AvroKey[GenericRecord], NullWritable) = {
//    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema("""{                              
//              "type": "record",                                            
//              "name": "RPT_GT_EVENT_STREAM",                                         
//              "fields": [      
//              	      {"name": "test1", "type": ["null","string"]},                    
//                      {"name": "test2", "type": ["null","long"]},  
//                      {"name": "test3", "type": ["null","int"]} 
//                      ] }"""))
//    recordBuilder.set("test1", "hello")
//    recordBuilder.set("test2", 999999999999L)
//    recordBuilder.set("test3", 21)
//
//    val record = recordBuilder.build
//
//    (new AvroKey[GenericRecord](record), NullWritable.get)
//
//  }

//  def transformAvroWithJson(row: (AvroKey[GenericRecord], NullWritable)): (AvroKey[GenericRecord], NullWritable) = {
//
//    val key = row._1
//    val test1 = key.datum().get("test1")
//    val test2 = key.datum().get("test2")
//    val test3 = key.datum().get("test3")
//
//    val mapper = new ObjectMapper()
//    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
//    mapper.registerModule(DefaultScalaModule)
//
//    val jsonMap = mapper.readValue(key.datum().get("test1").toString(), classOf[Map[String, String]])
//
//    //using getOrElse because Avro cannot handle Some from the Option trait
//    val text = jsonMap.get("text").getOrElse("")
//    val sessionId = jsonMap.get("session_id").getOrElse("")
//    val characterId = jsonMap.get("character_ids").getOrElse(0)
//    val buyInId = jsonMap.get("buy_in_id").getOrElse("")
//    val tableId = jsonMap.get("table_id").getOrElse("")
//
//    val record = new GenericRecordBuilder(AvroIO.parseAvroSchema("""{                              
//              "type": "record",                                            
//              "name": "RPT_GT_EVENT_STREAM",                                         
//              "fields": [      
//              	      {"name": "test1", "type": ["null","string"]},                    
//                      {"name": "test2", "type": ["null","long"]},  
//                      {"name": "test3", "type": ["null","int"]},
//                      {"name": "text", "type": ["null","string"]},
//                      {"name": "sessionId", "type": ["null","string"]},
//                      {"name": "characterId", "type": ["null","long"]},
//                      {"name": "buyInId", "type": ["null","string"]},
//                      {"name": "tableId", "type": ["null","string"]}
//                      ] }"""))
//      .set("test1", test1)
//      .set("test2", test2)
//      .set("test3", test3)
//      .set("text", text)
//      .set("sessionId", sessionId)
//      .set("characterId", castToLong(characterId))
//      .set("buyInId", buyInId)
//      .set("tableId", tableId)
//      .build
//
//    (new AvroKey[GenericRecord](record), NullWritable.get)
//  }
//}

class ArvoIOStreamingTest extends FunSuite with Serializable with BeforeAndAfter {

//  val schemaStr = """{                              
//              "type": "record",                                            
//              "name": "RPT_GT_EVENT_STREAM",                                         
//              "fields": [      
//              	      {"name": "test1", "type": ["null","string"]},                    
//                      {"name": "test2", "type": ["null","long"]},  
//                      {"name": "test3", "type": ["null","int"]} 
//                      ] }"""
//
  private def currentDate = {
    val today = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(today)
  }
  
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


  //Create Spark Configuration and Context
  var ssc: StreamingContext = _
  before {

//    val path = Path.fromString("src/test/resources/" + currentDate)
//    path.deleteRecursively(continueOnFailure = true)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
      .setAppName("avro stream test")
      .setMaster("local[1]")

    ssc = new StreamingContext(conf, Seconds(30))
    
  }

  after {
//    val path = Path.fromString("src/test/resources/" + currentDate)
//    path.deleteRecursively(continueOnFailure = true)

   
    ssc.stop()
    System.clearProperty("spark.driver.port")
  }

  
  test("AvroReadStream Test") { 
    
    val avroStream  = AvroIO.readAvroStream(ssc, "src/test/resources/input/", "/staging")
    assert(avroStream != null)    
  }
  
  //src/test/resources/input/2016-04-22/staging/
//  test("de-dup avro stream test") {
//    //need to move file to this directory
//     val avroStream  = AvroIO.readAvroStream(ssc, "src/test/resources/input/", "/staging/")
//     val pathSource = Path.fromString("src/test/resources/input/gt_events.1461255995576.avro")
//     val pathTarget = Path.fromString("src/test/resources/input/" + currentDate + "/staging/gt_events.1461255995576.avro")
//     pathSource.copyTo(pathTarget)
//     avroStream.foreachRDD(rdd => {
//      if (!rdd.partitions.isEmpty) {
//        AvroIO.dedupAvroHadoopOutputstream(rdd, outputSchemaStr, "src/test/resources/output/")
//      }
//    })
//  }

}