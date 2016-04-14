package com.bigfighsgames.spark.poc

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.scalatest._

import java.text.SimpleDateFormat
import java.util.Calendar

import scalax.file.Path
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.bigfishgames.spark.poc.AvroIO

object transformer {
    def transformAvro(row: (AvroKey[GenericRecord], NullWritable)): (AvroKey[GenericRecord], NullWritable) = {
    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema("""{                              
              "type": "record",                                            
              "name": "RPT_GT_EVENT_STREAM",                                         
              "fields": [      
              	      {"name": "test1", "type": ["null","string"]},                    
                      {"name": "test2", "type": ["null","long"]},  
                      {"name": "test3", "type": ["null","int"]} 
                      ] }"""))
    recordBuilder.set("test1", "hello")
    recordBuilder.set("test2", 999999999999L)
    recordBuilder.set("test3", 21)

    val record = recordBuilder.build

    (new AvroKey[GenericRecord](record), NullWritable.get)
  }
    
  def transformAvroWithJson(row: (AvroKey[GenericRecord], NullWritable)): (AvroKey[GenericRecord], NullWritable) = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    
    val key = row._1
    val test1 = key.datum().get("test1").asInstanceOf[String]
    val test2 = key.datum().get("test2").asInstanceOf[Long]
    val test3 = key.datum().get("test3").asInstanceOf[Long]
    
    val jsonMap = mapper.readValue(key.datum().get("data").asInstanceOf[String], classOf[Map[String, String]])
    
    val value = jsonMap.get("value").get.asInstanceOf[String]
    val tsClientDevice = jsonMap.get("ts_client_device").get.asInstanceOf[Integer]
    val eventName = jsonMap.get("event_name").get.asInstanceOf[String]
    
    val record = new GenericRecordBuilder(AvroIO.parseAvroSchema("""{                              
              "type": "record",                                            
              "name": "RPT_GT_EVENT_STREAM",                                         
              "fields": [      
              	      {"name": "test1", "type": ["null","string"]},                    
                      {"name": "test2", "type": ["null","long"]},  
                      {"name": "test3", "type": ["null","int"]},
                      {"name": "value", "type": ["null","string"]}, 
                      {"name": "tsClientDevice", "type": ["null","string"]},
                      {"name": "eventName", "type": ["null","string"]}, 
                      ] }"""))
                      .set("test1", test1)
                      .set("test2", test2)
                      .set("test3", test2)
                      .set("value", value)
                      .set("tsClientDevice", tsClientDevice)
                      .set("eventName", eventName)
                      .build
                      
    
    (new AvroKey[GenericRecord](record), new NullWritable()) 
  }
}

class ArvoIOTest extends FunSuite with Serializable with BeforeAndAfter {

  val schemaStr = """{                              
              "type": "record",                                            
              "name": "RPT_GT_EVENT_STREAM",                                         
              "fields": [      
              	      {"name": "test1", "type": ["null","string"]},                    
                      {"name": "test2", "type": ["null","long"]},  
                      {"name": "test3", "type": ["null","int"]} 
                      ] }"""

  private def currentDate = {
    val today = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(today)
  }


  //Create Spark Configuration and Context
  var sc: SparkContext = _
  before {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
      .setAppName("read avro")

    sc = new SparkContext(conf)
  }

  after {
    val path = Path.fromString("src/test/resources/" + currentDate)
    path.deleteRecursively(continueOnFailure = true)

    sc.stop
    System.clearProperty("spark.driver.port")
  }

  test("write Avro formatted data") {
    //Create test Avro data and RDD
    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(schemaStr))
    recordBuilder.set("test1", "test")
    recordBuilder.set("test2", 1234566L)
    recordBuilder.set("test3", 1)

    val record = recordBuilder.build

    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))

    //This is the method I'm testing
    AvroIO.dedupAvroHadoopOutputstream(avroRdd, schemaStr, "src/test/resources/")

    //Read the data written and verify 
    val rdd = sc.hadoopFile(
      "src/test/resources/" + currentDate,
      classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
      classOf[org.apache.hadoop.io.NullWritable])

    //only one row, so this is very contrived. 
    val actual = rdd.first()
    assert(actual._1.datum().get("test1").toString() == "test")
    assert(actual._1.datum().get("test2").toString() == "1234566")
    assert(actual._1.datum().get("test3").toString() == "1")
  }

  test("test function dedupAvroHadoopOutputstream") {
    //Create test Avro data and RDD
    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(schemaStr))
    recordBuilder.set("test1", "test")
    recordBuilder.set("test2", 1234566L)
    recordBuilder.set("test3", 1)

    val record1 = recordBuilder.build
    val record2 = recordBuilder.build

    //this create two tuples (aka records) with the same data
    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record1), NullWritable.get), (new AvroKey[GenericRecord](record1), NullWritable.get)))

    //This function should de-dup the two records and only return one record
    AvroIO.dedupAvroHadoopOutputstream(avroRdd, schemaStr, "src/test/resources/")

    //Read the data written and verify 
    val rdd = sc.hadoopFile(
      "src/test/resources/" + currentDate,
      classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
      classOf[org.apache.hadoop.io.NullWritable])

    assert(rdd.count() == 1)
    val actual = rdd.first()
    assert(actual._1.datum().get("test1").toString() == "test")
    assert(actual._1.datum().get("test2").toString() == "1234566")
    assert(actual._1.datum().get("test3").toString() == "1")
  }

  test("transform avro") {
    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(schemaStr))
    recordBuilder.set("test1", "test")
    recordBuilder.set("test2", 1234566L)
    recordBuilder.set("test3", 1)

    val record = recordBuilder.build
    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))

    //This is the function I'm testing
    AvroIO.transformAvroHadoopOutputStream(avroRdd, schemaStr, "src/test/resources/", transformer.transformAvro)
    
    //Read the data written and verify 
    val rdd = sc.hadoopFile(
      "src/test/resources/" + currentDate,
      classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
      classOf[org.apache.hadoop.io.NullWritable])

    //only one row, so this is very contrived. 
    val actual = rdd.first()
    assert(actual._1.datum().get("test1").toString() == "hello")
    assert(actual._1.datum().get("test2").toString() == "999999999999")
    assert(actual._1.datum().get("test3").toString() == "21")
  }
  
  test("transform avro include json") {
    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(schemaStr))
    recordBuilder.set("test1", "{character_ids: 7865307,text: \"test\",session_id: \"177372818\",buy_in_id: \"152\",table_id: \"27773\"}")
    recordBuilder.set("test2", 987654321L)
    recordBuilder.set("test3", 10)

    val record = recordBuilder.build
    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))
    
    //This is the function I'm testing
    AvroIO.transformAvroHadoopOutputStream(avroRdd, schemaStr, "src/test/resources/", transformer.transformAvroWithJson)

  }
}