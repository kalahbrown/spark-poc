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
import com.bigfishgames.spark.poc.AvroIO

object transformer {

  def castToLong(value: Any) = {
    var st: java.lang.Long = 0L

    if (value.isInstanceOf[Long]) {
      st = new java.lang.Long(value.asInstanceOf[Long])
      print("Long")
    } else if (value.isInstanceOf[java.math.BigInteger]) {
      print("BigInteger")
      //If we want a Big Integer, change the avro schema to use java.math.BigInteger and cast to that instead of Long
      throw new Exception("BigInteger is not supported. Long is supported")
    } else {
      st = new java.lang.Long(value.asInstanceOf[Int].intValue()) 
      print("Int")
    }
    st
  }

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

    val key = row._1
    val test1 = key.datum().get("test1")
    val test2 = key.datum().get("test2")
    val test3 = key.datum().get("test3")

    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    val jsonMap = mapper.readValue(key.datum().get("test1").toString(), classOf[Map[String, String]])

    //using getOrElse because Avro cannot handle Some from the Option trait
    val text = jsonMap.get("text").getOrElse("")
    val sessionId = jsonMap.get("session_id").getOrElse("")

    val characterId = jsonMap.get("character_ids").getOrElse(0)

    val buyInId = jsonMap.get("buy_in_id").getOrElse("")
    val tableId = jsonMap.get("table_id").getOrElse("")

    val record = new GenericRecordBuilder(AvroIO.parseAvroSchema("""{                              
              "type": "record",                                            
              "name": "RPT_GT_EVENT_STREAM",                                         
              "fields": [      
              	      {"name": "test1", "type": ["null","string"]},                    
                      {"name": "test2", "type": ["null","long"]},  
                      {"name": "test3", "type": ["null","int"]},
                      {"name": "text", "type": ["null","string"]},
                      {"name": "sessionId", "type": ["null","string"]},
                      {"name": "characterId", "type": ["null","long"]},
                      {"name": "buyInId", "type": ["null","string"]},
                      {"name": "tableId", "type": ["null","string"]}
                      ] }"""))
      .set("test1", test1)
      .set("test2", test2)
      .set("test3", test3)
      .set("text", text)
      .set("sessionId", sessionId)
      .set("characterId", castToLong(characterId))
      .set("buyInId", buyInId)
      .set("tableId", tableId)
      .build

    (new AvroKey[GenericRecord](record), NullWritable.get)
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

    val path = Path.fromString("src/test/resources/" + currentDate)
    path.deleteRecursively(continueOnFailure = true)

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

  //math.BigInteger???? test for that
  test("transform avro include json") {
    val recordBuilder = new GenericRecordBuilder(AvroIO.parseAvroSchema(schemaStr))
    recordBuilder.set("test1", "{\"character_ids\": 92 , \"text\": \"test\", \"session_id\": \"177372818\", \"buy_in_id\": \"152\", \"table_id\": \"27773\"}")

    recordBuilder.set("test2", 987654321L)
    recordBuilder.set("test3", 10)

    val record = recordBuilder.build
    val avroRdd = sc.parallelize(Seq((new AvroKey[GenericRecord](record), NullWritable.get)))

    //This is the function I'm testing
    AvroIO.transformAvroHadoopOutputStream(avroRdd, """{                              
              "type": "record",                                            
              "name": "RPT_GT_EVENT_STREAM",                                         
              "fields": [      
              	      {"name": "test1", "type": ["null","string"]},                    
                      {"name": "test2", "type": ["null","long"]},  
                      {"name": "test3", "type": ["null","int"]},
                      {"name": "text", "type": ["null","string"]},
                      {"name": "sessionId", "type": ["null","string"]},
                      {"name": "characterId", "type": ["null","long"]},
                      {"name": "buyInId", "type": ["null","string"]},
                      {"name": "tableId", "type": ["null","string"]}
                      ] }""", "src/test/resources/", transformer.transformAvroWithJson)

    //Read the data written and verify 
    val rdd = sc.hadoopFile(
      "src/test/resources/" + currentDate,
      classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
      classOf[org.apache.hadoop.io.NullWritable])

    //only one row, so this is very contrived. 
    val actual = rdd.first()
    assert(actual._1.datum().get("test1").toString() == "{\"character_ids\": 92 , \"text\": \"test\", \"session_id\": \"177372818\", \"buy_in_id\": \"152\", \"table_id\": \"27773\"}")
    assert(actual._1.datum().get("test2").toString() == "987654321")
    assert(actual._1.datum().get("test3").toString() == "10")

    assert(actual._1.datum().get("characterId").toString() == "92")
    assert(actual._1.datum().get("text").toString() == "test")
    assert(actual._1.datum().get("sessionId").toString() == "177372818")
    assert(actual._1.datum().get("buyInId").toString() == "152")
    assert(actual._1.datum().get("tableId").toString() == "27773")

  }
}