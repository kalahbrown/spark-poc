package com.bigfighsgames.spark.poc

import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.avro.generic.GenericRecordBuilder
import com.bigfishgames.spark.poc.AvroIO
import org.apache.avro.mapreduce.AvroKeyInputFormat
import java.text.SimpleDateFormat
import java.util.Calendar
import scalax.file.Path

class ArvoIOTest extends FunSuite with Serializable {

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

  test("write Avro single thread") {
    
    //Create Spark Configuration and Context
    val conf = new SparkConf()
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
      .setAppName("read avro")

    val sc = new SparkContext(conf)

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

    sc.stop
    System.clearProperty("spark.driver.port")
    
    val path = Path.fromString( "src/test/resources/" + currentDate)
    path.deleteRecursively(continueOnFailure = true) 

  }

}