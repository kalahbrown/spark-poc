package com.bigfighsgames.spark.poc

import org.scalatest._
import com.holdenkarau.spark.testing._
import org.apache.spark.SparkContext._
import java.net.URLClassLoader
import java.net.URL

class SparkUnitTestExample extends FunSuite with SharedSparkContext {
   
   test("test class loader") {
    
       val cl = ClassLoader.getSystemClassLoader();
     
       
       val urls = cl.asInstanceOf[URLClassLoader].getURLs();
      

        for(i <- 1 to (urls.length - 1)){
        	println(urls(i).getFile());
        }
    
    assert(true)
  }
  
}