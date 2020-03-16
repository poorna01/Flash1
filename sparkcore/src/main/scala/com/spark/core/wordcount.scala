package com.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount {
  
  def main(args:Array[String]){
    
    val conf = new SparkConf()
    conf.setAppName("wordcount programme")
    
    val sc = new SparkContext(conf)
    val  r1 = sc.textFile(args(0))
    val r2 = r1.flatMap(x=>x.split(""))
    val r3 = r2.map(x=>(x,1))
    val r4 = r3.reduceByKey((x,y)=>(x+y))
    r4.coalesce(1).saveAsTextFile(args(1))
    sc.stop()
    
  }
  
  
  
}