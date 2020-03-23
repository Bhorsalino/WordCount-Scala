package org.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object WordCount {

   def main(args: Array[String]){
    val conf = new SparkConf();
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "Spark_Demo")
    val sc = new SparkContext(conf);
    
    val test = sc.textFile("Food.txt", 1)
    test.flatMap {line => 
      line.split(" ")
    }
    .map {word => 
      (word,1)
    }
    .reduceByKey(_ + _)
    .saveAsTextFile("Food_count.txt");
    
  }
   
}
