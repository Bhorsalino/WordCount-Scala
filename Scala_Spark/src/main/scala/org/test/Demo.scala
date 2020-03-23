package org.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object Demo {
   def main(args: Array[String]){
    val conf = new SparkConf();
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "Spark_Demo")
    val sc = new SparkContext(conf);
    val rl = sc.range(1,100)
    rl.collect.foreach(println)
    sc.stop();
  }
}