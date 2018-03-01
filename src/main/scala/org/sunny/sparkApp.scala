package org.sunny

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.log4j.{Level,Logger}
object sparkApp{
  print("hellp")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("mengapp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("hdfs://localhost:9000/huser/test/NOTICE.txt")
//    val result = text.flatMap(_.split(' ')).map((_, 1)).reduceByKey(_ + _).collect()
//    result.foreach(println)
    text.take(10).foreach(println(_))

  }
}