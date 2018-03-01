package org.sunny

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.log4j.{Level,Logger}
object sparkApp{
  print("hellp")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("mengapp").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("hdfs://localhost:9000/huser/test/NOTICE.txt")
    val splitText = text.flatMap(_.split(" "))
    val filterLenText = splitText.filter(_.length<3).filter(_.length>0)
    filterLenText.take(20).foreach(println)
    println("单词长度0-3之间的个数为："+filterLenText.count)
    println("单词不重复且字符0-3之间的个数为："+filterLenText.distinct.count())


  }
}