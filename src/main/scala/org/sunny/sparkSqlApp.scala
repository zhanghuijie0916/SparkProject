package org.sunny

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object sparkSqlApp {

  case class Person(name:String,age:BigInt)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkSql")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config("spark.some.config.option", "some-value").getOrCreate()
    import spark.implicits._

    val jsonDF = spark.read.json("hdfs://localhost:9000/huser/resources/people.json").na.fill(0).as[Person]    //Dataset[Person]
    jsonDF.foreach(row=>println(row))
    jsonDF.createTempView("people") //temp table
    spark.sql("SELECT * FROM people where age!=null") //select

    val textRDD = sc.textFile("hdfs://localhost:9000/huser/resources/people.txt")
    val textDF = textRDD.map(line=>line.split(","))
      .map(attributes=>Person(attributes(0),attributes(1).trim.toInt))
      .toDF()
    textDF.map(row=>"Name:"+row.getAs("name")).show()

    //创建StructField并将它包裹在StructType中
    val schema = StructType(Array(StructField("name",StringType,true),
      StructField("age",IntegerType,true)))
    val textRDD1 = textRDD.map(line=>line.split(","))
      .map(attrs=>Row(attrs(0),attrs(1).trim.toInt))
    val textDF1 = spark.createDataFrame(textRDD1,schema)//参数1:RDD,参数2:StructType
    textDF1.show()

  }

}
