package org.sunny

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object sparkSqlApp {
  val fileLoc = "hdfs://localhost:9000/huser"

  case class Person(name:String,age:BigInt)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkSql")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config("spark.some.config.option", "some-value").getOrCreate()
    import spark.implicits._ //very important

    /*
    读取json数据并且转换成Dataset
     */
    val jsonDF = spark.read.json("hdfs://localhost:9000/huser/resources/people.json").na.fill(0).as[Person] //Dataset[Person]
    jsonDF.foreach(row => println(row))
    jsonDF.createTempView("people") //temp table
    spark.sql("SELECT * FROM people where age!=null") //select
    println("年龄小于20的结果：")
    jsonDF.select($"age" < 19).show //return true or false
    println("每个人的年龄增加20的结果：")
    jsonDF.select($"age".cast(IntegerType) + 20).show //将年龄强转成Int,并加上20岁


    /*
    读取parquet数据源
     */
    val parquetDF = spark.read.parquet("hdfs://localhost:9000/huser/resources/users.parquet")
    //不使用读取 API 将文件加载到 DataFrame 并进行查询
    val sqlDF = spark.sql("SELECT * FROM parquet.`hdfs://localhost:9000/huser/resources/users.parquet`")
    parquetDF.select($"name".contains("Al"), $"favorite_numbers".as[Array[Int]].isNotNull)

    /*
      result：
      +------------------+------------------------------+
      |contains(name, Al)|(favorite_numbers IS NOT NULL)|
      +------------------+------------------------------+
      |              true|                          true|
      |             false|                          true|
      +------------------+------------------------------+
     */
    //一下两条语句不一样
    parquetDF("name")
    parquetDF.select("name").show()

    //选取部分属性保存到指定的文件夹下
    parquetDF.select("name", "favorite_color")
      .write.mode("append").format("json").save(fileLoc + "/test/userCopy")
    //"mode": append overwrite
    parquetDF.select($"name", $"favorite_numbers")
      .write.format("json").mode("overwrite").save(fileLoc + "/test/userCopy")
    //merge data
    val combineDF = spark.read.option("mergeSchema", true)
      .json(fileLoc + "/test/userCopy")
    parquetDF.where('name.contains("a")).show()

    /*
    使用反射推倒Dataset
     */
    val textRDD = sc.textFile("hdfs://localhost:9000/huser/resources/people.txt")
    val textDF = textRDD.map(line => line.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    textDF.map(row => "Name:" + row.getAs("name")).show()

    /*
    以编程的方式置顶Schema
     */
    //创建StructField并将它包裹在StructType中
    val schema = StructType(Array(StructField("name", StringType, true),
      StructField("age", IntegerType, true)))
    val textRDD1 = textRDD.map(line => line.split(","))
      .map(attrs => Row(attrs(0), attrs(1).trim.toInt))
    val textDF1 = spark.createDataFrame(textRDD1, schema) //参数1:RDD,参数2:StructType
    textDF1.show()

    //mergeschema
    val squareDF = sc.makeRDD(1 until 7).map(x => (x, x * x)).toDF("value", "spuare")
    squareDF.write.parquet(fileLoc + "/resources/key/key=1")
    val cubeDF = sc.makeRDD(1 until 7).map(x => (x, x * x * x)).toDF("value", "cube")
    cubeDF.write.parquet(fileLoc + "/resources/key/key=2")
    val mergeDF = spark.read.option("mergeSchema", true).parquet(fileLoc + "/resources/key")

    //连接数据库
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "clientDB.client")
      .option("user", "root")
      .option("password", "MENG940811linxi**")
      .load()
    jdbcDF.show()

    val csvDF = spark.read.option("header",true).option("inferSchema",true).csv(fileLoc+"/resources/shampoo-sales.csv")

    val csvDF2 = csvDF.toDF("month","sales") //改变columnsName
  }

}
