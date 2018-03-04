package org.sunny

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

import scala.util.Sorting
object sparkApp{
  print("hellp")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    //spark://mengdeMacBook-Pro.local:7077  local[3]
    val conf = new SparkConf().setAppName("mengapp")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    //read textFile
    val text = sc.textFile("hdfs://localhost:9000/huser/test/NOTICE.txt")
    val splitText = text.flatMap(_.split(" ")) //split
    val filterLenText = splitText.filter(_.length<3).filter(_.length>0) //filter

    filterLenText.take(20).foreach(println) //show
    println("单词长度0-3之间的个数为："+filterLenText.count)
    println("单词不重复且字符0-3之间的个数为："+filterLenText.distinct.count())

    val splitRdd = filterLenText.randomSplit(Array(0.3,0.7))
    print("将RDD随机3、7开分："+splitRdd(0).count()+"---"+splitRdd(1).count())

    //word counter
    println("---------------------单词统计--------------------")
    val wordCounter = splitText.map(word=>(word,1)).reduceByKey(_+_)
    //将统计结果保存到文件夹wordCount中
    try {
      wordCounter.saveAsTextFile("hdfs://localhost:9000/huser/test/wordCount")
    }catch {
      case e:Exception => e.printStackTrace()
    }

    //关于pairRDD的练习
    //找出对应组别的所有人员姓名
    println("----------------共享变量:广播变量------------------")
    val employeeRdd = sc.parallelize(List(("java","张三"),("java","李四"),
      ("scala","王五"),("python","赵六")))
    val brocastRdd = sc.broadcast(employeeRdd.groupByKey().collectAsMap())
    val lookforList = List("java")
    val lookRes = lookforList.map(x=>brocastRdd.value(x))  //查询结果
    lookRes.foreach(println)

    println("----------------共享变量:累加器------------------")
    val sumAccumulator = sc.doubleAccumulator("成绩累加器")
    val gradesRdd = sc.parallelize(List(95,96.0,91,89,75))
    gradesRdd.foreach(x=>sumAccumulator.add(x))
    println("累加器名字："+sumAccumulator.name+"--平均成绩="+sumAccumulator.avg)
    println(""+sumAccumulator.count+"---"+sumAccumulator.value)

    gradesRdd.persist(StorageLevel.MEMORY_ONLY)
  }
}