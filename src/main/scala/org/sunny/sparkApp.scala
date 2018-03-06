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

    /*//read textFile
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

    /*
    RDD aggregate
    def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): U
     seqOp操作会聚合各分区中的元素，然后combOp操作把所有分区的聚合结果再次聚合，两个操作的初始值都是zeroValue.
     seqOp的操作是遍历分区中的所有元素(T)，第一个T跟zeroValue做操作，结果再作为与第二个T做操作的zeroValue，
     直到遍历完整个分区。combOp操作是把各分区聚合的结果，再聚合。aggregate函数返回一个跟RDD不同类型的值。
     因此，需要一个操作seqOp来把分区中的元素T合并成一个U，另外一个操作combOp把所有U聚合。
     */
    val rdd1 = sc.parallelize(List(1,2,3,4,5,7,8,9,11),4)
    rdd1.aggregate((0,0))((result,number)=>(result._1+number,result._2+1),
      (result1,result2)=>(result1._1+result2._1,result1._2+result2._2))
    //字符串的aggregate
    val rdd2 = sc.parallelize(Array("abc","d","ef","g","hig"),4)
    rdd2.aggregate("")((result,str)=>result+str,_+_)  //结果和分区数有关
*/
    //RDD groupBy
    def my(x:Int):Boolean = x match {
      case x if x>3 => true
      case x if x<=3 => false
    }
    val rdd3 = sc.parallelize(List(1,2,3,4,5,7,8,9,11))
    val res = rdd3.groupBy(my)
    res.foreach(println)
    //取出RDD中某一个元素
    val num = res.count()
    val rddList = res.take(num.toInt) //将RDD转为Array

    //keyBy：为数据集中每个个体数据增加一个key，形成键值对
    val rdd4 = sc.parallelize(List(1,2,3,4,5,7,8,9,11))
    val res2 = rdd4.keyBy(_*2)

    val res3 = res2.sortBy(x=>x._1,false) //或者直接使用
    res3.foreach(println)
    val res4 = res2.sortByKey()
    res4.foreach(println)



  }
}