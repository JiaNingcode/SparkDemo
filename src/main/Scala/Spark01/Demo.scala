package Spark01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkWordCount")
    //创建sparkContext对象
    val sc =  new SparkContext(conf)
    //通过sparkcontext对象就可以处理数据


    //读取文件 参数是一个String类型的字符串 传入的是路径
    val lines: RDD[String] = sc.textFile(args(0))

    //切分数据
    val words: RDD[String] = lines.flatMap(_.split(" "))


    val tuples: RDD[(String, Int)] = words.map((_,1))


    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_+_)


    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2,false)




    sorted.saveAsTextFile(args(1))

    sc.stop()



  }
}
