package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scala Word Count").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://namenode:8020/user/root/constitution.txt")
    val counts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.cache()
    counts.saveAsTextFile("hdfs://namenode:8020/user/root/scala-wc-out")
  }
}