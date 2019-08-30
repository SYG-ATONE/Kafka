//package com.ToRedis
//
//import com.alibaba.fastjson.{JSON, JSONObject}
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable
//
//object KafkaToRedis {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .master("local[2]")
//      .appName(this.getClass.getName)
//      .getOrCreate()
//    val rdd = spark.sparkContext.textFile("C:\\Users\\shiyagung\\Desktop\\充值平台实时统计分析\\cmcc.json")
//
//    rdd.map(x => JSON.parseObject(x)).collect().toBuffer.foreach(println)
//  }
//}
