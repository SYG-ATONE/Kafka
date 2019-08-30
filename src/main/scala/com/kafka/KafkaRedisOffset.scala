package com.kafka

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.driver.allowMultipleContext", "true")
      // 设置序列化机制
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sc = ssc.sparkContext

    //取消日志显示
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val jedis = JedisConnectionPool.getConnection()
    val procity: Map[String, String] = sc.textFile("C://Users/shiyagung/Desktop/充值平台实时统计分析/city.txt")
      .map(x => x.split(" "))
      .map(x => (x(0), x(1)))
      .collect().toMap

    val broadcast = sc.broadcast(procity)

    //todo 先进行需求数据的初始化

    jedis.set("order", "0")
    jedis.set("money", "0")
    jedis.set("success", "0")
    jedis.set("fail", "0")


    // 配置参数
    // 配置基本参数
    //
    val groupId = "test-consumer-group"
    // topic
    val topic = "hz1803b"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      // kafka的Key和values解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 从头消费
      "auto.offset.reset" -> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset: Map[TopicPartition, Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream: InputDStream[ConsumerRecord[String, String]] =
      if (fromOffset.size == 0) {
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String, String](topics, kafkas)
        )
      } else {
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String, String](fromOffset.keys, kafkas, fromOffset)
        )
      }
    //todo 把需求的数据转换为long类型
    var order = jedis.get("order").toLong
    var money = jedis.get("money").toLong
    var success = jedis.get("success").toLong
    var fail = jedis.get("fail").toLong


    stream.foreachRDD({
      rdd =>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        //rdd.map(_.value()).foreach(println)
        val data: mutable.Buffer[String] = rdd.map(_.value()).collect().toBuffer
        data.foreach(x => {
          val jsondata = JSON.parseObject(x)
          if (!jsondata.getString("orderId").isEmpty) order += 1
          val chargefee = jsondata.getString("chargefee").toLong
          money += chargefee
          if (jsondata.getString("bussinessRst").equals("0000")) success += 1

          //统计全网每分钟的订单量数据
          val logOutTime = jsondata.getString("logOutTime").substring(0, 12)
          if (jedis.exists(logOutTime)) {
            jedis.set(logOutTime, (jedis.get(logOutTime).toInt + 1).toString)
          } else {
            jedis.set(logOutTime, "1")
          }

          //2.统计每小时各个省份的充值失败数据量
          val pid = jsondata.getString("provinceCode")
          val province = broadcast.value.get(pid).get
          if (!jsondata.getString("bussinessRst").equals("0000")) fail += 1

          val everyhour = jsondata.getString("logOutTime").substring(0, 10)
          if (jedis.exists(everyhour)) {
            jedis.set(everyhour, (jedis.get(everyhour).toInt + 1).toString)
          } else {
            jedis.set(everyhour, "1")
          }
          val list: RDD[(String, Int)] = sc.makeRDD(List((province, everyhour.toInt)))
          val rddlist: RDD[(String, Int)] = list.map(x => (x._1, x._2))

          rddlist.foreachPartition(data2MySQL)


        })

        //将数据存储到mysql

        def data2MySQL(it: Iterator[(String, Int)]) = {
          val dataSource = new ComboPooledDataSource()
          //第三步：从连接池对象中获取数据库连接
          val con = dataSource.getConnection()
          val sql = "insert into procount (省名,省订单量) values(?,?) "

          it.foreach(tup => {
            val ps = con.prepareStatement(sql)
            ps.setString(1,tup._1)
            ps.setInt(2,tup._2)

                     ps.executeUpdate()



              if(ps!= null)
                ps.close();
              if(con!= null)
                con.close();


          })
        }


        // 将偏移量进行更新

        for (or <- offestRange) {
          jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
        }
        jedis.set("order", order.toString)
        jedis.set("money", money.toString)
        jedis.set("success", success.toString)

        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
