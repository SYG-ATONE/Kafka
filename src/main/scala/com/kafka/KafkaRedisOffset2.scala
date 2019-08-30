package com.kafka

import java.lang
import java.util.Properties

import com.Utils.{Utils_Time, jedisAPP}
import com.alibaba.fastjson.JSON
import com.mchange.v2.c3p0.ComboPooledDataSource
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
object KafkaRedisOffset2 {
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


    val dataSource = new ComboPooledDataSource()
    stream.foreachRDD({
      rdd =>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val baseData = rdd.map(_.value()).map(t => JSON.parseObject(t))
          //过滤需要的数据
          .filter(_.getString("serviceName").equals("reChargeNotifyReq"))
          .map(t => {
            // 先判断一下充值结果是否成功
            val result = t.getString("bussinessRst") // 充值结果
            val money: Double = if (result.equals("0000")) t.getDouble("chargefee") else 0.0 // 充值金额
            val success = if (result.equals("0000")) 1 else 0 // 充值成功数

            val starttime = t.getString("requestId") //开始充值时间
            val stoptime = t.getString("receiveNotifyTime") //结束充值时间

            val pid = t.getString("provinceCode")
            val province = broadcast.value.get(pid).get //省份

            //充值时长
            val costtime = Utils_Time.consttime(starttime, stoptime)

            (starttime.substring(0, 8),
              starttime.substring(0, 12),
              starttime.substring(0, 10),
              List[Double](1, money, success, costtime),
              (province, starttime.substring(0, 10)),
              province
            )
          }).cache()

        //指标一  1)	统计每天全网的充值订单量, 充值金额, 充值成功数
        val result1: RDD[(String, List[Double])] = baseData.map(t => (t._1, t._4)).reduceByKey((list1, list2) =>
          // list1(1,2,3).zip(list2(1,2,3)) = list((1,1),(2,2),(3,3))
          // map处理内部的元素相加
          list1.zip(list2).map(t => t._1 + t._2)
        )

        jedisAPP.Result01(result1)

        //指标一 2）实时充值业务办理趋势, 主要统计全网每分钟的订单量数据

        val result2: RDD[(String, Double)] = baseData.map(t => (t._2, (t._4) (0))).reduceByKey(_ + _)
        jedisAPP.Result02(result2)

        //指标二 统计每小时各个省份的充值失败的数量(并将结果存入到MySQL)
        val result3: RDD[((String, String), Double)] = baseData.map(t => (t._5, t._4(0) - t._4(2))).reduceByKey(_ + _)


        result3.foreachPartition(data2MySQL)

        //指标三：充值订单省份TOP10,并且统计每个省份的订单成功率，只保留一位小数（存入MySQL，并进行前台页面展示）
        // val result4 = baseData.map(x => (x._6,x._4(0))).reduceByKey(_+_).sortBy(_._2,false).take(10)

        //统计的个省的总订单量，和成功订单的数量/总订单量
        val result4: RDD[(String, (Double, Double))] = baseData.map(x => (x._6, x._4))
          .reduceByKey((list1, list2) => list1.zip(list2).map(x => x._1 + x._2)).map(x => (x._1, (x._2(0), x._2(2) / x._2(0))))

        result4.foreachPartition(Result4Data2MySQL)


        val result5 = baseData.map(x => (x._6, x._4))
          .reduceByKey((list1, list2) =>
            list1.zip(list2).map(x => x._1 + x._2)).map(x => (x._1, (x._2(2) / x._2(0)).toInt)) //成功率


        //指标四：实时统计每小时的充值笔数和充值金额
        val result6 = baseData.map(x => (x._2, x._4))
          .reduceByKey((list1, list2) => list1.zip(list2).map(x => x._2 + x._1)).map(x => (x._1, (x._2(0), x._2(1))))


        // 将偏移量进行更新

        for (or <- offestRange) {
          jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
        }


    })






    //rddlist.foreachPartition(data2MySQL)


    //将数据存储到mysql

    def data2MySQL(it: Iterator[((String, String), Double)]) = {

      //第三步：从连接池对象中获取数据库连接
      val con = dataSource.getConnection()
      val sql = "insert into procount (省名,省订单量) values(?,?) "

      it.foreach(tup => {
        val ps = con.prepareStatement(sql)
        ps.setString(1, tup._1._1 + "_" + tup._1._2)
        ps.setDouble(2, tup._2)
        ps.executeUpdate()
      })
      con.close()
    }

    def Result4Data2MySQL(it: Iterator[(String, (Double, Double))]) = {

      //第三步：从连接池对象中获取数据库连接
      val con = dataSource.getConnection()
      val sql = "insert into Result4 (province,allCount,successRate) values(?,?,?) "

      it.foreach(tup => {
        val ps = con.prepareStatement(sql)
        ps.setString(1, tup._1.toString)
        ps.setDouble(2, tup._2._1)
        ps.setDouble(3, tup._2._2)
        ps.executeUpdate()
      })
      con.close()
    }

    // 启动
    ssc.start()
    ssc.awaitTermination()


  }
}