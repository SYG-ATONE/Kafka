package com.Utils

import com.kafka.JedisConnectionPool
import org.apache.spark.rdd.RDD

object jedisAPP {

  def Result01(result1: RDD[(String, List[Double])]) = {

    result1.foreachPartition(f => {
      val jedis = JedisConnectionPool.getConnection()
      jedis.select(1)
      f.foreach(t => {

        // 充值订单数
        jedis.hincrBy(t._1, "count", t._2.head.toLong)
        // 充值金额
        jedis.hincrByFloat(t._1, "money", t._2(1))
        // 充值成功数
        jedis.hincrBy(t._1, "success", t._2(2).toLong)
        // 充值总时长
        jedis.hincrBy(t._1, "time", t._2(3).toLong)
      })
      jedis.close()
    })
  }

  //指标一 2）
  def Result02(result2: RDD[(String, Double)]) = {

    result2.foreachPartition(f => {

      val jedis = JedisConnectionPool.getConnection()
      jedis.select(1)
      f.foreach(x => {
        jedis.incrBy(x._1,x._2.toLong)
      })
    })
  }


}