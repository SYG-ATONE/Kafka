package com.Utils

import java.text.SimpleDateFormat

/**
  * 时间工具类
  */
object Utils_Time {

  def consttime(startTime:String,stopTime:String): Long ={
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")

    // 20170412030013393282687799171031
    // 开始时间
    val st = df.parse(startTime.substring(0,17)).getTime
    //结束时间
    val et: Long = df.parse(stopTime).getTime
    et - st

  }
}
