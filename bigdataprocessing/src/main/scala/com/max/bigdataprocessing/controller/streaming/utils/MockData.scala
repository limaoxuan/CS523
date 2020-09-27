package com.max.bigdataprocessing.controller.streaming.utils

import java.util
import java.util.{Date, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang.time.FastDateFormat

import scala.util.Random

object MockData {

  def main(args: Array[String]): Unit = {
    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    // time, userid, courseid, orderid, fee, flag
    for (i <- 0 to 9) {
      val time = dateFormat.format(new Date()) + "";
      var userId = random.nextInt(1000) + ""
      var courseId = random.nextInt(500) + ""
      var fee = random.nextInt(400) + ""
      val result = Array("0", "1")// 1 paid 0 no pay
      val flag = result(random.nextInt(2))
      val orderId = UUID.randomUUID();

      val map = new util.HashMap[String, Object]()
      map.put("time", time);
      map.put("userId", userId);
      map.put("courseId", courseId);
      map.put("fee", fee);
      map.put("flag", flag);
      map.put("orderId", orderId);

      val json = new JSONObject(map)

      println(json)

    }


  }
}
