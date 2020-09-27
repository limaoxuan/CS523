package com.max.bigdataprocessing.controller.streaming.utils

import java.util
import java.util.{Date, Properties, UUID}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object KafkaProducerApp {

  def main(args: Array[String]): Unit = {

    val props = new Properties
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", "192.168.8.178:9092")
    props.put("request.required.acks", "1")

    val topic = ParamsConf.topic
    val producer = new KafkaProducer[String, String](props)

    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    for (i <- 1 to 10) {
      val time = dateFormat.format(new Date()) + ""
      val userid = random.nextInt(1000) + ""
      val courseid = random.nextInt(500) + ""
      val fee = random.nextInt(400) + ""
      val result = Array("0", "1") // 0未成功支付，1成功支付
      val flag = result(random.nextInt(2))
      var orderid = UUID.randomUUID().toString

      val map = new util.HashMap[String, Object]()
      map.put("time", time)
      map.put("userId", userid)
      map.put("courseId", courseid)
      map.put("fee", fee)
      map.put("flag", flag)
      map.put("orderId", orderid)

      val json = new JSONObject(map)

      producer.send(new ProducerRecord[String, String]("mytopic", json + ""))

      put("MyOrder",json)
    }
  }

  def put(tableName: String, map: JSONObject): Unit = {
    println(map)
    val conf = new Configuration()
    conf.set("hbase.rootdir", "hdfs://hadoop001:8020/hbase")
    conf.set("hbase.zookeeper.quorum", "hadoop001:2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    println("put put put")
    val table = tableName;

    var connection: Connection = null
    var admin: Admin = null
    var myTable: Table = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin


      val tableName = TableName.valueOf(table)
//     val map = JSON.parseObject(jsonStr)
      myTable = connection.getTable(tableName)
      val orderId = map.getString("orderId");
      val time = map.getString("time");
      val free = map.getString("fee");
      val courseId = map.getString("courseId");
      val userId = map.getString("userId");
      println("fr")
      println(map)
      println(orderId)

      val put = new Put(Bytes.toBytes(orderId))

      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(time))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("free"), Bytes.toBytes(free))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("courseId"), Bytes.toBytes(courseId))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userId"), Bytes.toBytes(userId))

      myTable.put(put)

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != admin) {
        admin.close()
      }

      if (null != connection) {
        connection.close()
      }

      if (null != myTable) {
        myTable.close()
      }
    }
  }
}
