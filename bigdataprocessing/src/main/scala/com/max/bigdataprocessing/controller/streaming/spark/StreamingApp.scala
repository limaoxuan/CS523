package com.max.bigdataprocessing.controller.streaming.spark

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.max.bigdataprocessing.controller.streaming.utils.ParamsConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingApp {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    conf.set("hbase.rootdir", "hdfs://hadoop001:8020/hbase")
    conf.set("hbase.zookeeper.quorum", "hadoop001:2181")
    val tableName = "MyOrder"
    //    createTable(tableName, conf)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)


    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](ParamsConf.topic, ParamsConf.kafkaParams)
    )

    stream.map(x=>x.value()).print(10)

    stream.foreachRDD(rdd => {
      // flag fee time
      println(rdd)
      val data = rdd.map(x => JSON.parseObject(x.value()))
      println(data)
//      data.cache()
      data.map(x => {
        //                val time = x.getString()
        println(x)
        val flag = x.getString("flag")
        println("flag")
        println(flag)
        if (flag == "1") {
          put(tableName, x)
        }
      })

//      stream.map(x=>JSON.parseObject(x.value())).print()
      //
//            data.unpersist(true)

    }
    )

    //    println(value)

    ssc.start()
    ssc.awaitTermination()
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

  def createTable(tableName: String, conf: Configuration): Unit = {
    val table = tableName;


    var connection: Connection = null
    var admin: Admin = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin


      val tableName = TableName.valueOf(table)
      if (admin.tableExists(tableName)) {
        return
      }

      val tableDesc = new HTableDescriptor(TableName.valueOf(table))
      val columnDescRowKey = new HColumnDescriptor("row_key")
      val columnDescInfo = new HColumnDescriptor("info")
      tableDesc.addFamily(columnDescRowKey)
      tableDesc.addFamily(columnDescInfo)
      admin.createTable(tableDesc)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != admin) {
        admin.close()
      }

      if (null != connection) {
        connection.close()
      }
    }


  }
}
