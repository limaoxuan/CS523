# CS523

### Video 

### https://web.microsoftstream.com/video/5059ab95-de7b-4f9b-b51b-c1ada5ed0a23



### All the i/p and o/p files for your project

![image](https://github.com/limaoxuan/CS523/blob/master/hbase1.png)

![image](https://github.com/limaoxuan/CS523/blob/master/hbase2.png)



### SOURCE CODE

https://github.com/limaoxuan/CS523/tree/master/bigdataprocessing



### SET UP ENV

DOWNLOAD ENV

JDK 1.8

HADOOP
http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.6.0-cdh5.15.1

ZOOKEEPER
http://archive.cloudera.com/cdh5/cdh/5/zookeeper-3.4.5-cdh5.15.1

KAFKA
https://archive.apache.org/dist/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz

HBASE
http://archive.cloudera.com/cdh5/cdh/5/hbase-1.2.0-cdh5.15.1/

SET ENV



HADOOP

```xml
core-site.xml
			<property>
			    <name>fs.defaultFS</name>
			    <value>hdfs://hadoop001:8020</value>
			</property>
```



```xml
	hdfs-site.xml
		<property>
		    <name>dfs.replication</name>
		    <value>1</value>
		</property>
	
		<property>
		    <name>hadoop.tmp.dir</name>
		    <value>/home/hadoop/app/tmp</value>
		</property>
```




```shell
vim .bash_profile

export HADOOP_HOME=/root/app/hadoop-2.6.0-cdh5.15.1

export PATH=$HADOOP_HOME/bin:$PATH

wq

source ./bash_profile
```

ZOOKEEPER

```
zoo.cfg

dataDir=/root/app/tmp/zookeeper
```



```
vim .bash_profile

export ZK_HOME=/root/app/zookeeper-3.4.5-cdh5.15.1/bin
export PATH=$ZK_HOME/bin:$PATH

wq

source ./bash_profile
```



KAFKA



```
vim server.properties

advertised.listeners=PLAINTEXT://hadoop001:9092

log.dirs=/root/app/tmp/kafka-logs

zookeeper.connect=hadoop001:2181
```



```
vim .bash_profile

export KAFKA_HOME=/root/app/kafka_2.11-0.10.0.0
export PATH=$KAFKA_HOME/bin:$PATH

wq

source ./bash_profile
```



HBASE

```
vim hbase-env.sh

export JAVA_HOME=/root/app/jdk1.8.0_241

export HBASE_MANAGES_ZK=false
```





```
vim hbase-site.xml
<configuration>
<property>
<name>hbase.rootdir</name>
<value>hdfs://hadoop001:8020/hbase</value>
</property>


<property>
<name>hbase.cluster.distributed</name>
<value>true</value>
</property>

<property>
<name>hbase.zookeeper.quorum</name>
<value>hadoop001:2181</value>
</property>

</configuration>
```



```
vim .bash_profile

export HBASE_HOME=/root/app/hbase-1.2.0-cdh5.15.1
export PATH=$HBASE_HOME/bin:$PATH

wq

source ./bash_profile
```



RUN ENV

HADOOP

```
cd $HADOOP_HOME/sbin

./start-dfs.sh
```



ZOOKEEPER

```
cd $ZK_HOME

./zkServer.sh start


```

KAFKA && CREATE TOPIC

```
cd $KAFKA_HOME/bin

./kafka-server-start.sh -daemon $$KAFKA_HOME/config/server.properties



./kafka-topics.sh --create --zookeeper hadoop1:2081  --topic mytopic --partitions 1 --replication-factor 1


```

HBASE

```
cd $HBASE_HOME/bin

./start-hbase.sh
```



CHECK ENV

```
jps

9168 Jps
24930 HMaster
25091 HRegionServer
2788 DataNode
5814 QuorumPeerMain
6423 Kafka
2969 SecondaryNameNode
2637 NameNode
```



### RUN APPLICATION

1.import maven project.

2.install pom 

3.run StreamingApp first 

4.after that run KafkaProducerApp  for generating new datas which simulate real data



### EXPLANING THE DETAILS OF PARTS

 Part 1 && part2

```scala
src/main/scala/StreamingApp is that spark streaming process data from kafka, and then processing data store in hbase



// application run entrance
  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    
    // set up env for connecting hbase
    conf.set("hbase.rootdir", "hdfs://hadoop001:8020/hbase")
    conf.set("hbase.zookeeper.quorum", "hadoop001:2181")
    val tableName = "MyOrder"
    // create table if it is not exist
    createTable(tableName, conf)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)


    // setting spark env
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // kafaka and spark streamning setting
    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](ParamsConf.topic, ParamsConf.kafkaParams)
    )

    // print receving data
    stream.map(x=>x.value()).print(10)

    
    // process data
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
        
        // if flag is one which mean it is paid and put into table
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

    // start spark stream
    ssc.start()
    ssc.awaitTermination()
  }
  
  
  
  
  // put table into hbase
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
  // create table in hbase
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
```



Part3



```scala
src/main/scala/KafkaProducerApp is for generating mock data and send data to kafka


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

  // set mock data as real data
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

      // send those data send into kafaka with mytopic 
      producer.send(new ProducerRecord[String, String]("mytopic", json + ""))

      put("MyOrder",json)
    }
  }



```







