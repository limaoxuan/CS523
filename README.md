# CS523

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



./kafka-topics.sh --create --zookeeper hadoop1:2081  --topic MyOrder --partitions 1 --replication-factor 1


```

HBASE

```
cd $HBASE_HOME/bin

./start-hbase.sh
```



CHECK ENV

jps

![image-20200927234648827](/Users/lee/Library/Application Support/typora-user-images/image-20200927234648827.png)



### RUN APPLICATION

1.import maven project.

2.install pom 

3.run StreamingApp first 

4.after that run KafkaProducerApp  for generating new datas which simulate real data



 Part 1 && part2

```
src/main/scala/StreamingApp is that spark streaming process data from kafka, and then processing data store in hbase
```



Part3



```scala
src/main/scala/KafkaProducerApp is for generating mock data and send data to kafka
```







