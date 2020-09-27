package com.max.bigdataprocessing;

import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseApp {
    Connection connection = null;

    Admin admin = null;

    String tableName = "MyOrder";

    @Test
    public void should_getConnection() {


    }

    @Test
    public void createTable() throws Exception {
        TableName table = TableName.valueOf("MyOrder");
//        admin.disableTable(table);
//        admin.deleteTable(table);
        if (admin.tableExists(table)) {
            System.out.println(tableName + " 已经存在...");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(table);
            descriptor.addFamily(new HColumnDescriptor("row_key"));
            descriptor.addFamily(new HColumnDescriptor("info"));
            admin.createTable(descriptor);
            System.out.println(tableName + " 创建成功...");
        }
    }

    @Test
    public void testPut() throws Exception {
        Table table  = connection.getTable(TableName.valueOf("MyOrder"));

        if (table != null) {
            List<Put> puts = new ArrayList<>();

            Put put1 = new Put(Bytes.toBytes("jepson"));
            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes("18"));

//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"),  Bytes.toBytes(time))

            puts.add(put1);


            table.put(puts);

            if (table != null) {
                table.close();
            }
        }

    }

    @Before
    public void setUp()  {
        System.setProperty("hadoop.home.dir", "/");
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir", "hdfs://hadoop001:8020/hbase");
        configuration.set("hbase.zookeeper.quorum", "hadoop001:2181");



        try {
             connection = ConnectionFactory.createConnection(configuration);
            System.out.println(connection);
            System.out.println("connection-----------");
            admin = connection.getAdmin();
            Assert.assertNotNull(connection);
            Assert.assertNotNull(admin);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            if (connection != null) {
                connection.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
